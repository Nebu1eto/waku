//! Pi RPC transport, shared by Pi and Oh My Pi.
//!
//! Oh My Pi is a fork of Pi that kept the newline-delimited RPC transport but
//! renamed part of the surface: forking is `branch`, a run settles on
//! `agent_end` instead of `agent_settled`, and oversized frames are chunked
//! once protocol v2 is negotiated. [`PiFlavor`] carries those differences so
//! both providers share one transport instead of two near-copies.
//!
//! choco-pi is Pi with a profile on top. It keeps Pi's dialect and adds the
//! host bridge (see [`super::pi_host_bridge`]) plus real user dialogs.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow};
use crossbeam_channel::{Sender, bounded, unbounded};
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::pi_host_bridge::{self, HostBridgeState};
use super::{activity, computer_use as computer_use_runtime};
use crate::choco_pi_profile::{ChocoPiProfile, HostBridgeSpec};
use crate::driver::{
    DriverControl, DriverEventSender, DriverEventSink, DriverStartOptions, SessionOptions,
};
use crate::model::{
    ActivityKind, BackgroundWorkEvent, BackgroundWorkKey, BackgroundWorkKind, DriverEvent,
    GoalOperation, ProviderResumeCursor, ReportedCommand, RuntimeMode, ThreadGoalStatus,
    UserInputAnswer, UserInputOption, UserInputQuestion,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// choco-pi's foreground `Agent` reports progress every ~80 ms; anything
/// faster than the stream commit cadence is only churn.
const TOOL_UPDATE_DEBOUNCE: Duration = Duration::from_millis(100);

/// Every snapshot appends entries to the session file, so the client's
/// periodic refresh is honored only this often, and only with live work.
const SNAPSHOT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

const HOST_BRIDGE_COMMAND: &str = "host-bridge";
const GOAL_COMMAND: &str = "goal";
const HOST_BRIDGE_SNAPSHOT: &str = "/host-bridge snapshot";

/// The question id every forwarded dialog uses; Pi dialogs ask one thing.
const DIALOG_QUESTION_ID: &str = "value";
const CONFIRM_YES: &str = "Yes";
const CONFIRM_NO: &str = "No";

/// The handshake races the agent's own startup, so it needs more headroom than
/// a request against the already-running process. Pi loads extensions and
/// resources and, when model networking is on, refreshes its model catalog
/// before it reads stdin — it budgets 15 s for that refresh alone — and a
/// timed-out `get_state` there fails the whole session.
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(30);

/// Oh My Pi has to start a whole second agent to clone a session, so it needs
/// more headroom than a request against the already-running process.
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);

/// Oh My Pi refuses to reassemble beyond this, so neither should Waku.
const MAX_REASSEMBLED_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Which dialect of the Pi RPC protocol a session speaks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PiFlavor {
    Pi,
    OhMyPi,
    /// Pi running the choco-pi profile: Pi's RPC dialect plus the host bridge.
    ChocoPi,
}

impl PiFlavor {
    fn display_name(self) -> &'static str {
        match self {
            Self::Pi => "Pi",
            Self::OhMyPi => "Oh My Pi",
            Self::ChocoPi => "choco-pi",
        }
    }

    /// Pi has no permission system and only needs project-local files trusted;
    /// Oh My Pi does have one, and Waku only ever runs these in Full access.
    fn full_access_arg(self) -> &'static str {
        match self {
            Self::Pi | Self::ChocoPi => "--approve",
            Self::OhMyPi => "--yolo",
        }
    }

    /// The event that means the run is over and no more work is scheduled.
    fn settled_event(self) -> &'static str {
        match self {
            Self::Pi | Self::ChocoPi => "agent_settled",
            Self::OhMyPi => "agent_end",
        }
    }

    fn session_info_event(self) -> &'static str {
        match self {
            Self::Pi | Self::ChocoPi => "session_info_changed",
            Self::OhMyPi => "session_info_update",
        }
    }

    fn session_info_title_field(self) -> &'static str {
        match self {
            Self::Pi | Self::ChocoPi => "name",
            Self::OhMyPi => "title",
        }
    }

    fn branch_messages_command(self) -> &'static str {
        match self {
            Self::Pi | Self::ChocoPi => "get_fork_messages",
            Self::OhMyPi => "get_branch_messages",
        }
    }

    fn branch_command(self) -> &'static str {
        match self {
            Self::Pi | Self::ChocoPi => "fork",
            Self::OhMyPi => "branch",
        }
    }

    /// Oh My Pi dropped Pi's opt-out env var; it gates its update check on a
    /// setting instead, and does it off the startup path either way.
    fn skips_version_check_by_env(self) -> bool {
        matches!(self, Self::Pi | Self::ChocoPi)
    }

    /// Only Oh My Pi chunks oversized frames, and only after it is asked to.
    fn negotiates_protocol_v2(self) -> bool {
        matches!(self, Self::OhMyPi)
    }

    /// Waku's computer-use bridge is a Pi extension written against Pi's
    /// extension API. Oh My Pi ships its own `/computer` instead, and
    /// choco-pi loads its own computer-use package; a second toolset would
    /// confuse the model and bypass choco-pi's tool-surface policy.
    fn supports_waku_computer_use(self) -> bool {
        matches!(self, Self::Pi)
    }

    /// The bridge this session decodes, from the profile read once at start.
    /// A marker whose bridge schema is newer than this build yields `None`,
    /// and the session behaves like plain Pi.
    fn host_bridge(self, profile: Option<&ChocoPiProfile>) -> Option<HostBridgeSpec> {
        match self {
            Self::Pi | Self::OhMyPi => None,
            Self::ChocoPi => profile
                .and_then(ChocoPiProfile::supported_host_bridge)
                .cloned(),
        }
    }

    /// Whether background-work and goal controls can exist at all. The
    /// session still checks that the bridge is supported and registered.
    fn host_bridge_capable(self) -> bool {
        matches!(self, Self::ChocoPi)
    }

    /// Whether extension dialogs reach the user. Pi and Oh My Pi keep
    /// cancelling them; turning that on for them is a separate change.
    fn forwards_extension_ui(self) -> bool {
        matches!(self, Self::ChocoPi)
    }

    /// Whether the agent begins runs nobody prompted: choco-pi wakes the
    /// session when background work finishes. Such a run is announced by
    /// `agent_start` alone, so a prompt the agent reports as handled
    /// locally can settle its own turn without stranding a later run.
    fn starts_turns_unprompted(self) -> bool {
        matches!(self, Self::ChocoPi)
    }

    /// Only choco-pi's `Agent` streams progress fast enough to need it.
    fn debounces_tool_updates(self) -> bool {
        matches!(self, Self::ChocoPi)
    }

    fn cursor(self, session_id: String, session_file: Option<PathBuf>) -> ProviderResumeCursor {
        match self {
            Self::Pi => ProviderResumeCursor::Pi {
                session_id,
                session_file,
            },
            Self::OhMyPi => ProviderResumeCursor::OhMyPi {
                session_id,
                session_file,
            },
            Self::ChocoPi => ProviderResumeCursor::ChocoPi {
                session_id,
                session_file,
            },
        }
    }

    fn session_file_from_cursor(self, cursor: &ProviderResumeCursor) -> Option<&PathBuf> {
        match (self, cursor) {
            (Self::Pi, ProviderResumeCursor::Pi { session_file, .. })
            | (Self::OhMyPi, ProviderResumeCursor::OhMyPi { session_file, .. })
            | (Self::ChocoPi, ProviderResumeCursor::ChocoPi { session_file, .. }) => {
                session_file.as_ref()
            }
            _ => None,
        }
    }

    fn owns_cursor(self, cursor: &ProviderResumeCursor) -> bool {
        matches!(
            (self, cursor),
            (Self::Pi, ProviderResumeCursor::Pi { .. })
                | (Self::OhMyPi, ProviderResumeCursor::OhMyPi { .. })
                | (Self::ChocoPi, ProviderResumeCursor::ChocoPi { .. })
        )
    }
}

enum CommandMessage {
    Prompt(String),
    Steer(String),
    Cancel,
    CancelExtensionRequest(String),
    /// A complete `extension_ui_response`. Pi never acknowledges it.
    ExtensionUiResponse(Value),
    RefreshBackgroundWork,
    StopBackgroundWork {
        key: BackgroundWorkKey,
        control_id: String,
    },
    Goal(GoalOperation),
    Options(SessionOptions),
    Rollback {
        turns: usize,
        response: Sender<Result<ProviderResumeCursor, String>>,
    },
    Fork {
        turns_to_remove: usize,
        response: Sender<Result<ProviderResumeCursor, String>>,
    },
    Shutdown,
}

enum PendingResponse {
    Request(Sender<Result<Value, String>>),
    /// A `/host-bridge snapshot` command. Answered like a request, but the
    /// reader also settles the entries it streamed before the response.
    Snapshot(Sender<Result<Value, String>>),
    /// The turn marker of a user prompt. Command prompts never use it.
    Prompt,
}

impl PendingResponse {
    fn is_prompt(&self) -> bool {
        matches!(self, Self::Prompt)
    }
}

type PendingResponses = Arc<Mutex<HashMap<String, PendingResponse>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DialogKind {
    Confirm,
    Select,
    Text,
}

/// A forwarded extension dialog awaiting the user. Pi resolves a dialog
/// with a `timeout` on its own and never says so, so an answer arriving
/// after the deadline is dropped instead of sent to a request Pi forgot.
struct PendingDialog {
    kind: DialogKind,
    options: Vec<String>,
    deadline: Option<Instant>,
}

type PendingDialogs = Arc<Mutex<HashMap<String, PendingDialog>>>;

pub struct PiDriver {
    flavor: PiFlavor,
    commands: Sender<CommandMessage>,
    computer_use: Option<computer_use_runtime::ComputerUseRuntime>,
    dialogs: PendingDialogs,
}

fn configure_pi_computer_use_command(
    command: &mut std::process::Command,
    config: Option<(&computer_use_runtime::ComputerUseConfig, &Path)>,
) {
    if let Some((config, extension)) = config {
        command
            .arg("--extension")
            .arg(extension)
            .arg("--skill")
            .arg(&config.skill_path)
            .env("WAKU_JS_REPL_SERVER", &config.repl_path)
            .env("WAKU_COMPUTER_USE_SERVER", &config.server_path)
            .env(
                "WAKU_COMPUTER_USE_PROCESS_DIRECTORY",
                &config.process_directory,
            );
    }
}

impl PiDriver {
    pub fn start(
        flavor: PiFlavor,
        options: DriverStartOptions,
        events: DriverEventSender,
    ) -> anyhow::Result<Self> {
        let DriverStartOptions {
            binary,
            cwd,
            mode,
            model,
            reasoning_effort,
            service_tier: _,
            context_window: _,
            agent_preset: _,
            computer_use_enabled,
            provider_cursor,
        } = options;
        if mode != RuntimeMode::FullAccess {
            return Err(anyhow!(
                "{} currently supports Full access only",
                flavor.display_name()
            ));
        }
        let resume_session_file = match provider_cursor {
            Some(cursor) if flavor.owns_cursor(&cursor) => {
                let Some(session_file) = flavor.session_file_from_cursor(&cursor).cloned() else {
                    return Err(anyhow!(
                        "cannot resume {} because its native session file is missing",
                        flavor.display_name()
                    ));
                };
                Some(session_file)
            }
            Some(cursor) => {
                return Err(anyhow!(
                    "cannot resume {} from a {} cursor",
                    flavor.display_name(),
                    cursor.provider().display_name()
                ));
            }
            None => None,
        };
        if let Some(model) = model.as_deref() {
            parse_model_slug(model)?;
        }
        // Read the marker once per session, never per frame.
        let host_bridge = match flavor {
            PiFlavor::ChocoPi => {
                flavor.host_bridge(crate::choco_pi_profile::read_profile().as_ref())
            }
            PiFlavor::Pi | PiFlavor::OhMyPi => None,
        };
        let host_bridge_enabled = host_bridge.is_some();

        let computer_use = (computer_use_enabled && flavor.supports_waku_computer_use())
            .then(|| computer_use_runtime::ComputerUseRuntime::start(events.clone()))
            .transpose()?;
        let pi_extension = computer_use
            .as_ref()
            .map(|_| crate::computer_use::pi_extension_path())
            .transpose()?;
        let mut command = crate::command_env::command(&binary);
        command.args(["--mode", "rpc", flavor.full_access_arg()]);
        if flavor.skips_version_check_by_env() {
            command.env("PI_SKIP_VERSION_CHECK", "1");
        }
        configure_choco_pi_host_command(&mut command, flavor);
        configure_pi_computer_use_command(
            &mut command,
            computer_use
                .as_ref()
                .zip(pi_extension.as_deref())
                .map(|(runtime, extension)| (&runtime.config, extension)),
        );
        let command = command
            .current_dir(&cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = crate::command_env::spawn(command)
            .with_context(|| format!("failed to start `{} --mode rpc`", binary.display()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("{} stdin unavailable", flavor.display_name()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("{} stdout unavailable", flavor.display_name()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("{} stderr unavailable", flavor.display_name()))?;

        let (commands, command_rx) = unbounded();
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let dialogs = PendingDialogs::default();
        let bridge_live = Arc::new(AtomicBool::new(false));
        let writer_bridge_live = bridge_live.clone();
        let reader_pending = pending.clone();
        let reader_commands = commands.clone();
        let reader_events = events.clone();
        let reader_dialogs = dialogs.clone();
        let reader_thread =
            thread::Builder::new()
                .name("waku-pi-reader".into())
                .spawn(move || {
                    let mut stream_state =
                        PiStreamState::new(host_bridge, reader_dialogs, bridge_live);
                    let mut chunks = ChunkAssembly::default();
                    for line in BufReader::new(stdout).lines() {
                        match line {
                            Ok(line) if !line.trim().is_empty() => {
                                match serde_json::from_str::<Value>(&line) {
                                    Ok(value) => {
                                        // A chunked frame arrives as an
                                        // uninterrupted run of `rpc_chunk`
                                        // envelopes that reassemble into one
                                        // logical message.
                                        match chunks.accept(value) {
                                            Ok(Some(value)) => handle_pi_message(
                                                flavor,
                                                value,
                                                &reader_pending,
                                                &reader_commands,
                                                &reader_events,
                                                &mut stream_state,
                                            ),
                                            Ok(None) => {}
                                            Err(error) => {
                                                let _ =
                                                    reader_events.send(DriverEvent::Error(tr!(
                                                        "errors.provider_transport_read",
                                                        provider = flavor.display_name(),
                                                        error = error
                                                    )));
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        let _ = reader_events.send(DriverEvent::Error(tr!(
                                            "errors.provider_invalid_json",
                                            provider = flavor.display_name(),
                                            error = error
                                        )));
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(error) => {
                                let _ = reader_events.send(DriverEvent::Error(tr!(
                                    "errors.provider_transport_read",
                                    provider = flavor.display_name(),
                                    error = error
                                )));
                                break;
                            }
                        }
                    }
                    // Unblock anything waiting on an RPC reply immediately; the
                    // process thread owns the `ProcessExited` announcement so a
                    // non-zero exit can be reported before the runtime is torn down.
                    fail_pending(
                        &reader_pending,
                        &format!("{} RPC process exited", flavor.display_name()),
                    );
                })?;

        let writer_pending = pending;
        let writer_events = events.clone();
        thread::Builder::new()
            .name("waku-pi-writer".into())
            .spawn(move || {
                let mut stdin = stdin;
                let mut next_request_id = 0_u64;
                let initialize = (|| -> Result<Value, String> {
                    // The agent does not answer until it has finished loading,
                    // so every handshake request gets the startup timeout.
                    // Negotiate before anything else so a large first response
                    // arrives chunked rather than shrunk to an error frame.
                    if flavor.negotiates_protocol_v2() {
                        send_request_with_timeout(
                            &mut stdin,
                            &writer_pending,
                            &mut next_request_id,
                            json!({"type": "negotiate_protocol", "protocolVersion": 2}),
                            INITIALIZE_TIMEOUT,
                        )?;
                    }
                    let _ = send_request_with_timeout(
                        &mut stdin,
                        &writer_pending,
                        &mut next_request_id,
                        json!({"type": "get_state"}),
                        INITIALIZE_TIMEOUT,
                    )?;
                    if let Some(session_file) = resume_session_file {
                        let response = send_request_with_timeout(
                            &mut stdin,
                            &writer_pending,
                            &mut next_request_id,
                            json!({
                                "type": "switch_session",
                                "sessionPath": session_file
                            }),
                            INITIALIZE_TIMEOUT,
                        )?;
                        if response.pointer("/data/cancelled").and_then(Value::as_bool)
                            == Some(true)
                        {
                            return Err(format!(
                                "{} session switch was cancelled",
                                flavor.display_name()
                            ));
                        }
                    }
                    if let Some(model) = model.as_deref() {
                        let (provider, model_id) =
                            parse_model_slug(model).map_err(|error| error.to_string())?;
                        let _ = send_request_with_timeout(
                            &mut stdin,
                            &writer_pending,
                            &mut next_request_id,
                            json!({
                                "type": "set_model",
                                "provider": provider,
                                "modelId": model_id
                            }),
                            INITIALIZE_TIMEOUT,
                        )?;
                    }
                    if let Some(level) = reasoning_effort.as_deref() {
                        let _ = send_request_with_timeout(
                            &mut stdin,
                            &writer_pending,
                            &mut next_request_id,
                            json!({"type": "set_thinking_level", "level": level}),
                            INITIALIZE_TIMEOUT,
                        )?;
                    }
                    send_request_with_timeout(
                        &mut stdin,
                        &writer_pending,
                        &mut next_request_id,
                        json!({"type": "get_state"}),
                        INITIALIZE_TIMEOUT,
                    )
                })();

                let state = match initialize {
                    Ok(state) => state,
                    Err(error) => {
                        let _ = writer_events.send(DriverEvent::Error(tr!(
                            "errors.initialize_provider",
                            provider = flavor.display_name(),
                            error = error
                        )));
                        let _ = writer_events.send(DriverEvent::TurnFinished {
                            success: false,
                            summary: Some(tr!(
                                "errors.provider_initialize_session",
                                provider = flavor.display_name()
                            )),
                        });
                        return;
                    }
                };
                let Some(mut cursor) = cursor_from_state(flavor, &state) else {
                    let _ = writer_events.send(DriverEvent::Error(tr!(
                        "errors.provider_no_session_id",
                        provider = flavor.display_name()
                    )));
                    let _ = writer_events.send(DriverEvent::TurnFinished {
                        success: false,
                        summary: Some(tr!(
                            "errors.provider_initialize_session",
                            provider = flavor.display_name()
                        )),
                    });
                    return;
                };
                let initial_usage = send_request(
                    &mut stdin,
                    &writer_pending,
                    &mut next_request_id,
                    json!({"type": "get_session_stats"}),
                )
                .ok()
                .and_then(|stats| pi_context_usage(&state, Some(&stats)))
                .or_else(|| pi_context_usage(&state, None));
                let _ = writer_events.send(DriverEvent::Connected {
                    provider_cursor: Some(cursor.clone()),
                });
                if let Some((context_tokens, context_window)) = initial_usage {
                    let _ = writer_events.send(DriverEvent::UsageUpdated {
                        context_tokens,
                        context_window,
                    });
                }
                if let Some(title) = state
                    .pointer("/data/sessionName")
                    .and_then(Value::as_str)
                    .filter(|title| !title.trim().is_empty())
                {
                    let _ =
                        writer_events.send(DriverEvent::AutoTitleUpdated(Some(title.to_owned())));
                }

                // Pi reports extension commands, prompts and skills on request;
                // Oh My Pi pushes its registry at startup and after reloads.
                let catalog = matches!(flavor, PiFlavor::Pi | PiFlavor::ChocoPi)
                    .then(|| {
                        send_request(
                            &mut stdin,
                            &writer_pending,
                            &mut next_request_id,
                            json!({"type": "get_commands"}),
                        )
                        .ok()
                    })
                    .flatten();
                if let Some(catalog) = catalog.as_ref() {
                    publish_commands(
                        crate::slash_command_catalog::parse_pi_commands(catalog),
                        &writer_events,
                    );
                }
                // A `/host-bridge …` prompt that no extension claims would
                // reach the model as a user message, so commands go out only
                // when this session reported registering them.
                let bridge_commands = host_bridge_enabled
                    && catalog
                        .as_ref()
                        .is_some_and(|catalog| catalog_has_command(catalog, HOST_BRIDGE_COMMAND));
                let goal_command = bridge_commands
                    && catalog
                        .as_ref()
                        .is_some_and(|catalog| catalog_has_command(catalog, GOAL_COMMAND));
                // No `get_entries` replay: sessions reach tens of megabytes.
                // The snapshot re-announces the goal and every live item.
                let mut last_snapshot = None::<Instant>;
                if bridge_commands {
                    last_snapshot = Some(Instant::now());
                    let _ = send_command_prompt(
                        &mut stdin,
                        &writer_pending,
                        &mut next_request_id,
                        HOST_BRIDGE_SNAPSHOT,
                        true,
                    );
                }

                // Both flavors expose setters for these, so changing either is
                // an RPC on the live session rather than a restart.
                let mut current_model = model;
                let mut current_effort = reasoning_effort;
                while let Ok(message) = command_rx.recv() {
                    match message {
                        CommandMessage::Prompt(prompt) => {
                            let result = send_prompt(
                                &mut stdin,
                                &writer_pending,
                                &mut next_request_id,
                                &prompt,
                            );
                            if let Err(error) = result {
                                let _ = writer_events.send(DriverEvent::Error(tr!(
                                    "errors.provider_rejected_prompt_detail",
                                    provider = flavor.display_name(),
                                    error = error
                                )));
                                let _ = writer_events.send(DriverEvent::TurnFinished {
                                    success: false,
                                    summary: Some(tr!(
                                        "errors.provider_rejected_prompt",
                                        provider = flavor.display_name()
                                    )),
                                });
                            }
                        }
                        CommandMessage::Steer(prompt) => {
                            let result = send_request(
                                &mut stdin,
                                &writer_pending,
                                &mut next_request_id,
                                json!({"type": "steer", "message": prompt}),
                            );
                            match result {
                                Ok(_) => {
                                    let _ = writer_events
                                        .send(DriverEvent::SteerAccepted { message: prompt });
                                }
                                Err(error) => {
                                    let _ = writer_events.send(DriverEvent::SteerRejected {
                                        message: prompt,
                                        reason: error,
                                    });
                                }
                            }
                        }
                        CommandMessage::Cancel => {
                            if let Err(error) = send_request(
                                &mut stdin,
                                &writer_pending,
                                &mut next_request_id,
                                json!({"type": "abort"}),
                            ) {
                                let _ = writer_events.send(DriverEvent::Error(tr!(
                                    "errors.stop_provider",
                                    provider = flavor.display_name(),
                                    error = error
                                )));
                            }
                        }
                        CommandMessage::Options(options) => {
                            if options.model != current_model {
                                match options.model.as_deref().map(parse_model_slug).transpose() {
                                    Ok(Some((provider, model_id))) => {
                                        match send_request(
                                            &mut stdin,
                                            &writer_pending,
                                            &mut next_request_id,
                                            json!({
                                                "type": "set_model",
                                                "provider": provider,
                                                "modelId": model_id
                                            }),
                                        ) {
                                            Ok(response) => {
                                                if let Some(window) = response
                                                    .pointer("/data/contextWindow")
                                                    .and_then(Value::as_u64)
                                                    .filter(|window| *window > 0)
                                                {
                                                    let _ = writer_events.send(
                                                        DriverEvent::UsageUpdated {
                                                            context_tokens: None,
                                                            context_window: Some(window),
                                                        },
                                                    );
                                                }
                                            }
                                            Err(error) => {
                                                let _ =
                                                    writer_events.send(DriverEvent::Error(tr!(
                                                        "errors.switch_provider_model",
                                                        provider = flavor.display_name(),
                                                        error = error
                                                    )));
                                            }
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        let _ = writer_events
                                            .send(DriverEvent::Error(error.to_string()));
                                    }
                                }
                                current_model = options.model;
                            }
                            if options.reasoning_effort != current_effort {
                                if let Some(level) = options.reasoning_effort.as_deref()
                                    && let Err(error) = send_request(
                                        &mut stdin,
                                        &writer_pending,
                                        &mut next_request_id,
                                        json!({"type": "set_thinking_level", "level": level}),
                                    )
                                {
                                    let _ = writer_events.send(DriverEvent::Error(tr!(
                                        "errors.change_provider_thinking",
                                        provider = flavor.display_name(),
                                        error = error
                                    )));
                                }
                                current_effort = options.reasoning_effort;
                            }
                        }
                        CommandMessage::CancelExtensionRequest(id) => {
                            if write_json_line(
                                &mut stdin,
                                &json!({
                                    "type": "extension_ui_response",
                                    "id": id,
                                    "cancelled": true
                                }),
                            )
                            .is_err()
                            {
                                break;
                            }
                        }
                        CommandMessage::ExtensionUiResponse(response) => {
                            if write_json_line(&mut stdin, &response).is_err() {
                                break;
                            }
                        }
                        CommandMessage::RefreshBackgroundWork => {
                            if bridge_commands
                                && writer_bridge_live.load(Ordering::Acquire)
                                && last_snapshot
                                    .is_none_or(|at| at.elapsed() >= SNAPSHOT_REFRESH_INTERVAL)
                            {
                                last_snapshot = Some(Instant::now());
                                let _ = send_command_prompt(
                                    &mut stdin,
                                    &writer_pending,
                                    &mut next_request_id,
                                    HOST_BRIDGE_SNAPSHOT,
                                    true,
                                );
                            }
                        }
                        CommandMessage::StopBackgroundWork { key, control_id } => {
                            if let Err(message) = stop_via_bridge(
                                &mut stdin,
                                &writer_pending,
                                &mut next_request_id,
                                &key,
                                &control_id,
                                bridge_commands,
                            ) {
                                let _ = writer_events.send(DriverEvent::BackgroundWork(
                                    BackgroundWorkEvent::StopFailed { key, message },
                                ));
                            }
                        }
                        CommandMessage::Goal(operation) => {
                            let commands = if !bridge_commands {
                                Err(format!(
                                    "{} goals need the choco-pi host bridge",
                                    flavor.display_name()
                                ))
                            } else {
                                goal_commands(&operation, goal_command)
                            };
                            if operation == GoalOperation::Refresh {
                                last_snapshot = Some(Instant::now());
                            }
                            let result = commands.and_then(|commands| {
                                commands.into_iter().try_for_each(|command| {
                                    send_command_prompt(
                                        &mut stdin,
                                        &writer_pending,
                                        &mut next_request_id,
                                        &command,
                                        command == HOST_BRIDGE_SNAPSHOT,
                                    )
                                    .map(drop)
                                })
                            });
                            if let Err(error) = result {
                                let _ = writer_events.send(DriverEvent::Error(error));
                            }
                        }
                        CommandMessage::Rollback { turns, response } => {
                            let result = fork_pi_session(
                                flavor,
                                &mut stdin,
                                &writer_pending,
                                &mut next_request_id,
                                &binary,
                                &cwd,
                                &cursor,
                                turns,
                                false,
                            );
                            if let Ok(next_cursor) = &result {
                                cursor = next_cursor.clone();
                            }
                            let _ = response.send(result);
                        }
                        CommandMessage::Fork {
                            turns_to_remove,
                            response,
                        } => {
                            let result = fork_pi_session(
                                flavor,
                                &mut stdin,
                                &writer_pending,
                                &mut next_request_id,
                                &binary,
                                &cwd,
                                &cursor,
                                turns_to_remove,
                                true,
                            );
                            let _ = response.send(result);
                        }
                        CommandMessage::Shutdown => break,
                    }
                }
            })?;

        let last_visible_stderr = Arc::new(Mutex::new(None::<String>));
        let stderr_last_error = last_visible_stderr.clone();
        let stderr_events = events.clone();
        let stderr_thread =
            thread::Builder::new()
                .name("waku-pi-stderr".into())
                .spawn(move || {
                    for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                        if line.to_ascii_lowercase().contains("error") {
                            let error = format!("{}: {}", flavor.display_name(), line.trim());
                            *stderr_last_error.lock() = Some(error.clone());
                            let _ = stderr_events.send(DriverEvent::Error(error));
                        }
                    }
                })?;

        // Nothing signals or kills the agent process: it exits when the writer
        // thread drops its stdin. Something still has to reap it, or every
        // session that ever ran leaves a zombie behind for the life of the app.
        thread::Builder::new()
            .name("waku-pi-process".into())
            .spawn(move || {
                let status = child.wait();
                let _ = reader_thread.join();
                let _ = stderr_thread.join();
                match status {
                    Ok(status) if !status.success() && last_visible_stderr.lock().is_none() => {
                        let _ = events.send(DriverEvent::Error(tr!(
                            "errors.provider_rpc_exited",
                            provider = flavor.display_name(),
                            status = status
                        )));
                    }
                    Err(error) => {
                        let _ = events.send(DriverEvent::Error(tr!(
                            "errors.read_provider_exit_status",
                            provider = format!("{} RPC", flavor.display_name()),
                            error = error
                        )));
                    }
                    _ => {}
                }
                let _ = events.send(DriverEvent::ProcessExited);
            })?;

        Ok(Self {
            flavor,
            commands,
            computer_use,
            dialogs,
        })
    }
}

/// choco-pi's host bridge streams progress frames only to a host that asks.
fn configure_choco_pi_host_command(command: &mut std::process::Command, flavor: PiFlavor) {
    if flavor == PiFlavor::ChocoPi {
        command.env("CHOCO_PI_HOST", "waku").env(
            "CHOCO_PI_HOST_BRIDGE_SCHEMA",
            crate::choco_pi_profile::HOST_BRIDGE_SCHEMA.to_string(),
        );
    }
}

impl DriverControl for PiDriver {
    fn prompt(&self, prompt: String) {
        let _ = self.commands.send(CommandMessage::Prompt(prompt));
    }

    fn supports_steer(&self) -> bool {
        true
    }

    fn steer(&self, prompt: String) {
        let _ = self.commands.send(CommandMessage::Steer(prompt));
    }

    fn cancel(&self) {
        // An open dialog blocks the extension that raised it; abort alone
        // would leave it waiting forever.
        let open = self
            .dialogs
            .lock()
            .drain()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        for id in open {
            let _ = self
                .commands
                .send(CommandMessage::CancelExtensionRequest(id));
        }
        let _ = self.commands.send(CommandMessage::Cancel);
    }

    fn cancel_computer_use(&self) {
        if let Some(computer_use) = self.computer_use.as_ref() {
            computer_use.stop();
        }
    }

    fn refresh_background_work(&self) {
        if self.flavor.host_bridge_capable() {
            let _ = self.commands.send(CommandMessage::RefreshBackgroundWork);
        }
    }

    fn stop_background_work(&self, key: BackgroundWorkKey, control_id: String) {
        if self.flavor.host_bridge_capable() {
            let _ = self
                .commands
                .send(CommandMessage::StopBackgroundWork { key, control_id });
        }
    }

    fn respond(&self, _request_id: String, _option_id: String) {}

    fn respond_user_input(&self, request_id: String, answers: Vec<UserInputAnswer>) {
        let Some(dialog) = self.dialogs.lock().remove(&request_id) else {
            return;
        };
        if dialog
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return;
        }
        let _ = self
            .commands
            .send(CommandMessage::ExtensionUiResponse(extension_ui_response(
                &request_id,
                &dialog,
                &answers,
            )));
    }

    fn goal(&self, operation: GoalOperation) {
        if self.flavor.host_bridge_capable() {
            let _ = self.commands.send(CommandMessage::Goal(operation));
        }
    }

    fn apply_options(&self, options: SessionOptions) -> bool {
        // Both flavors have setters for the model and thinking level, so those
        // apply to the live session. Neither exposes one for permissions — and
        // Waku only runs them with Full access anyway, so a mode change asks
        // for a fresh start, which is where that is reported.
        if options.mode != RuntimeMode::FullAccess {
            return false;
        }
        self.commands.send(CommandMessage::Options(options)).is_ok()
    }

    fn rollback(&self, turns: usize) -> anyhow::Result<Option<ProviderResumeCursor>> {
        if turns == 0 {
            return Ok(None);
        }
        let (response_tx, response_rx) = bounded(1);
        self.commands
            .send(CommandMessage::Rollback {
                turns,
                response: response_tx,
            })
            .with_context(|| {
                format!(
                    "{} driver stopped before rollback",
                    self.flavor.display_name()
                )
            })?;
        response_rx
            .recv_timeout(Duration::from_secs(60))
            .with_context(|| {
                format!(
                    "timed out waiting for {} conversation rollback",
                    self.flavor.display_name()
                )
            })?
            .map(Some)
            .map_err(anyhow::Error::msg)
    }

    fn fork(&self, turns_to_remove: usize) -> anyhow::Result<ProviderResumeCursor> {
        let (response_tx, response_rx) = bounded(1);
        self.commands
            .send(CommandMessage::Fork {
                turns_to_remove,
                response: response_tx,
            })
            .with_context(|| {
                format!(
                    "{} driver stopped before forking",
                    self.flavor.display_name()
                )
            })?;
        response_rx
            .recv_timeout(Duration::from_secs(60))
            .with_context(|| {
                format!(
                    "timed out waiting for {} conversation fork",
                    self.flavor.display_name()
                )
            })?
            .map_err(anyhow::Error::msg)
    }
}

impl Drop for PiDriver {
    fn drop(&mut self) {
        self.cancel_computer_use();
        let _ = self.commands.send(CommandMessage::Shutdown);
    }
}

fn send_request(
    stdin: &mut impl Write,
    pending: &PendingResponses,
    next_request_id: &mut u64,
    request: Value,
) -> Result<Value, String> {
    send_request_with_timeout(stdin, pending, next_request_id, request, RPC_TIMEOUT)
}

fn send_request_with_timeout(
    stdin: &mut impl Write,
    pending: &PendingResponses,
    next_request_id: &mut u64,
    request: Value,
    timeout: Duration,
) -> Result<Value, String> {
    send_tracked_request(
        stdin,
        pending,
        next_request_id,
        request,
        timeout,
        PendingResponse::Request,
    )
}

/// Sends an extension command (`/host-bridge …`, `/goal …`) as an ordinary
/// request. It must not touch the turn marker: a command sent while a user
/// prompt is in flight would otherwise orphan that prompt's response.
fn send_command_prompt(
    stdin: &mut impl Write,
    pending: &PendingResponses,
    next_request_id: &mut u64,
    message: &str,
    snapshot: bool,
) -> Result<Value, String> {
    send_tracked_request(
        stdin,
        pending,
        next_request_id,
        json!({"type": "prompt", "message": message}),
        RPC_TIMEOUT,
        if snapshot {
            PendingResponse::Snapshot
        } else {
            PendingResponse::Request
        },
    )
}

fn send_tracked_request(
    stdin: &mut impl Write,
    pending: &PendingResponses,
    next_request_id: &mut u64,
    mut request: Value,
    timeout: Duration,
    track: fn(Sender<Result<Value, String>>) -> PendingResponse,
) -> Result<Value, String> {
    *next_request_id += 1;
    let id = format!("waku-{}", next_request_id);
    request["id"] = Value::String(id.clone());
    let (response_tx, response_rx) = bounded(1);
    pending.lock().insert(id.clone(), track(response_tx));
    if let Err(error) = write_json_line(stdin, &request) {
        pending.lock().remove(&id);
        return Err(format!("transport write failed: {error}"));
    }
    match response_rx.recv_timeout(timeout) {
        Ok(response) => response,
        Err(_) => {
            pending.lock().remove(&id);
            Err(format!(
                "{} timed out after {}s",
                request["type"].as_str().unwrap_or("request"),
                timeout.as_secs()
            ))
        }
    }
}

fn send_prompt(
    stdin: &mut impl Write,
    pending: &PendingResponses,
    next_request_id: &mut u64,
    prompt: &str,
) -> Result<(), String> {
    *next_request_id += 1;
    let id = format!("waku-{}", next_request_id);
    {
        let mut pending = pending.lock();
        // A response from an older prompt cannot settle the next turn.
        pending.retain(|_, response| !response.is_prompt());
        pending.insert(id.clone(), PendingResponse::Prompt);
    }
    // OMP built-ins can hold the prompt response until compaction or another
    // command finishes. Do not apply the short control-RPC timeout or block
    // the writer from sending abort while waiting for that response.
    if let Err(error) = write_json_line(
        stdin,
        &json!({"id": id, "type": "prompt", "message": prompt}),
    ) {
        pending.lock().remove(&id);
        return Err(format!("transport write failed: {error}"));
    }
    Ok(())
}

fn write_json_line(writer: &mut impl Write, value: &Value) -> std::io::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn fail_pending(pending: &PendingResponses, message: &str) {
    for (_, response) in pending.lock().drain() {
        if let PendingResponse::Request(response) | PendingResponse::Snapshot(response) = response {
            let _ = response.send(Err(message.to_owned()));
        }
    }
}

fn catalog_has_command(catalog: &Value, name: &str) -> bool {
    catalog
        .pointer("/data/commands")
        .and_then(Value::as_array)
        .is_some_and(|commands| {
            commands
                .iter()
                .any(|command| command.get("name").and_then(Value::as_str) == Some(name))
        })
}

/// The host-bridge command that stops one item. IDs are positional
/// arguments, so anything that would split into more is refused here.
fn stop_command(
    key: &BackgroundWorkKey,
    control_id: &str,
    bridge_commands: bool,
) -> Result<String, String> {
    if !bridge_commands {
        return Err("the choco-pi host bridge is not available in this session".to_owned());
    }
    let kind = match key.kind {
        BackgroundWorkKind::Subagent => "subagent",
        BackgroundWorkKind::Process => "shell",
        BackgroundWorkKind::Monitor => {
            return Err("choco-pi has no monitors to stop".to_owned());
        }
    };
    if control_id.is_empty() || control_id.chars().any(char::is_whitespace) {
        return Err(format!("cannot stop work with the id `{control_id}`"));
    }
    Ok(format!("/host-bridge stop {kind} {control_id}"))
}

fn stop_via_bridge(
    stdin: &mut impl Write,
    pending: &PendingResponses,
    next_request_id: &mut u64,
    key: &BackgroundWorkKey,
    control_id: &str,
    bridge_commands: bool,
) -> Result<(), String> {
    let command = stop_command(key, control_id, bridge_commands)?;
    let response = send_command_prompt(stdin, pending, next_request_id, &command, false)?;
    require_handled(&response)
}

/// Prompt acceptance alone is not a stop: only a command the extension
/// handled counts, and the `stopped` entry then settles the item.
fn require_handled(response: &Value) -> Result<(), String> {
    match response
        .pointer("/data/disposition")
        .and_then(Value::as_str)
    {
        Some("handled") => Ok(()),
        disposition => Err(format!(
            "choco-pi did not handle the stop request ({})",
            disposition.unwrap_or("no disposition")
        )),
    }
}

/// The `/goal` commands that carry out `operation`. Waku's status vocabulary
/// is Codex's; choco-pi can only pause, resume, and complete.
fn goal_commands(operation: &GoalOperation, goal_command: bool) -> Result<Vec<String>, String> {
    let mut commands = Vec::new();
    match operation {
        GoalOperation::Refresh => commands.push(HOST_BRIDGE_SNAPSHOT.to_owned()),
        GoalOperation::Clear => commands.push("/goal clear".to_owned()),
        GoalOperation::Set {
            objective,
            status,
            replace,
        } => {
            let objective = objective
                .as_deref()
                .map(str::trim)
                .filter(|objective| !objective.is_empty());
            if let Some(objective) = objective {
                if *replace {
                    commands.push("/goal clear".to_owned());
                }
                commands.push(format!("/goal {objective}"));
            }
            match status {
                None => {}
                // A goal just created is already active.
                Some(ThreadGoalStatus::Active) if objective.is_some() => {}
                Some(ThreadGoalStatus::Active) => commands.push("/goal resume".to_owned()),
                Some(ThreadGoalStatus::Paused) => commands.push("/goal pause".to_owned()),
                Some(ThreadGoalStatus::Complete) => {
                    commands.push("/host-bridge goal complete".to_owned());
                }
                Some(
                    status @ (ThreadGoalStatus::Blocked
                    | ThreadGoalStatus::UsageLimited
                    | ThreadGoalStatus::BudgetLimited),
                ) => {
                    return Err(format!("choco-pi cannot set a goal to {status:?}"));
                }
            }
        }
    }
    if !goal_command && commands.iter().any(|command| command.starts_with("/goal")) {
        return Err("this choco-pi session has no /goal command".to_owned());
    }
    Ok(commands)
}

/// The question a forwarded dialog asks, or `None` when it cannot be
/// answered (a select with no options) and should be cancelled.
fn dialog_question(method: &str, value: &Value) -> Option<(PendingDialog, UserInputQuestion)> {
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .unwrap_or("choco-pi")
        .to_owned();
    let (kind, question, options) = match method {
        "confirm" => (
            DialogKind::Confirm,
            value
                .get("message")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|message| !message.is_empty())
                .map_or_else(|| title.clone(), str::to_owned),
            vec![CONFIRM_YES.to_owned(), CONFIRM_NO.to_owned()],
        ),
        "select" => {
            let options = value
                .get("options")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|option| {
                    option
                        .as_str()
                        .or_else(|| option.get("label").and_then(Value::as_str))
                })
                .map(str::to_owned)
                .collect::<Vec<_>>();
            if options.is_empty() {
                return None;
            }
            (DialogKind::Select, title.clone(), options)
        }
        "input" | "editor" => (DialogKind::Text, title.clone(), Vec::new()),
        _ => return None,
    };
    let deadline = value
        .get("timeout")
        .and_then(Value::as_u64)
        .filter(|timeout| *timeout > 0)
        .map(|timeout| Instant::now() + Duration::from_millis(timeout));
    let question = UserInputQuestion {
        id: DIALOG_QUESTION_ID.to_owned(),
        header: title,
        question,
        options: options
            .iter()
            .map(|label| UserInputOption {
                label: label.clone(),
                description: None,
            })
            .collect(),
        multi_select: false,
    };
    let options = match kind {
        // Only a select has to answer with one of its own labels.
        DialogKind::Select => options,
        DialogKind::Confirm | DialogKind::Text => Vec::new(),
    };
    Some((
        PendingDialog {
            kind,
            options,
            deadline,
        },
        question,
    ))
}

/// Pi's reply shape for each dialog kind. An answer the dialog cannot
/// accept (none, or a select label it never offered) cancels instead.
fn extension_ui_response(id: &str, dialog: &PendingDialog, answers: &[UserInputAnswer]) -> Value {
    let answer = answers
        .iter()
        .find(|answer| answer.question_id == DIALOG_QUESTION_ID)
        .or_else(|| answers.first())
        .and_then(|answer| answer.answers.first())
        .map(String::as_str);
    match (dialog.kind, answer) {
        (DialogKind::Confirm, Some(CONFIRM_YES)) => {
            json!({"type": "extension_ui_response", "id": id, "confirmed": true})
        }
        (DialogKind::Confirm, Some(CONFIRM_NO)) => {
            json!({"type": "extension_ui_response", "id": id, "confirmed": false})
        }
        (DialogKind::Select, Some(label))
            if dialog.options.iter().any(|option| option == label) =>
        {
            json!({"type": "extension_ui_response", "id": id, "value": label})
        }
        (DialogKind::Text, Some(text)) => {
            json!({"type": "extension_ui_response", "id": id, "value": text})
        }
        _ => json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
    }
}

fn parse_model_slug(model: &str) -> anyhow::Result<(&str, &str)> {
    let Some((provider, model_id)) = model.trim().split_once('/') else {
        return Err(anyhow!(
            "models must use provider/model format; received `{model}`"
        ));
    };
    if provider.is_empty() || model_id.is_empty() {
        return Err(anyhow!(
            "models must use provider/model format; received `{model}`"
        ));
    }
    Ok((provider, model_id))
}

fn cursor_from_state(flavor: PiFlavor, response: &Value) -> Option<ProviderResumeCursor> {
    let session_id = response
        .pointer("/data/sessionId")
        .and_then(Value::as_str)?;
    let session_file = response
        .pointer("/data/sessionFile")
        .and_then(Value::as_str)
        .map(PathBuf::from);
    Some(flavor.cursor(session_id.to_owned(), session_file))
}

/// Reassembles the `rpc_chunk` runs Oh My Pi emits for frames over its 1 MiB
/// stdout ceiling. Without this a large tool result degrades to an error frame
/// and the activity row renders empty.
#[derive(Default)]
struct ChunkAssembly {
    active: Option<PendingChunks>,
}

struct PendingChunks {
    chunk_id: String,
    count: u64,
    next_index: u64,
    byte_length: usize,
    data: Vec<u8>,
}

impl ChunkAssembly {
    /// Returns the logical message to dispatch, or `None` while a chunked
    /// frame is still arriving.
    fn accept(&mut self, value: Value) -> Result<Option<Value>, String> {
        if value.get("type").and_then(Value::as_str) != Some("rpc_chunk") {
            // The run must be uninterrupted, so anything else invalidates a
            // partial frame rather than silently splicing around it.
            if self.active.take().is_some() {
                return Err("chunked frame was interrupted".to_owned());
            }
            return Ok(Some(value));
        }
        let (chunk_id, index, count, byte_length, data) = (|| {
            Some((
                value.get("chunkId").and_then(Value::as_str)?,
                value.get("index").and_then(Value::as_u64)?,
                value.get("count").and_then(Value::as_u64)?,
                value.get("byteLength").and_then(Value::as_u64)?,
                value.get("data").and_then(Value::as_str)?,
            ))
        })()
        .ok_or_else(|| "chunk frame was malformed".to_owned())?;
        let byte_length = usize::try_from(byte_length)
            .map_err(|_| "chunked frame exceeds the reassembly limit".to_owned())?;
        if count == 0 || index >= count {
            self.active = None;
            return Err("chunk frame was malformed".to_owned());
        }
        if byte_length > MAX_REASSEMBLED_FRAME_BYTES {
            self.active = None;
            return Err("chunked frame exceeds the reassembly limit".to_owned());
        }
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)
            .map_err(|error| format!("chunk payload was not valid base64: {error}"))?;

        let pending = match self.active.take() {
            Some(pending)
                if pending.chunk_id == chunk_id
                    && pending.count == count
                    && pending.byte_length == byte_length
                    && pending.next_index == index =>
            {
                pending
            }
            Some(_) => {
                return Err("chunked frame was interrupted".to_owned());
            }
            None if index == 0 => PendingChunks {
                chunk_id: chunk_id.to_owned(),
                count,
                next_index: 0,
                byte_length,
                data: Vec::with_capacity(byte_length),
            },
            None => return Err("chunked frame started mid-sequence".to_owned()),
        };
        let mut pending = pending;
        pending.data.extend_from_slice(&decoded);
        pending.next_index += 1;
        if pending.data.len() > pending.byte_length {
            return Err("chunked frame overran its declared length".to_owned());
        }
        if pending.next_index < pending.count {
            self.active = Some(pending);
            return Ok(None);
        }
        if pending.data.len() != pending.byte_length {
            return Err("chunked frame did not match its declared length".to_owned());
        }
        let text = String::from_utf8(pending.data)
            .map_err(|_| "chunked frame was not valid UTF-8".to_owned())?;
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|error| format!("chunked frame was not valid JSON: {error}"))
    }
}

/// Pi already computes context occupancy for its own footer. Prefer that
/// native value when session stats are available, and use the active model in
/// `get_state` for the window before the first assistant message arrives.
fn pi_context_usage(state: &Value, stats: Option<&Value>) -> Option<(Option<u64>, Option<u64>)> {
    let context = stats.and_then(|stats| stats.pointer("/data/contextUsage"));
    let tokens = context
        .and_then(|context| context.get("tokens"))
        .and_then(Value::as_u64);
    let window = context
        .and_then(|context| context.get("contextWindow"))
        .and_then(Value::as_u64)
        .or_else(|| {
            state
                .pointer("/data/model/contextWindow")
                .and_then(Value::as_u64)
        })
        .filter(|window| *window > 0);
    (tokens.is_some() || window.is_some()).then_some((tokens, window))
}

/// Pi's providers normally fill `totalTokens`, but Pi itself deliberately
/// falls back to the four component counters when a provider leaves it zero.
/// Keep Waku's meter aligned with that provider-native calculation.
fn pi_message_context_tokens(message: &Value) -> Option<u64> {
    let usage = message.get("usage")?;
    usage
        .get("totalTokens")
        .and_then(Value::as_u64)
        .filter(|tokens| *tokens > 0)
        .or_else(|| {
            let total = ["input", "output", "cacheRead", "cacheWrite"]
                .into_iter()
                .filter_map(|field| usage.get(field).and_then(Value::as_u64))
                .fold(0_u64, u64::saturating_add);
            (total > 0).then_some(total)
        })
}

#[allow(clippy::too_many_arguments)]
fn fork_pi_session(
    flavor: PiFlavor,
    stdin: &mut impl Write,
    pending: &PendingResponses,
    next_request_id: &mut u64,
    binary: &Path,
    cwd: &Path,
    original_cursor: &ProviderResumeCursor,
    turns_to_remove: usize,
    restore_original: bool,
) -> Result<ProviderResumeCursor, String> {
    let name = flavor.display_name();
    let messages = send_request(
        stdin,
        pending,
        next_request_id,
        json!({"type": flavor.branch_messages_command()}),
    )?;
    let messages = messages
        .pointer("/data/messages")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{name} returned an invalid fork-message list"))?;

    // Keeping every turn is a whole-session copy, which Pi does in place and
    // Oh My Pi only does at launch. The out-of-process copy leaves this
    // session untouched, so it never needs restoring afterwards.
    if turns_to_remove == 0 && flavor == PiFlavor::OhMyPi {
        let session_file = flavor
            .session_file_from_cursor(original_cursor)
            .ok_or_else(|| format!("{name}'s original session file is unavailable"))?;
        return clone_ohmypi_session(binary, cwd, session_file);
    }

    let request = pi_fork_request(flavor, messages, turns_to_remove)?;
    let fork = send_request(stdin, pending, next_request_id, request)?;
    if fork.pointer("/data/cancelled").and_then(Value::as_bool) == Some(true) {
        return Err(format!("{name} session fork was cancelled"));
    }
    let fork_state = send_request(
        stdin,
        pending,
        next_request_id,
        json!({"type": "get_state"}),
    )?;
    let fork_cursor = cursor_from_state(flavor, &fork_state)
        .ok_or_else(|| format!("{name} did not report the forked session cursor"))?;

    if restore_original {
        let session_file = flavor
            .session_file_from_cursor(original_cursor)
            .ok_or_else(|| format!("{name}'s original session file is unavailable"))?;
        let switched = send_request(
            stdin,
            pending,
            next_request_id,
            json!({
                "type": "switch_session",
                "sessionPath": session_file
            }),
        )?;
        if switched.pointer("/data/cancelled").and_then(Value::as_bool) == Some(true) {
            return Err(format!(
                "{name} could not return to the source session after forking"
            ));
        }
        let restored_state = send_request(
            stdin,
            pending,
            next_request_id,
            json!({"type": "get_state"}),
        )?;
        let restored_cursor = cursor_from_state(flavor, &restored_state)
            .ok_or_else(|| format!("{name} did not report the restored source session"))?;
        if restored_cursor.native_id() != original_cursor.native_id() {
            return Err(format!(
                "{name} returned to the wrong source session after forking"
            ));
        }
    }

    Ok(fork_cursor)
}

fn pi_fork_request(
    flavor: PiFlavor,
    messages: &[Value],
    turns_to_remove: usize,
) -> Result<Value, String> {
    let name = flavor.display_name();
    if turns_to_remove > messages.len() {
        return Err(format!(
            "{name} has only {} native turns, but Waku needs to remove {turns_to_remove}",
            messages.len()
        ));
    }
    let retained_turns = messages.len() - turns_to_remove;
    if turns_to_remove == 0 {
        // Only reachable for Pi; Oh My Pi copies out of process instead.
        Ok(json!({"type": "clone"}))
    } else {
        let entry_id = messages
            .get(retained_turns)
            .and_then(|message| message.get("entryId"))
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{name} returned a fork message without an entry ID"))?;
        Ok(json!({"type": flavor.branch_command(), "entryId": entry_id}))
    }
}

/// Copies a whole Oh My Pi session by launching a throwaway agent with
/// `--fork`, which is the only place it exposes a full-session copy, then
/// reading back the session the copy landed in.
fn clone_ohmypi_session(
    binary: &Path,
    cwd: &Path,
    session_file: &Path,
) -> Result<ProviderResumeCursor, String> {
    let mut command = crate::command_env::command(binary);
    let command = command
        .args(["--mode", "rpc"])
        .arg(PiFlavor::OhMyPi.full_access_arg())
        .arg("--fork")
        .arg(session_file)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = crate::command_env::spawn(command)
        .map_err(|error| format!("could not start Oh My Pi to copy the session: {error}"))?;
    let result = (|| {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| "Oh My Pi stdin unavailable".to_owned())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "Oh My Pi stdout unavailable".to_owned())?;
        let (tx, rx) = bounded(1);
        thread::Builder::new()
            .name("waku-ohmypi-clone".into())
            .spawn(move || {
                let mut chunks = ChunkAssembly::default();
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    let Ok(value) = serde_json::from_str::<Value>(&line) else {
                        continue;
                    };
                    let Ok(Some(value)) = chunks.accept(value) else {
                        continue;
                    };
                    if value.get("id").and_then(Value::as_str) == Some("waku-clone") {
                        let _ = tx.send(value);
                        break;
                    }
                }
            })
            .map_err(|error| format!("could not read the Oh My Pi session copy: {error}"))?;
        write_json_line(
            &mut stdin,
            &json!({"id": "waku-clone", "type": "get_state"}),
        )
        .map_err(|error| format!("could not ask Oh My Pi for the copied session: {error}"))?;
        let state = rx
            .recv_timeout(CLONE_TIMEOUT)
            .map_err(|_| "timed out waiting for Oh My Pi to copy the session".to_owned())?;
        if state.get("success").and_then(Value::as_bool) != Some(true) {
            return Err(state
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Oh My Pi could not copy the session")
                .to_owned());
        }
        cursor_from_state(PiFlavor::OhMyPi, &state)
            .ok_or_else(|| "Oh My Pi did not report the copied session cursor".to_owned())
    })();
    let _ = child.kill();
    let _ = child.wait();
    result
}

#[derive(Default)]
struct PiStreamState {
    run_started: bool,
    message_saw_text: bool,
    message_saw_reasoning: bool,
    failed: bool,
    tools: HashMap<String, (ActivityKind, String)>,
    tool_updates: HashMap<String, ToolUpdateGate>,
    /// Session-scoped: survives the per-run resets below.
    bridge: Option<HostBridgeState>,
    dialogs: PendingDialogs,
    bridge_live: Arc<AtomicBool>,
}

/// Coalesces one tool's `tool_execution_update` stream to the latest frame
/// per [`TOOL_UPDATE_DEBOUNCE`].
#[derive(Default)]
struct ToolUpdateGate {
    last_emit: Option<Instant>,
    pending: Option<Value>,
}

impl PiStreamState {
    fn new(
        host_bridge: Option<HostBridgeSpec>,
        dialogs: PendingDialogs,
        bridge_live: Arc<AtomicBool>,
    ) -> Self {
        Self {
            bridge: host_bridge.map(HostBridgeState::new),
            dialogs,
            bridge_live,
            ..Self::default()
        }
    }

    /// Clears what belongs to one run and keeps what belongs to the session.
    fn reset_run(&mut self) {
        *self = Self {
            bridge: self.bridge.take(),
            dialogs: std::mem::take(&mut self.dialogs),
            bridge_live: std::mem::take(&mut self.bridge_live),
            ..Self::default()
        };
    }

    fn publish_bridge_events(&self, events: Vec<DriverEvent>, sink: &impl DriverEventSink) {
        if let Some(bridge) = self.bridge.as_ref() {
            self.bridge_live.store(bridge.has_live(), Ordering::Release);
        }
        for event in events {
            let _ = sink.send(event);
        }
    }
}

fn handle_pi_message(
    flavor: PiFlavor,
    value: Value,
    pending: &PendingResponses,
    commands: &Sender<CommandMessage>,
    events: &impl DriverEventSink,
    state: &mut PiStreamState,
) {
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if flavor.debounces_tool_updates() && !state.tool_updates.is_empty() {
        let current_update = (event_type == "tool_execution_update")
            .then(|| value.get("toolCallId").and_then(Value::as_str))
            .flatten();
        flush_due_tool_updates(flavor, current_update, events, state);
    }
    if event_type == "response" {
        let Some(id) = value.get("id").and_then(Value::as_str) else {
            return;
        };
        let prompt_response = matches!(pending.lock().get(id), Some(PendingResponse::Prompt));
        if prompt_response {
            let success = value.get("success").and_then(Value::as_bool) == Some(true);
            // Pi reports an extension command that never reached the model
            // as `handled`; no run will follow to settle the turn. Only
            // choco-pi is verified to rely on this (its commands are many).
            // A command that queued a model message may already have started
            // that run; then the run's own settle ends the turn. One that
            // starts later opens an unprompted turn of its own.
            let handled_locally = flavor.starts_turns_unprompted()
                && !state.run_started
                && value.pointer("/data/disposition").and_then(Value::as_str) == Some("handled");
            let local_only = handled_locally
                || value.pointer("/data/agentInvoked").and_then(Value::as_bool) == Some(false);
            if success && !local_only {
                // OMP may acknowledge a prompt before reporting that an
                // extension handled it locally. Retain the id for that second
                // response; normal agent completion retires it below.
                return;
            }
            pending.lock().remove(id);
            if !state.run_started {
                let _ = events.send(DriverEvent::TurnStarted);
            }
            let error = (!success).then(|| {
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("{} RPC command failed", flavor.display_name()))
            });
            if let Some(error) = error.as_ref() {
                let _ = events.send(DriverEvent::Error(error.clone()));
            }
            let _ = events.send(DriverEvent::TurnFinished {
                success,
                summary: error,
            });
            state.reset_run();
            return;
        }
        let removed = pending.lock().remove(id);
        let response = match removed {
            Some(PendingResponse::Request(response)) => response,
            Some(PendingResponse::Snapshot(response)) => {
                settle_snapshot(id, &value, events, state);
                response
            }
            Some(PendingResponse::Prompt) => return,
            None => {
                // The writer gave up waiting, but what the snapshot already
                // streamed is still current once the extension answers.
                if state
                    .bridge
                    .as_ref()
                    .is_some_and(|bridge| bridge.snapshot_request() == Some(id))
                {
                    settle_snapshot(id, &value, events, state);
                }
                return;
            }
        };
        if value.get("success").and_then(Value::as_bool) == Some(true) {
            let _ = response.send(Ok(value));
        } else {
            let error = value
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{} RPC command failed", flavor.display_name()));
            let _ = response.send(Err(error));
        }
        return;
    }

    if event_type == "available_commands_update" {
        publish_commands(
            crate::slash_command_catalog::parse_oh_my_pi_commands(&value),
            events,
        );
        return;
    }

    if event_type == flavor.session_info_event() {
        let title = value
            .get(flavor.session_info_title_field())
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(str::to_owned);
        let _ = events.send(DriverEvent::AutoTitleUpdated(title));
        return;
    }

    // Oh My Pi reuses `agent_end` for intermediate settles, flagging the real
    // one with `isTerminal`. Anything else here would end the turn early while
    // maintenance or async delivery still has work queued.
    if event_type == flavor.settled_event() {
        if value.get("isTerminal").and_then(Value::as_bool) == Some(false) {
            return;
        }
        if state.run_started {
            pending.lock().retain(|_, response| !response.is_prompt());
            let success = !state.failed;
            let _ = events.send(DriverEvent::TurnFinished {
                success,
                summary: (!success).then(|| {
                    tr!(
                        "errors.provider_complete_turn",
                        provider = flavor.display_name()
                    )
                }),
            });
        }
        state.reset_run();
        return;
    }

    match event_type {
        "entry_appended" => {
            let Some(entry) = value.get("entry") else {
                return;
            };
            let Some(bridge) = state
                .bridge
                .as_mut()
                .filter(|bridge| bridge.owns_entry(entry))
            else {
                return;
            };
            let mut bridge_events = sync_snapshot(bridge, pending);
            bridge_events.extend(bridge.apply_entry(entry));
            state.publish_bridge_events(bridge_events, events);
        }
        "command_output" => {
            if let Some(text) = value
                .get("text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                if !state.run_started {
                    state.run_started = true;
                    let _ = events.send(DriverEvent::TurnStarted);
                }
                // Each command_output is a complete output block, unlike
                // assistant deltas. Keep successive reports separated.
                let _ = events.send(DriverEvent::TextDelta(format!("{text}\n\n")));
            }
        }
        "agent_start" | "turn_start" => {
            if !state.run_started {
                state.run_started = true;
                state.failed = false;
                let _ = events.send(DriverEvent::TurnStarted);
            }
        }
        "message_start" => {
            if value.pointer("/message/role").and_then(Value::as_str) == Some("assistant") {
                state.message_saw_text = false;
                state.message_saw_reasoning = false;
            }
        }
        "message_update" => {
            let update = value.get("assistantMessageEvent").unwrap_or(&Value::Null);
            match update.get("type").and_then(Value::as_str) {
                Some("text_delta") => {
                    if let Some(delta) = update
                        .get("delta")
                        .and_then(Value::as_str)
                        .filter(|delta| !delta.is_empty())
                    {
                        state.message_saw_text = true;
                        let _ = events.send(DriverEvent::TextDelta(delta.to_owned()));
                    }
                }
                Some("thinking_delta") => {
                    if let Some(delta) = update
                        .get("delta")
                        .and_then(Value::as_str)
                        .filter(|delta| !delta.is_empty())
                    {
                        state.message_saw_reasoning = true;
                        let _ = events.send(DriverEvent::ReasoningDelta(delta.to_owned()));
                    }
                }
                Some("error") => {
                    state.failed = true;
                    let _ = events.send(DriverEvent::Error(pi_error_message(flavor, update)));
                }
                _ => {}
            }
        }
        "message_end" => {
            if value.pointer("/message/role").and_then(Value::as_str) == Some("assistant") {
                // This is the context the next call starts from, not the
                // cumulative billed total for the whole session.
                if let Some(tokens) = value.get("message").and_then(pi_message_context_tokens) {
                    let _ = events.send(DriverEvent::UsageUpdated {
                        context_tokens: Some(tokens),
                        context_window: None,
                    });
                }
                emit_completed_message_fallback(value.get("message"), events, state);
            }
        }
        "tool_execution_start" | "tool_execution_update" | "tool_execution_end" => {
            if flavor.debounces_tool_updates()
                && let Some(id) = value.get("toolCallId").and_then(Value::as_str)
            {
                match event_type {
                    "tool_execution_update" => {
                        let now = Instant::now();
                        let gate = state.tool_updates.entry(id.to_owned()).or_default();
                        if gate
                            .last_emit
                            .is_some_and(|last| now.duration_since(last) < TOOL_UPDATE_DEBOUNCE)
                        {
                            gate.pending = Some(value);
                            return;
                        }
                        gate.last_emit = Some(now);
                        gate.pending = None;
                    }
                    "tool_execution_end" => {
                        // The last progress frame still lands before the end.
                        if let Some(pending) =
                            state.tool_updates.remove(id).and_then(|gate| gate.pending)
                        {
                            emit_tool_event(flavor, &pending, events, state);
                        }
                    }
                    _ => {}
                }
            }
            emit_tool_event(flavor, &value, events, state);
        }
        "auto_retry_end" => {
            if value.get("success").and_then(Value::as_bool) == Some(true) {
                state.failed = false;
            } else {
                state.failed = true;
                let message = value
                    .get("finalError")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        format!("{} exhausted its automatic retries", flavor.display_name())
                    });
                let _ = events.send(DriverEvent::Error(message));
            }
        }
        "extension_ui_request" if flavor.forwards_extension_ui() => {
            forward_extension_ui(&value, pending, commands, events, state);
        }
        "extension_ui_request" => {
            let method = value.get("method").and_then(Value::as_str);
            let id = value.get("id").and_then(Value::as_str);
            if matches!(method, Some("select" | "confirm" | "input" | "editor"))
                && let Some(id) = id
            {
                let _ = commands.send(CommandMessage::CancelExtensionRequest(id.to_owned()));
            }
        }
        "extension_error" => {
            let _ = events.send(DriverEvent::Error(pi_error_message(flavor, &value)));
        }
        _ => {}
    }
}

fn emit_tool_event(
    flavor: PiFlavor,
    value: &Value,
    events: &impl DriverEventSink,
    state: &mut PiStreamState,
) {
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let id = value
        .get("toolCallId")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let tool_name = value.get("toolName").and_then(Value::as_str);
    // choco-pi's `Agent` streams a progress record, not output.
    let agent_progress = flavor == PiFlavor::ChocoPi
        && tool_name.is_some_and(|name| name.eq_ignore_ascii_case("agent"));
    let (kind, mut title) = id
        .as_ref()
        .and_then(|id| state.tools.get(id))
        .cloned()
        .unwrap_or_else(|| {
            tool_name
                .map(|tool_name| (classify_tool(tool_name), tool_title(tool_name)))
                .unwrap_or_else(|| (ActivityKind::Tool, tr!("activity.tool")))
        });
    if event_type == "tool_execution_start" {
        if let Some(input_title) = activity::input_title(value.get("args")) {
            title = input_title;
        } else if agent_progress
            && let Some(description) = value
                .pointer("/args/description")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|description| !description.is_empty())
        {
            title = description.to_owned();
        }
    }
    if event_type == "tool_execution_start"
        && let Some(id) = id.as_ref()
    {
        state.tools.insert(id.clone(), (kind, title.clone()));
    }
    let arguments = (event_type == "tool_execution_start")
        .then(|| value.get("args"))
        .flatten();
    let output = match event_type {
        "tool_execution_update" if agent_progress => None,
        "tool_execution_update" => value.get("partialResult"),
        "tool_execution_end" => value.get("result"),
        _ => None,
    };
    let complete = event_type == "tool_execution_end";
    let failed = value.get("isError").and_then(Value::as_bool) == Some(true);
    let mut item = activity::tool_activity(
        id.clone(),
        kind,
        title,
        arguments,
        output,
        output,
        failed,
        complete,
    )
    .with_tool_name(tool_name);
    if agent_progress && event_type == "tool_execution_update" {
        item.detail = value
            .pointer("/partialResult/details")
            .and_then(agent_progress_detail);
    }
    let _ = events.send(DriverEvent::RichActivity(item));
    if complete && let Some(id) = id {
        state.tools.remove(&id);
    }
}

/// Emits held-back progress whose debounce window has passed, so a tool
/// that goes quiet still shows its latest state at the next frame. The
/// tool whose update is being handled right now decides for itself.
fn flush_due_tool_updates(
    flavor: PiFlavor,
    current_update: Option<&str>,
    events: &impl DriverEventSink,
    state: &mut PiStreamState,
) {
    let now = Instant::now();
    let due = state
        .tool_updates
        .iter_mut()
        .filter(|(id, gate)| {
            Some(id.as_str()) != current_update
                && gate.pending.is_some()
                && gate
                    .last_emit
                    .is_none_or(|last| now.duration_since(last) >= TOOL_UPDATE_DEBOUNCE)
        })
        .filter_map(|(_, gate)| {
            gate.last_emit = Some(now);
            gate.pending.take()
        })
        .collect::<Vec<_>>();
    for update in due {
        emit_tool_event(flavor, &update, events, state);
    }
}

/// One line for an `Agent` progress record:
/// `"<toolUses> tool uses · <tokens> · <activity>"`.
fn agent_progress_detail(details: &Value) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(tool_uses) = details.get("toolUses").and_then(Value::as_u64) {
        parts.push(format!("{tool_uses} tool uses"));
    }
    match details.get("tokens") {
        Some(Value::String(tokens)) if !tokens.trim().is_empty() => {
            parts.push(tokens.trim().to_owned());
        }
        Some(tokens) => {
            if let Some(tokens) = tokens.as_u64() {
                parts.push(format!("{tokens} tokens"));
            }
        }
        None => {}
    }
    if let Some(activity) = details
        .get("activity")
        .and_then(Value::as_str)
        .and_then(pi_host_bridge::one_line)
    {
        parts.push(activity);
    }
    (!parts.is_empty()).then(|| parts.join(" · "))
}

/// choco-pi's dialogs reach the user; its status frames feed the bridge.
fn forward_extension_ui(
    value: &Value,
    pending: &PendingResponses,
    commands: &Sender<CommandMessage>,
    events: &impl DriverEventSink,
    state: &mut PiStreamState,
) {
    let Some(method) = value.get("method").and_then(Value::as_str) else {
        return;
    };
    match method {
        "setStatus" => {
            let Some(bridge) = state.bridge.as_mut().filter(|bridge| {
                value.get("statusKey").and_then(Value::as_str) == Some(bridge.status_key())
            }) else {
                return;
            };
            let Some(text) = value.get("statusText").and_then(Value::as_str) else {
                return;
            };
            let bridge_events = bridge.apply_status(text);
            state.publish_bridge_events(bridge_events, events);
        }
        "notify" => {
            if value.get("notifyType").and_then(Value::as_str) == Some("error")
                && let Some(message) = value
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|message| !message.is_empty())
            {
                let _ = events.send(DriverEvent::Error(message.to_owned()));
            }
        }
        "confirm" | "select" | "input" | "editor" => {
            let Some(id) = value.get("id").and_then(Value::as_str) else {
                return;
            };
            // The client only shows questions inside a turn. A dialog raised
            // outside one (choco-pi-goal asks to resume a paused goal from
            // `session_start`, which `switch_session` awaits) would never be
            // answered and would stall the handshake, so it is cancelled as
            // plain Pi does.
            let turn_open =
                state.run_started || pending.lock().values().any(PendingResponse::is_prompt);
            let question = turn_open.then(|| dialog_question(method, value)).flatten();
            let Some((dialog, question)) = question else {
                let _ = commands.send(CommandMessage::CancelExtensionRequest(id.to_owned()));
                return;
            };
            {
                let mut dialogs = state.dialogs.lock();
                let now = Instant::now();
                dialogs.retain(|_, open| open.deadline.is_none_or(|deadline| deadline > now));
                dialogs.insert(id.to_owned(), dialog);
            }
            let _ = events.send(DriverEvent::UserInputRequested {
                request_id: id.to_owned(),
                questions: vec![question],
            });
        }
        // Widgets, titles, and editor text have no Waku surface.
        _ => {}
    }
}

/// Starts collecting for the snapshot now in flight, or abandons one whose
/// request timed out. Entries are what reveal it: the writer registers the
/// request before writing it, so it is visible by the time they arrive.
fn sync_snapshot(bridge: &mut HostBridgeState, pending: &PendingResponses) -> Vec<DriverEvent> {
    let active = pending.lock().iter().find_map(|(id, response)| {
        matches!(response, PendingResponse::Snapshot(_)).then(|| id.clone())
    });
    match active {
        Some(id) if bridge.snapshot_request() != Some(id.as_str()) => {
            let events = bridge.cancel_snapshot();
            bridge.begin_snapshot(id);
            events
        }
        None if bridge.snapshot_request().is_some() => bridge.cancel_snapshot(),
        _ => Vec::new(),
    }
}

fn settle_snapshot(
    id: &str,
    response: &Value,
    events: &impl DriverEventSink,
    state: &mut PiStreamState,
) {
    let Some(bridge) = state.bridge.as_mut() else {
        return;
    };
    let handled = response.get("success").and_then(Value::as_bool) == Some(true)
        && response
            .pointer("/data/disposition")
            .and_then(Value::as_str)
            == Some("handled");
    let bridge_events = if handled {
        if bridge.snapshot_request() != Some(id) {
            // Nothing streamed: no goal entry and no live work.
            bridge.begin_snapshot(id.to_owned());
        }
        bridge.finish_snapshot()
    } else if bridge.snapshot_request() == Some(id) {
        bridge.cancel_snapshot()
    } else {
        Vec::new()
    };
    state.publish_bridge_events(bridge_events, events);
}

fn publish_commands(
    commands: Vec<waku_protocol::composer::SlashCommand>,
    events: &impl DriverEventSink,
) {
    let commands = commands
        .into_iter()
        .map(|command| ReportedCommand {
            name: command.name,
            description: command.description,
        })
        .collect();
    let _ = events.send(DriverEvent::AvailableCommands(commands));
}

fn emit_completed_message_fallback(
    message: Option<&Value>,
    events: &impl DriverEventSink,
    state: &mut PiStreamState,
) {
    let Some(content) = message
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return;
    };
    for block in content {
        match block.get("type").and_then(Value::as_str) {
            Some("text") if !state.message_saw_text => {
                if let Some(text) = block
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    state.message_saw_text = true;
                    let _ = events.send(DriverEvent::TextDelta(text.to_owned()));
                }
            }
            Some("thinking") if !state.message_saw_reasoning => {
                if let Some(thinking) = block
                    .get("thinking")
                    .and_then(Value::as_str)
                    .filter(|thinking| !thinking.is_empty())
                {
                    state.message_saw_reasoning = true;
                    let _ = events.send(DriverEvent::ReasoningDelta(thinking.to_owned()));
                }
            }
            _ => {}
        }
    }
}

fn pi_error_message(flavor: PiFlavor, value: &Value) -> String {
    value
        .get("error")
        .and_then(Value::as_str)
        .or_else(|| value.get("errorMessage").and_then(Value::as_str))
        .or_else(|| value.get("reason").and_then(Value::as_str))
        .map(str::to_owned)
        .unwrap_or_else(|| {
            tr!(
                "errors.provider_reported_error",
                provider = flavor.display_name()
            )
        })
}

fn classify_tool(name: &str) -> ActivityKind {
    ActivityKind::from_tool_name(name)
}

fn tool_title(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "bash" => tr!("activity.run_command"),
        "edit" => tr!("activity.edit_file"),
        "write" => tr!("activity.write_file"),
        "read" => tr!("activity.read_file"),
        "grep" => tr!("activity.search_files"),
        "find" => tr!("activity.find_files"),
        "ls" => tr!("activity.list_files"),
        "exec" | "exec_command" | "shell_start" => tr!("activity.run_command"),
        "read_symbol" | "read_enclosing" => tr!("activity.read_file"),
        "symbol_search" | "ast_grep_search" => tr!("activity.search_files"),
        "agent" => tr!("background.subagent"),
        _ => name.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::TryRecvError;

    #[test]
    fn live_command_updates_reach_the_composer_and_clear_removed_commands() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        for entries in [
            json!([
                {"name": "compact", "source": "builtin", "description": "Compact context"},
                {"name": "skill:verify", "source": "skill", "description": "Verify changes"}
            ]),
            json!([]),
        ] {
            handle_pi_message(
                PiFlavor::OhMyPi,
                json!({"type": "available_commands_update", "commands": entries}),
                &pending,
                &commands,
                &events,
                &mut state,
            );
        }
        let DriverEvent::AvailableCommands(reported) = event_rx.recv().unwrap() else {
            panic!("missing command update")
        };
        assert_eq!(
            reported
                .iter()
                .map(|command| command.name.as_str())
                .collect::<Vec<_>>(),
            ["compact", "verify"]
        );
        assert_eq!(reported[0].description, "Compact context");
        assert!(
            matches!(event_rx.recv().unwrap(), DriverEvent::AvailableCommands(commands) if commands.is_empty())
        );
        assert!(!state.run_started);
    }

    #[test]
    fn local_slash_command_output_and_completion_do_not_need_an_agent_turn() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        send_prompt(&mut Vec::new(), &pending, &mut 0, "/context").unwrap();
        for frame in [
            json!({"type": "response", "id": "waku-1", "command": "prompt", "success": true}),
            json!({"type": "command_output", "text": "Available commands\n"}),
            json!({"type": "command_output", "text": "/compact"}),
            json!({"type": "response", "id": "waku-1", "command": "prompt", "success": true, "data": {"agentInvoked": false}}),
            json!({"type": "agent_end", "isTerminal": true}),
        ] {
            handle_pi_message(
                PiFlavor::OhMyPi,
                frame,
                &pending,
                &commands,
                &events,
                &mut state,
            );
        }
        assert!(matches!(event_rx.recv().unwrap(), DriverEvent::TurnStarted));
        assert!(
            matches!(event_rx.recv().unwrap(), DriverEvent::TextDelta(text) if text == "Available commands\n\n\n")
        );
        assert!(
            matches!(event_rx.recv().unwrap(), DriverEvent::TextDelta(text) if text == "/compact\n\n")
        );
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TurnFinished { success: true, .. }
        ));
        assert!(event_rx.try_recv().is_err());
        assert!(pending.lock().is_empty());
    }

    #[test]
    fn asynchronous_prompt_errors_settle_only_the_current_prompt() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        let mut next = 0;
        let mut wire = Vec::new();
        send_prompt(&mut wire, &pending, &mut next, "/old").unwrap();
        send_prompt(&mut wire, &pending, &mut next, "/new").unwrap();
        handle_pi_message(
            PiFlavor::Pi,
            json!({"type": "response", "id": "waku-1", "success": false, "error": "stale"}),
            &pending,
            &commands,
            &events,
            &mut state,
        );
        assert!(event_rx.try_recv().is_err());
        handle_pi_message(
            PiFlavor::Pi,
            json!({"type": "response", "id": "waku-2", "success": false, "error": "command failed"}),
            &pending,
            &commands,
            &events,
            &mut state,
        );
        assert!(matches!(event_rx.recv().unwrap(), DriverEvent::TurnStarted));
        assert!(
            matches!(event_rx.recv().unwrap(), DriverEvent::Error(error) if error == "command failed")
        );
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TurnFinished { success: false, .. }
        ));
        assert!(pending.lock().is_empty());
        assert_eq!(String::from_utf8(wire).unwrap().lines().count(), 2);
    }
    fn harness() -> (
        PendingResponses,
        Sender<CommandMessage>,
        crossbeam_channel::Receiver<CommandMessage>,
        PiStreamState,
    ) {
        let (commands, receiver) = unbounded();
        (
            Arc::new(Mutex::new(HashMap::new())),
            commands,
            receiver,
            PiStreamState::default(),
        )
    }

    /// Drives the installed Pi RPC through one real provider turn. Ignored by
    /// default because it needs the CLI, credentials, and network access.
    #[test]
    #[ignore = "requires an installed, authenticated pi"]
    fn pi_context_usage_against_the_real_rpc() {
        let binary = crate::command_env::find_executable("pi").expect("pi is not installed");
        let (events, event_rx) = crate::driver::test_event_channel();
        let driver = PiDriver::start(
            PiFlavor::Pi,
            DriverStartOptions {
                binary,
                cwd: std::env::temp_dir(),
                mode: RuntimeMode::FullAccess,
                model: None,
                reasoning_effort: None,
                service_tier: None,
                context_window: None,
                agent_preset: None,
                computer_use_enabled: false,
                provider_cursor: None,
            },
            events,
        )
        .expect("the Pi RPC session should start");

        let mut connected = false;
        let mut context_tokens = None;
        let mut context_window = None;
        while let Ok(event) = event_rx.recv_timeout(Duration::from_secs(30)) {
            match event {
                DriverEvent::Connected { .. } => {
                    connected = true;
                    break;
                }
                DriverEvent::Error(error) => panic!("Pi failed to initialize: {error}"),
                _ => {}
            }
        }
        assert!(connected, "Pi never reported its native session");

        driver.prompt("Reply with exactly: OK. Do not use any tools.".into());
        let mut finished = false;
        while let Ok(event) = event_rx.recv_timeout(Duration::from_secs(180)) {
            match event {
                DriverEvent::UsageUpdated {
                    context_tokens: tokens,
                    context_window: window,
                } => {
                    context_tokens = tokens.or(context_tokens);
                    context_window = window.or(context_window);
                }
                DriverEvent::TurnFinished { success, .. } => {
                    assert!(success, "Pi should finish the probe turn");
                    finished = true;
                    break;
                }
                DriverEvent::Error(error) => panic!("Pi reported: {error}"),
                _ => {}
            }
        }

        assert!(finished, "Pi never settled the probe turn");
        assert!(context_tokens.is_some_and(|tokens| tokens > 0));
        assert!(context_window.is_some_and(|window| window > 0));
    }

    #[test]
    fn model_and_thinking_changes_reach_the_running_session_but_mode_changes_do_not() {
        let (commands, command_rx) = unbounded();
        let driver = PiDriver {
            flavor: PiFlavor::Pi,
            commands,
            computer_use: None,
            dialogs: PendingDialogs::default(),
        };
        let options = |mode| SessionOptions {
            mode,
            model: Some("anthropic/claude-opus-5".to_owned()),
            reasoning_effort: Some("high".to_owned()),
            service_tier: None,
            context_window: None,
        };

        assert!(driver.apply_options(options(RuntimeMode::FullAccess)));
        assert!(matches!(
            command_rx.try_recv(),
            Ok(CommandMessage::Options(_))
        ));

        // Pi has no permission setter and only runs with Full access.
        assert!(!driver.apply_options(options(RuntimeMode::Ask)));
        assert!(command_rx.try_recv().is_err());
    }

    #[test]
    fn pi_computer_use_uses_only_session_scoped_extension_and_skill_arguments() {
        let config = computer_use_runtime::ComputerUseConfig {
            server_path: PathBuf::from("/tmp/Waku Computer Use"),
            repl_path: PathBuf::from("/Applications/Waku.app/Resources/waku_js_repl"),
            skill_path: PathBuf::from("/Applications/Waku.app/Resources/skills/SKILL.md"),
            process_directory: PathBuf::from("/tmp/waku-computer-use/session"),
        };
        let mut command = std::process::Command::new("pi");

        configure_pi_computer_use_command(
            &mut command,
            Some((
                &config,
                Path::new("/Applications/Waku.app/Resources/computer-use/pi-extension.ts"),
            )),
        );

        let arguments = command
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            arguments,
            [
                "--extension",
                "/Applications/Waku.app/Resources/computer-use/pi-extension.ts",
                "--skill",
                "/Applications/Waku.app/Resources/skills/SKILL.md",
            ]
        );
        let environment = command
            .get_envs()
            .map(|(name, value)| {
                (
                    name.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(
            environment.get("WAKU_JS_REPL_SERVER"),
            Some(&Some(
                "/Applications/Waku.app/Resources/waku_js_repl".into()
            ))
        );
        assert_eq!(
            environment.get("WAKU_COMPUTER_USE_PROCESS_DIRECTORY"),
            Some(&Some("/tmp/waku-computer-use/session".into()))
        );
    }

    #[test]
    fn pi_fork_selects_the_first_removed_user_turn_or_clones_the_tip() {
        let messages = [
            json!({"entryId": "turn-1"}),
            json!({"entryId": "turn-2"}),
            json!({"entryId": "turn-3"}),
        ];
        assert_eq!(
            pi_fork_request(PiFlavor::Pi, &messages, 0).unwrap(),
            json!({"type": "clone"})
        );
        assert_eq!(
            pi_fork_request(PiFlavor::Pi, &messages, 2).unwrap(),
            json!({"type": "fork", "entryId": "turn-2"})
        );
        assert_eq!(
            pi_fork_request(PiFlavor::Pi, &messages, 3).unwrap(),
            json!({"type": "fork", "entryId": "turn-1"})
        );
        assert!(pi_fork_request(PiFlavor::Pi, &messages, 4).is_err());
    }

    #[test]
    fn streams_pi_text_reasoning_tools_and_settles_once() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        for value in [
            json!({"type": "agent_start"}),
            json!({"type": "turn_start"}),
            json!({
                "type": "message_update",
                "assistantMessageEvent": {"type": "thinking_delta", "delta": "checking"}
            }),
            json!({
                "type": "tool_execution_start",
                "toolCallId": "tool-1",
                "toolName": "read",
                "args": {"path": "src/main.rs", "title": "Inspect Pi source"}
            }),
            json!({
                "type": "tool_execution_end",
                "toolCallId": "tool-1",
                "toolName": "read",
                "result": {"content": "..."},
                "isError": false
            }),
            json!({
                "type": "message_update",
                "assistantMessageEvent": {"type": "text_delta", "delta": "done"}
            }),
            json!({"type": "agent_end", "willRetry": false}),
            json!({"type": "agent_settled"}),
        ] {
            handle_pi_message(
                PiFlavor::Pi,
                value,
                &pending,
                &commands,
                &events,
                &mut state,
            );
        }

        assert!(matches!(event_rx.recv().unwrap(), DriverEvent::TurnStarted));
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::ReasoningDelta(value) if value == "checking"
        ));
        let DriverEvent::RichActivity(started) = event_rx.recv().unwrap() else {
            panic!("expected a rich Pi tool activity");
        };
        assert_eq!(started.title, "Inspect Pi source");
        assert_eq!(started.kind, ActivityKind::FileRead);
        assert_eq!(started.display_target.as_deref(), Some("src/main.rs"));
        assert!(
            started
                .arguments
                .as_deref()
                .is_some_and(|arguments| arguments.contains("src/main.rs"))
        );
        assert!(!started.complete);
        let DriverEvent::RichActivity(completed) = event_rx.recv().unwrap() else {
            panic!("expected a completed rich Pi tool activity");
        };
        assert!(
            completed
                .output
                .as_deref()
                .is_some_and(|output| output.contains("..."))
        );
        assert!(completed.complete);
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TextDelta(value) if value == "done"
        ));
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TurnFinished { success: true, .. }
        ));
        assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    /// Drives the installed Oh My Pi RPC through one real provider turn.
    /// Ignored by default because it needs the CLI, credentials, and network.
    #[test]
    #[ignore = "requires an installed, authenticated omp"]
    fn ohmypi_context_usage_against_the_real_rpc() {
        let binary = crate::command_env::find_executable("omp").expect("omp is not installed");
        let (events, event_rx) = crate::driver::test_event_channel();
        let driver = PiDriver::start(
            PiFlavor::OhMyPi,
            DriverStartOptions {
                binary,
                cwd: std::env::temp_dir(),
                mode: RuntimeMode::FullAccess,
                model: None,
                reasoning_effort: None,
                service_tier: None,
                context_window: None,
                agent_preset: None,
                computer_use_enabled: false,
                provider_cursor: None,
            },
            events,
        )
        .expect("the Oh My Pi RPC session should start");

        let mut cursor = None;
        while let Ok(event) = event_rx.recv_timeout(Duration::from_secs(60)) {
            match event {
                DriverEvent::Connected { provider_cursor } => {
                    cursor = provider_cursor;
                    break;
                }
                DriverEvent::Error(error) => panic!("Oh My Pi failed to initialize: {error}"),
                _ => {}
            }
        }
        assert!(
            matches!(
                cursor,
                Some(ProviderResumeCursor::OhMyPi {
                    session_file: Some(_),
                    ..
                })
            ),
            "Oh My Pi should report its own cursor with a session file, got {cursor:?}"
        );

        driver.prompt("Reply with exactly: OK. Do not use any tools.".into());
        let mut finished = false;
        let mut context_tokens = None;
        let mut context_window = None;
        while let Ok(event) = event_rx.recv_timeout(Duration::from_secs(180)) {
            match event {
                DriverEvent::UsageUpdated {
                    context_tokens: tokens,
                    context_window: window,
                } => {
                    context_tokens = tokens.or(context_tokens);
                    context_window = window.or(context_window);
                }
                DriverEvent::TurnFinished { success, .. } => {
                    assert!(success, "Oh My Pi should finish the probe turn");
                    finished = true;
                    break;
                }
                DriverEvent::Error(error) => panic!("Oh My Pi reported: {error}"),
                _ => {}
            }
        }

        assert!(finished, "Oh My Pi never settled the probe turn");
        assert!(context_tokens.is_some_and(|tokens| tokens > 0));
        assert!(context_window.is_some_and(|window| window > 0));
    }

    #[test]
    fn ohmypi_branches_where_pi_forks_and_never_asks_it_to_clone() {
        let messages = [
            json!({"entryId": "turn-1"}),
            json!({"entryId": "turn-2"}),
            json!({"entryId": "turn-3"}),
        ];
        assert_eq!(
            pi_fork_request(PiFlavor::OhMyPi, &messages, 2).unwrap(),
            json!({"type": "branch", "entryId": "turn-2"})
        );
        assert!(pi_fork_request(PiFlavor::OhMyPi, &messages, 4).is_err());
    }

    /// Oh My Pi reuses `agent_end` for intermediate settles, so only the
    /// terminal one may end the turn.
    #[test]
    fn ohmypi_settles_on_the_terminal_agent_end_only() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        for value in [
            json!({"type": "agent_start"}),
            json!({
                "type": "message_update",
                "assistantMessageEvent": {"type": "text_delta", "delta": "done"}
            }),
            json!({"type": "agent_end", "isTerminal": false}),
            json!({"type": "agent_end", "messages": []}),
        ] {
            handle_pi_message(
                PiFlavor::OhMyPi,
                value,
                &pending,
                &commands,
                &events,
                &mut state,
            );
        }

        assert!(matches!(event_rx.recv().unwrap(), DriverEvent::TurnStarted));
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TextDelta(value) if value == "done"
        ));
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TurnFinished { success: true, .. }
        ));
        assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    /// Pi's own settle event carries no meaning for Oh My Pi, and vice versa.
    #[test]
    fn each_flavor_ignores_the_other_settle_and_title_events() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        for (flavor, value) in [
            (PiFlavor::OhMyPi, json!({"type": "agent_start"})),
            (PiFlavor::OhMyPi, json!({"type": "agent_settled"})),
            (
                PiFlavor::OhMyPi,
                json!({"type": "session_info_changed", "name": "Pi's spelling"}),
            ),
            (PiFlavor::Pi, json!({"type": "agent_end"})),
            (
                PiFlavor::Pi,
                json!({"type": "session_info_update", "title": "Oh My Pi's spelling"}),
            ),
        ] {
            handle_pi_message(flavor, value, &pending, &commands, &events, &mut state);
        }

        assert!(matches!(event_rx.recv().unwrap(), DriverEvent::TurnStarted));
        assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn ohmypi_session_titles_arrive_on_its_own_event() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        handle_pi_message(
            PiFlavor::OhMyPi,
            json!({"type": "session_info_update", "title": "Named by Oh My Pi"}),
            &pending,
            &commands,
            &events,
            &mut state,
        );
        assert!(matches!(
            event_rx.try_recv().unwrap(),
            DriverEvent::AutoTitleUpdated(Some(title)) if title == "Named by Oh My Pi"
        ));
    }

    #[test]
    fn chunked_frames_reassemble_and_reject_broken_runs() {
        use base64::Engine as _;
        let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);

        let payload = json!({"type": "response", "id": "waku-1", "success": true});
        let bytes = serde_json::to_vec(&payload).unwrap();
        let (first, second) = bytes.split_at(bytes.len() / 2);
        let chunk = |index: u64, data: &[u8]| {
            json!({
                "type": "rpc_chunk",
                "chunkId": "rpc-1",
                "index": index,
                "count": 2,
                "byteLength": bytes.len(),
                "data": encode(data),
            })
        };

        let mut assembly = ChunkAssembly::default();
        assert_eq!(assembly.accept(chunk(0, first)).unwrap(), None);
        assert_eq!(assembly.accept(chunk(1, second)).unwrap(), Some(payload));

        // An ordinary frame passes straight through.
        let mut assembly = ChunkAssembly::default();
        let plain = json!({"type": "agent_start"});
        assert_eq!(assembly.accept(plain.clone()).unwrap(), Some(plain.clone()));

        // Anything interleaved into a run invalidates it rather than splicing.
        let mut assembly = ChunkAssembly::default();
        assert_eq!(assembly.accept(chunk(0, first)).unwrap(), None);
        assert!(assembly.accept(plain).is_err());
        assert!(assembly.active.is_none());

        // A run that starts mid-sequence is not a frame Waku can trust.
        let mut assembly = ChunkAssembly::default();
        assert!(assembly.accept(chunk(1, second)).is_err());
    }

    #[test]
    fn session_name_changes_are_forwarded_as_automatic_titles() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();

        handle_pi_message(
            PiFlavor::Pi,
            json!({"type": "session_info_changed", "name": "Named by Pi"}),
            &pending,
            &commands,
            &events,
            &mut state,
        );
        assert!(matches!(
            event_rx.try_recv().unwrap(),
            DriverEvent::AutoTitleUpdated(Some(title)) if title == "Named by Pi"
        ));

        handle_pi_message(
            PiFlavor::Pi,
            json!({"type": "session_info_changed", "name": null}),
            &pending,
            &commands,
            &events,
            &mut state,
        );
        assert!(matches!(
            event_rx.try_recv().unwrap(),
            DriverEvent::AutoTitleUpdated(None)
        ));
    }

    #[test]
    fn tool_only_intermediate_message_does_not_emit_empty_text() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        handle_pi_message(
            PiFlavor::Pi,
            json!({
                "type": "message_end",
                "message": {
                    "role": "assistant",
                    "content": [{"type": "toolCall", "id": "tool-1", "name": "read"}]
                }
            }),
            &pending,
            &commands,
            &events,
            &mut state,
        );

        assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn completed_message_is_used_when_deltas_were_not_streamed() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        handle_pi_message(
            PiFlavor::Pi,
            json!({
                "type": "message_end",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "thinking", "thinking": "reason"},
                        {"type": "text", "text": "answer"}
                    ]
                }
            }),
            &pending,
            &commands,
            &events,
            &mut state,
        );

        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::ReasoningDelta(value) if value == "reason"
        ));
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TextDelta(value) if value == "answer"
        ));
    }

    #[test]
    fn context_usage_uses_pi_components_when_total_is_zero() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        handle_pi_message(
            PiFlavor::Pi,
            json!({
                "type": "message_end",
                "message": {
                    "role": "assistant",
                    "content": [],
                    "usage": {
                        "input": 33,
                        "output": 27,
                        "cacheRead": 5888,
                        "cacheWrite": 4,
                        "totalTokens": 0
                    }
                }
            }),
            &pending,
            &commands,
            &events,
            &mut state,
        );

        assert!(matches!(
            event_rx.try_recv().unwrap(),
            DriverEvent::UsageUpdated {
                context_tokens: Some(5952),
                context_window: None
            }
        ));
        assert!(event_rx.try_recv().is_err());
    }

    #[test]
    fn session_stats_supply_pi_context_tokens_and_window() {
        let state = json!({"data": {"model": {"contextWindow": 200_000}}});
        let stats = json!({
            "data": {
                "contextUsage": {
                    "tokens": 6109,
                    "contextWindow": 1_000_000,
                    "percent": 0.6109
                }
            }
        });

        assert_eq!(
            pi_context_usage(&state, Some(&stats)),
            Some((Some(6109), Some(1_000_000)))
        );
        assert_eq!(pi_context_usage(&state, None), Some((None, Some(200_000))));
    }

    #[test]
    fn recoverable_tool_error_does_not_fail_the_turn() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        for value in [
            json!({"type": "agent_start"}),
            json!({
                "type": "tool_execution_end",
                "toolCallId": "tool-1",
                "toolName": "read",
                "result": {"error": "missing"},
                "isError": true
            }),
            json!({"type": "agent_settled"}),
        ] {
            handle_pi_message(
                PiFlavor::Pi,
                value,
                &pending,
                &commands,
                &events,
                &mut state,
            );
        }

        assert!(matches!(event_rx.recv().unwrap(), DriverEvent::TurnStarted));
        let DriverEvent::RichActivity(completed) = event_rx.recv().unwrap() else {
            panic!("expected a completed rich Pi tool activity");
        };
        assert!(completed.failed);
        assert!(
            completed
                .output
                .as_deref()
                .is_some_and(|output| output.contains("missing"))
        );
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TurnFinished { success: true, .. }
        ));
    }

    #[test]
    fn successful_auto_retry_recovers_the_turn() {
        let (pending, commands, _command_rx, mut state) = harness();
        let (events, event_rx) = unbounded();
        for value in [
            json!({"type": "agent_start"}),
            json!({
                "type": "message_update",
                "assistantMessageEvent": {"type": "error", "error": "temporary"}
            }),
            json!({"type": "auto_retry_end", "success": true}),
            json!({"type": "agent_settled"}),
        ] {
            handle_pi_message(
                PiFlavor::Pi,
                value,
                &pending,
                &commands,
                &events,
                &mut state,
            );
        }

        assert!(matches!(event_rx.recv().unwrap(), DriverEvent::TurnStarted));
        assert!(matches!(event_rx.recv().unwrap(), DriverEvent::Error(_)));
        assert!(matches!(
            event_rx.recv().unwrap(),
            DriverEvent::TurnFinished { success: true, .. }
        ));
    }
}

/// choco-pi: host-bridge entries and frames, forwarded dialogs, command
/// prompts, and tool-update debouncing. Fixtures are built from captured
/// choco-pi RPC frames (envelopes verbatim, private paths neutralized).
#[cfg(test)]
mod chocopi_tests {
    use super::*;
    use crate::model::{BackgroundWorkItem, BackgroundWorkStatus, ThreadGoal};
    use crossbeam_channel::Receiver;

    const LIFECYCLE: &str = include_str!("fixtures/chocopi_host_bridge_lifecycle.jsonl");
    const SNAPSHOT: &str = include_str!("fixtures/chocopi_snapshot.jsonl");
    const DIALOGS: &str = include_str!("fixtures/chocopi_dialogs.jsonl");
    const AGENT_UPDATES: &str = include_str!("fixtures/chocopi_agent_tool_updates.jsonl");
    const VERIFIED: &str = include_str!("fixtures/chocopi_bridge_verified.jsonl");

    struct Harness {
        flavor: PiFlavor,
        pending: PendingResponses,
        commands: Sender<CommandMessage>,
        command_rx: Receiver<CommandMessage>,
        state: PiStreamState,
        dialogs: PendingDialogs,
    }

    impl Harness {
        fn new(flavor: PiFlavor) -> Self {
            let profile = crate::choco_pi_profile::parse_profile(
                br#"{"schema":1,"checkout":"/choco-pi","hostBridge":{"schema":1,"entryPrefix":"choco-pi:host-bridge","statusKey":"choco-pi.host-bridge"}}"#,
            )
            .expect("the test marker should parse");
            let dialogs = PendingDialogs::default();
            let (commands, command_rx) = unbounded();
            Self {
                flavor,
                pending: Arc::new(Mutex::new(HashMap::new())),
                commands,
                command_rx,
                state: PiStreamState::new(
                    flavor.host_bridge(Some(&profile)),
                    dialogs.clone(),
                    Arc::new(AtomicBool::new(false)),
                ),
                dialogs,
            }
        }

        fn driver(&self) -> PiDriver {
            PiDriver {
                flavor: self.flavor,
                commands: self.commands.clone(),
                computer_use: None,
                dialogs: self.dialogs.clone(),
            }
        }

        fn feed(&mut self, frames: &str) -> Vec<DriverEvent> {
            let (events, event_rx) = unbounded();
            for line in frames.lines().filter(|line| !line.trim().is_empty()) {
                handle_pi_message(
                    self.flavor,
                    serde_json::from_str(line).expect("fixture lines are JSON"),
                    &self.pending,
                    &self.commands,
                    &events,
                    &mut self.state,
                );
            }
            event_rx.try_iter().collect()
        }

        fn feed_value(&mut self, value: Value) -> Vec<DriverEvent> {
            self.feed(&value.to_string())
        }

        fn sent(&self) -> Vec<CommandMessage> {
            self.command_rx.try_iter().collect()
        }

        /// Sends a command prompt from a writer thread and waits until the
        /// reader would see it registered, as the real writer does.
        fn command_prompt(
            &self,
            message: &'static str,
            snapshot: bool,
        ) -> thread::JoinHandle<(Result<Value, String>, Vec<u8>)> {
            let pending = self.pending.clone();
            let before = pending.lock().len();
            let handle = thread::spawn(move || {
                let mut wire = Vec::new();
                let mut next = 0;
                let result = send_command_prompt(&mut wire, &pending, &mut next, message, snapshot);
                (result, wire)
            });
            while self.pending.lock().len() == before {
                thread::yield_now();
            }
            handle
        }
    }

    fn describe(event: &DriverEvent) -> String {
        match event {
            DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item)) => format!(
                "upsert {}:{} {:?}",
                kind_name(item),
                item.key.provider_id,
                item.status
            ),
            DriverEvent::BackgroundWork(BackgroundWorkEvent::ReconcileLive { items }) => format!(
                "reconcile [{}]",
                items
                    .iter()
                    .map(|item| item.key.provider_id.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            DriverEvent::BackgroundWork(BackgroundWorkEvent::OutputDelta { key, .. }) => {
                format!("output {}", key.provider_id)
            }
            DriverEvent::GoalUpdated(Some(goal)) => format!("goal {:?}", goal.status),
            DriverEvent::GoalUpdated(None) => "goal none".to_owned(),
            DriverEvent::TurnStarted => "turn started".to_owned(),
            DriverEvent::TurnFinished { success, .. } => format!("turn finished {success}"),
            DriverEvent::TextDelta(text) => format!("text {text}"),
            DriverEvent::Error(error) => format!("error {error}"),
            DriverEvent::UserInputRequested { request_id, .. } => format!("ask {request_id}"),
            DriverEvent::RichActivity(item) => format!("activity {}", item.complete),
            other => format!("{other:?}"),
        }
    }

    fn kind_name(item: &BackgroundWorkItem) -> &'static str {
        match item.key.kind {
            BackgroundWorkKind::Subagent => "subagent",
            BackgroundWorkKind::Process => "shell",
            BackgroundWorkKind::Monitor => "monitor",
        }
    }

    fn upserts(events: &[DriverEvent]) -> Vec<&BackgroundWorkItem> {
        events
            .iter()
            .filter_map(|event| match event {
                DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item)) => Some(item),
                _ => None,
            })
            .collect()
    }

    fn entry(custom_type: &str, data: Value) -> Value {
        json!({
            "type": "entry_appended",
            "entry": {"type": "custom", "customType": custom_type, "data": data, "id": "e1"}
        })
    }

    fn status_frame(text: &Value) -> Value {
        json!({
            "type": "extension_ui_request",
            "id": "status-1",
            "method": "setStatus",
            "statusKey": "choco-pi.host-bridge",
            "statusText": text.to_string(),
        })
    }

    #[test]
    fn chocopi_flavor_axes_match_the_design() {
        let profile = crate::choco_pi_profile::parse_profile(
            br#"{"schema":1,"checkout":"/choco-pi","hostBridge":{"schema":1}}"#,
        )
        .unwrap();
        let newer = crate::choco_pi_profile::parse_profile(
            br#"{"schema":1,"checkout":"/choco-pi","hostBridge":{"schema":2}}"#,
        )
        .unwrap();
        assert_eq!(
            PiFlavor::ChocoPi.host_bridge(Some(&profile)),
            Some(HostBridgeSpec::default())
        );
        // A newer bridge than this build understands degrades to plain Pi.
        assert_eq!(PiFlavor::ChocoPi.host_bridge(Some(&newer)), None);
        assert_eq!(PiFlavor::ChocoPi.host_bridge(None), None);
        assert_eq!(PiFlavor::Pi.host_bridge(Some(&profile)), None);
        assert_eq!(PiFlavor::OhMyPi.host_bridge(Some(&profile)), None);
        assert!(PiFlavor::ChocoPi.forwards_extension_ui());
        assert!(!PiFlavor::Pi.forwards_extension_ui());
        assert!(!PiFlavor::OhMyPi.forwards_extension_ui());
        assert!(!PiFlavor::ChocoPi.supports_waku_computer_use());
        assert!(PiFlavor::Pi.supports_waku_computer_use());
        assert!(!PiFlavor::OhMyPi.supports_waku_computer_use());
        assert!(PiFlavor::ChocoPi.starts_turns_unprompted());
        assert!(!PiFlavor::Pi.starts_turns_unprompted());
        assert!(!PiFlavor::OhMyPi.starts_turns_unprompted());
        assert_eq!(PiFlavor::ChocoPi.settled_event(), "agent_settled");
        assert_eq!(PiFlavor::ChocoPi.full_access_arg(), "--approve");
    }

    #[test]
    fn chocopi_start_environment_names_the_host_and_bridge_schema() {
        let environment = |flavor| {
            let mut command = std::process::Command::new("pi");
            configure_choco_pi_host_command(&mut command, flavor);
            command
                .get_envs()
                .map(|(name, value)| {
                    (
                        name.to_string_lossy().into_owned(),
                        value.map(|value| value.to_string_lossy().into_owned()),
                    )
                })
                .collect::<HashMap<_, _>>()
        };
        let choco = environment(PiFlavor::ChocoPi);
        assert_eq!(choco.get("CHOCO_PI_HOST"), Some(&Some("waku".to_owned())));
        assert_eq!(
            choco.get("CHOCO_PI_HOST_BRIDGE_SCHEMA"),
            Some(&Some("1".to_owned()))
        );
        assert!(environment(PiFlavor::Pi).is_empty());
        assert!(environment(PiFlavor::OhMyPi).is_empty());
    }

    /// W6 (a) and (b): transitions upsert and reconcile; a shell may settle
    /// after it was lost; a null exit code is a failure; progress refines
    /// only known work; a background completion wakes a fresh turn.
    #[test]
    fn chocopi_host_bridge_entries_drive_background_work() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let events = harness.feed(LIFECYCLE);
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            [
                "upsert subagent:f67bce3e-2646-476 Starting",
                "reconcile [f67bce3e-2646-476]",
                "upsert subagent:f67bce3e-2646-476 Running",
                "reconcile [f67bce3e-2646-476]",
                "upsert subagent:f67bce3e-2646-476 Running",
                "upsert shell:sh-1 Running",
                "reconcile [f67bce3e-2646-476,sh-1]",
                "upsert shell:sh-1 Lost",
                "reconcile [f67bce3e-2646-476]",
                "upsert shell:sh-1 Failed",
                "reconcile [f67bce3e-2646-476]",
                "upsert subagent:f67bce3e-2646-476 Completed",
                "reconcile []",
                "turn started",
                "text The background agent finished.",
                "turn finished true",
            ]
        );
        let items = upserts(&events);
        let created = items[0];
        assert_eq!(created.title, "Background transport probe listing");
        assert_eq!(created.role.as_deref(), Some("explore"));
        assert_eq!(created.model.as_deref(), Some("anthropic/claude-haiku-5-5"));
        assert_eq!(created.origin_activity_id.as_deref(), Some("toolu_01Bg"));
        assert_eq!(created.control_id.as_deref(), Some("f67bce3e-2646-476"));
        assert!(created.background && created.can_stop);
        assert_eq!(created.started_at_ms, 1_791_695_794_600);

        let progress = items[2];
        assert_eq!(
            progress.detail.as_deref(),
            Some("1 tool uses · 7012 tokens · The directory is empty:")
        );
        assert_eq!(progress.duration_ms, Some(2607));
        assert!(
            items
                .iter()
                .all(|item| item.key.provider_id != "unknown-agent"),
            "progress must not invent work no entry introduced"
        );

        let shell = items[3];
        assert_eq!(shell.title, "dev server");
        assert_eq!(shell.command.as_deref(), Some("pnpm dev"));
        assert_eq!(shell.cwd.as_deref(), Some("/work"));
        let exited = items[5];
        assert_eq!(exited.status, BackgroundWorkStatus::Failed);
        assert_eq!(exited.exit_code, None);
        assert!(!exited.can_stop);

        let completed = items[6];
        assert_eq!(
            completed.detail.as_deref(),
            Some("The current working directory is empty.")
        );
        assert_eq!(completed.duration_ms, Some(3866));
        assert!(!completed.can_stop);
        assert!(harness.sent().is_empty());
    }

    /// Replays the stdout of a live choco-pi 1.0.4 RPC session (trimmed to
    /// what the driver reads, private paths neutralized): a user command
    /// with dialogs, a snapshot, a background subagent whose completion
    /// wakes an unprompted run, a managed shell, and a final snapshot.
    #[test]
    fn chocopi_replays_the_verified_bridge_capture() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let lines = VERIFIED
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>();
        // The probe's prompts, in order: a user-typed extension command,
        // a snapshot, two user prompts, and a second snapshot.
        let requests = [
            ("probe-3", false),
            ("probe-4", true),
            ("probe-5", false),
            ("probe-6", false),
            ("probe-7", true),
        ];
        let responded = |id: &str| {
            let marker = format!(r#""id":"{id}","type":"response""#);
            lines
                .iter()
                .position(|line| line.contains(&marker))
                .expect("every probe prompt is answered")
        };
        let settled = |before: usize| {
            lines[..before]
                .iter()
                .rposition(|line| line.contains(r#""type":"agent_settled""#))
        };
        // A client sends the next prompt only after the previous one was
        // answered and any run it caused has settled.
        let mut sent_at = Vec::new();
        let mut previous = None::<usize>;
        for (id, snapshot) in requests {
            let response = responded(id);
            let after = previous
                .into_iter()
                .chain(settled(response))
                .max()
                .map_or(0, |index| index + 1);
            sent_at.push((after, id, snapshot));
            previous = Some(response);
        }
        let mut receivers = Vec::new();
        let mut events = Vec::new();
        let mut run_starts = Vec::new();
        for (index, line) in lines.iter().enumerate() {
            if line.contains(r#""type":"agent_start""#) {
                let prompted = harness
                    .pending
                    .lock()
                    .values()
                    .any(PendingResponse::is_prompt);
                run_starts.push(prompted);
            }
            for (_, id, snapshot) in sent_at.iter().filter(|(at, ..)| *at == index) {
                let mut pending = harness.pending.lock();
                if *snapshot {
                    let (sender, receiver) = bounded(1);
                    receivers.push(receiver);
                    pending.insert((*id).to_owned(), PendingResponse::Snapshot(sender));
                } else {
                    pending.retain(|_, response| !response.is_prompt());
                    pending.insert((*id).to_owned(), PendingResponse::Prompt);
                }
            }
            events.extend(harness.feed(line));
        }
        let described = events
            .iter()
            .filter(|event| {
                !matches!(
                    event,
                    DriverEvent::TextDelta(_)
                        | DriverEvent::ReasoningDelta(_)
                        | DriverEvent::RichActivity(_)
                        | DriverEvent::UsageUpdated { .. }
                        | DriverEvent::AutoTitleUpdated(_)
                )
            })
            .map(describe)
            .collect::<Vec<_>>();
        assert_eq!(
            described,
            [
                // `/rpc-host-bridge-probe-ui` raises every dialog kind, then
                // is handled without reaching the model.
                "ask d624dada-85b3-4656-a7c6-e9a99e80e461",
                "ask b3f06db9-f21b-4b9a-9fdf-12d5b83186de",
                "ask 37449fcd-8248-47cf-8955-66881a2f739f",
                "ask 3e7fdd00-0bf9-46bc-9eee-5bd010e17a7b",
                "turn started",
                "turn finished true",
                // Snapshot: nothing live, no goal.
                "reconcile []",
                "goal none",
                // A background Agent: `started` lands before `created`.
                "turn started",
                "upsert subagent:10b7e07d-d3e4-495 Running",
                "reconcile [10b7e07d-d3e4-495]",
                "upsert subagent:10b7e07d-d3e4-495 Running",
                "reconcile [10b7e07d-d3e4-495]",
                // Progress frame.
                "upsert subagent:10b7e07d-d3e4-495 Running",
                "upsert subagent:10b7e07d-d3e4-495 Completed",
                "reconcile []",
                "turn finished true",
                // The completion wakes the session with nobody prompting.
                "turn started",
                "turn finished true",
                // A managed shell.
                "turn started",
                "upsert shell:747031e1-de4f-41e3-976b-b0f286a748d4 Running",
                "reconcile [747031e1-de4f-41e3-976b-b0f286a748d4]",
                "upsert shell:747031e1-de4f-41e3-976b-b0f286a748d4 Completed",
                "reconcile []",
                "turn finished true",
                // Final snapshot.
                "reconcile []",
                "goal none",
            ]
        );
        assert_eq!(
            run_starts,
            [true, false, true],
            "the second run is unprompted"
        );
        let items = upserts(&events);
        let created = items[1];
        assert_eq!(created.title, "RPC probe child");
        assert_eq!(created.role.as_deref(), Some("general"));
        assert_eq!(created.model.as_deref(), Some("openai-codex/gpt-6.1-sol"));
        assert!(created.background && created.can_stop);
        assert!(
            created
                .origin_activity_id
                .as_deref()
                .is_some_and(|id| id.starts_with("call_3a8b392c")),
            "the later `created` entry supplies the tool call"
        );
        assert_eq!(items[2].detail.as_deref(), Some("0 tool uses · 0 tokens"));
        let completed = items[3];
        assert_eq!(completed.detail.as_deref(), Some("RPC_PROBE_CHILD_DONE"));
        assert_eq!(completed.duration_ms, Some(3320));
        assert!(!completed.can_stop);
        let shell = items[5];
        assert_eq!(shell.title, "bridge-probe-root-liveness");
        assert_eq!(shell.command.as_deref(), Some("printf bridge-shell-ok"));
        assert_eq!(shell.exit_code, Some(0));
        assert!(harness.pending.lock().is_empty());
        assert!(!harness.state.bridge_live.load(Ordering::Acquire));
        // Both snapshot requests were answered.
        assert!(
            receivers
                .iter()
                .all(|receiver| receiver.try_recv().is_ok_and(|response| response.is_ok()))
        );
    }

    /// W6 (b): a frame for an id no entry introduced, and for work an entry
    /// already settled, changes nothing.
    #[test]
    fn chocopi_progress_frames_update_only_known_live_work() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let events = harness.feed_value(status_frame(&json!({
            "schema": 1, "at": 1, "shells": [],
            "subagents": [{"id": "nobody", "status": "running", "toolUses": 1, "tokens": 2,
                "turnCount": 1, "maxTurns": null, "durationMs": 3, "activity": null}]
        })));
        assert!(events.is_empty(), "{events:?}");

        harness.feed(LIFECYCLE);
        let events = harness.feed_value(status_frame(&json!({
            "schema": 1, "at": 2, "shells": [{"id": "sh-1", "status": "running", "bytes": 10}],
            "subagents": [{"id": "f67bce3e-2646-476", "status": "running", "toolUses": 9,
                "tokens": 9, "turnCount": 1, "maxTurns": null, "durationMs": 9, "activity": null}]
        })));
        assert!(
            events.is_empty(),
            "settled work must stay settled: {events:?}"
        );
    }

    /// W6 (c): every dialog kind reaches the user and each answer goes back
    /// in the shape Pi expects, without an acknowledgement or pending entry.
    #[test]
    fn chocopi_dialogs_become_user_input_and_answers_reach_pi() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let driver = harness.driver();
        // A user command such as `/effort` is in flight.
        send_prompt(&mut Vec::new(), &harness.pending, &mut 0, "/effort").unwrap();
        let events = harness.feed(DIALOGS);
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            [
                "ask 79a3b55c-2199-4cd8-a90a-941b2f1ee579",
                "ask confirm-1",
                "ask input-1",
                "ask editor-1",
                "error MCP server figma failed to start",
            ]
        );
        let questions = events
            .iter()
            .filter_map(|event| match event {
                DriverEvent::UserInputRequested { questions, .. } => Some(&questions[0]),
                _ => None,
            })
            .collect::<Vec<_>>();
        let labels = |question: &UserInputQuestion| {
            question
                .options
                .iter()
                .map(|option| option.label.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(questions[0].id, "value");
        assert_eq!(questions[0].header, "Reasoning effort");
        assert_eq!(
            labels(questions[0]),
            ["low", "medium (current)", "high", "xhigh", "max"]
        );
        assert_eq!(questions[1].header, "Restore files?");
        assert_eq!(
            questions[1].question,
            "Rewind restores 3 files to the checkpoint."
        );
        assert_eq!(labels(questions[1]), ["Yes", "No"]);
        assert!(questions[2].options.is_empty() && questions[3].options.is_empty());
        assert!(questions.iter().all(|question| !question.multi_select));
        // Nothing was cancelled behind the user's back, and no RPC request
        // waits on an answer that Pi never acknowledges.
        assert!(harness.sent().is_empty());
        assert!(
            harness
                .pending
                .lock()
                .values()
                .all(PendingResponse::is_prompt)
        );

        let answer = |id: &str, answers: &[&str]| {
            driver.respond_user_input(
                id.to_owned(),
                vec![UserInputAnswer {
                    question_id: "value".to_owned(),
                    answers: answers.iter().map(|answer| (*answer).to_owned()).collect(),
                }],
            );
        };
        answer("79a3b55c-2199-4cd8-a90a-941b2f1ee579", &["high"]);
        answer("confirm-1", &["No"]);
        answer("input-1", &["refactor"]);
        answer("editor-1", &[]);
        // Already answered, and never asked.
        answer("confirm-1", &["Yes"]);
        answer("never-asked", &["Yes"]);
        let responses = harness
            .sent()
            .into_iter()
            .map(|message| match message {
                CommandMessage::ExtensionUiResponse(response) => response,
                _ => panic!("expected only dialog responses"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            responses,
            [
                json!({"type": "extension_ui_response", "id": "79a3b55c-2199-4cd8-a90a-941b2f1ee579", "value": "high"}),
                json!({"type": "extension_ui_response", "id": "confirm-1", "confirmed": false}),
                json!({"type": "extension_ui_response", "id": "input-1", "value": "refactor"}),
                json!({"type": "extension_ui_response", "id": "editor-1", "cancelled": true}),
            ]
        );
    }

    #[test]
    fn chocopi_dialog_answers_respect_options_timeouts_and_cancel() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let driver = harness.driver();
        harness.feed_value(json!({"type": "agent_start"}));
        harness.feed(
            &[
                json!({"type": "extension_ui_request", "id": "pick", "method": "select", "title": "Pick", "options": ["a", "b"]}),
                json!({"type": "extension_ui_request", "id": "yes", "method": "confirm", "title": "Sure?", "message": "Really"}),
                json!({"type": "extension_ui_request", "id": "late", "method": "confirm", "title": "Quick", "message": "?", "timeout": 1}),
                json!({"type": "extension_ui_request", "id": "open", "method": "input", "title": "Name"}),
                json!({"type": "extension_ui_request", "id": "empty", "method": "select", "title": "None", "options": []}),
            ]
            .map(|frame| frame.to_string())
            .join("\n"),
        );
        // A select with nothing to choose cannot be answered.
        assert!(matches!(
            harness.sent().as_slice(),
            [CommandMessage::CancelExtensionRequest(id)] if id == "empty"
        ));
        thread::sleep(Duration::from_millis(10));
        let answer = |id: &str, value: &str| {
            driver.respond_user_input(
                id.to_owned(),
                vec![UserInputAnswer {
                    question_id: "value".to_owned(),
                    answers: vec![value.to_owned()],
                }],
            );
        };
        answer("pick", "c");
        answer("yes", "Yes");
        answer("late", "Yes");
        let responses = harness
            .sent()
            .into_iter()
            .map(|message| match message {
                CommandMessage::ExtensionUiResponse(response) => response,
                _ => panic!("expected only dialog responses"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            responses,
            [
                json!({"type": "extension_ui_response", "id": "pick", "cancelled": true}),
                json!({"type": "extension_ui_response", "id": "yes", "confirmed": true}),
            ],
            "an expired dialog was already resolved by Pi"
        );

        // Stopping the turn releases the extension still waiting on a dialog.
        driver.cancel();
        let sent = harness.sent();
        assert!(matches!(
            sent.as_slice(),
            [CommandMessage::CancelExtensionRequest(id), CommandMessage::Cancel] if id == "open"
        ));
    }

    /// W6 (d)
    #[test]
    fn chocopi_error_notifications_surface_and_others_do_not() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let events = harness.feed(
            &[
                json!({"type": "extension_ui_request", "id": "n1", "method": "notify", "message": "boom", "notifyType": "error"}),
                json!({"type": "extension_ui_request", "id": "n2", "method": "notify", "message": "fine", "notifyType": "warning"}),
                json!({"type": "extension_ui_request", "id": "n3", "method": "setTitle", "title": "x"}),
            ]
            .map(|frame| frame.to_string())
            .join("\n"),
        );
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["error boom"]
        );
        assert!(harness.sent().is_empty());
    }

    /// W6 (e): a schema this build does not know is ignored, never guessed.
    #[test]
    fn chocopi_ignores_newer_bridge_schemas() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let shell = json!({"id": "sh-2", "name": null, "command": "make", "cwd": "/w",
            "pid": 1, "exitCode": null, "startedAt": 1, "finishedAt": null,
            "toolCallId": null, "tailPath": null});
        let events = harness.feed_value(entry(
            "choco-pi:host-bridge/shell",
            json!({"schema": 2, "at": 1, "event": "started", "shell": shell, "live": ["sh-2"]}),
        ));
        assert!(events.is_empty(), "{events:?}");
        let events = harness.feed_value(entry(
            "choco-pi:host-bridge/goal",
            json!({"schema": 2, "at": 1, "goal": null}),
        ));
        assert!(events.is_empty(), "{events:?}");

        let events = harness.feed_value(entry(
            "choco-pi:host-bridge/shell",
            json!({"schema": 1, "at": 1, "event": "started", "shell": shell, "live": ["sh-2"]}),
        ));
        assert_eq!(events.len(), 2);
        let events = harness.feed_value(status_frame(&json!({
            "schema": 2, "at": 2, "shells": [],
            "subagents": [], "processes": []
        })));
        assert!(events.is_empty(), "{events:?}");
    }

    /// W6 (f): a stop the extension did not handle fails visibly; one it
    /// handled waits for the `stopped` entry.
    #[test]
    fn chocopi_stop_requires_a_handled_command() {
        let harness = Harness::new(PiFlavor::ChocoPi);
        let key = BackgroundWorkKey::new(BackgroundWorkKind::Subagent, "agent-1");
        let run_stop = |response: Value| {
            let pending = harness.pending.clone();
            let thread_key = key.clone();
            let handle = thread::spawn(move || {
                let mut wire = Vec::new();
                let result =
                    stop_via_bridge(&mut wire, &pending, &mut 0, &thread_key, "agent-1", true);
                (result, wire)
            });
            while harness.pending.lock().is_empty() {
                thread::yield_now();
            }
            let (events, event_rx) = unbounded();
            let mut state = PiStreamState::default();
            handle_pi_message(
                PiFlavor::ChocoPi,
                response,
                &harness.pending,
                &harness.commands,
                &events,
                &mut state,
            );
            assert!(
                event_rx.try_recv().is_err(),
                "a command response is no turn"
            );
            handle.join().unwrap()
        };

        let (result, wire) = run_stop(json!({
            "id": "waku-1", "type": "response", "command": "prompt",
            "success": true, "data": {"disposition": "started"}
        }));
        assert_eq!(
            result.unwrap_err(),
            "choco-pi did not handle the stop request (started)"
        );
        let sent: Value = serde_json::from_slice(wire.trim_ascii_end()).unwrap();
        assert_eq!(
            sent,
            json!({"id": "waku-1", "type": "prompt", "message": "/host-bridge stop subagent agent-1"})
        );

        let (result, _) = run_stop(json!({
            "id": "waku-1", "type": "response", "command": "prompt",
            "success": false, "error": "Unknown agent"
        }));
        assert_eq!(result.unwrap_err(), "Unknown agent");

        let (result, _) = run_stop(json!({
            "id": "waku-1", "type": "response", "command": "prompt",
            "success": true, "data": {"disposition": "handled"}
        }));
        assert_eq!(result, Ok(()));

        let shell = BackgroundWorkKey::new(BackgroundWorkKind::Process, "sh-1");
        assert_eq!(
            stop_command(&shell, "sh-1", true).as_deref(),
            Ok("/host-bridge stop shell sh-1")
        );
        assert!(stop_command(&shell, "sh 1", true).is_err());
        assert!(stop_command(&shell, "sh-1", false).is_err());
        let monitor = BackgroundWorkKey::new(BackgroundWorkKind::Monitor, "m");
        assert!(stop_command(&monitor, "m", true).is_err());
    }

    /// W6 (g): plain Pi neither decodes the bridge nor surfaces dialogs,
    /// even on a machine with a choco-pi marker.
    #[test]
    fn pi_flavor_ignores_host_bridge_traffic() {
        for flavor in [PiFlavor::Pi, PiFlavor::OhMyPi] {
            let mut harness = Harness::new(flavor);
            assert!(harness.state.bridge.is_none());
            let events = harness.feed(LIFECYCLE);
            assert!(
                events
                    .iter()
                    .all(|event| !matches!(event, DriverEvent::BackgroundWork(_))),
                "{flavor:?} decoded bridge entries: {events:?}"
            );
            let events = harness.feed(SNAPSHOT);
            assert!(events.is_empty(), "{flavor:?}: {events:?}");
            let events = harness.feed(DIALOGS);
            assert!(events.is_empty(), "{flavor:?}: {events:?}");
            let cancelled = harness
                .sent()
                .into_iter()
                .map(|message| match message {
                    CommandMessage::CancelExtensionRequest(id) => id,
                    _ => panic!("expected only cancellations"),
                })
                .collect::<Vec<_>>();
            assert_eq!(
                cancelled,
                [
                    "79a3b55c-2199-4cd8-a90a-941b2f1ee579",
                    "confirm-1",
                    "input-1",
                    "editor-1"
                ]
            );
        }
    }

    #[test]
    fn chocopi_snapshot_reconciles_once_and_reports_the_goal() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let writer = harness.command_prompt(HOST_BRIDGE_SNAPSHOT, true);
        let events = harness.feed(SNAPSHOT);
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            [
                "upsert subagent:agent-live Running",
                "upsert shell:sh-live Running",
                "reconcile [agent-live,sh-live]",
                "goal Active",
            ]
        );
        let Some(DriverEvent::GoalUpdated(Some(goal))) = events.last() else {
            panic!("missing goal");
        };
        assert_eq!(
            goal,
            &ThreadGoal {
                objective: "Ship the host bridge".to_owned(),
                status: ThreadGoalStatus::Active,
                token_budget: Some(200_000),
                tokens_used: 51_234,
                time_used_seconds: 612,
            }
        );
        let (result, wire) = writer.join().unwrap();
        assert!(result.is_ok());
        let sent: Value = serde_json::from_slice(wire.trim_ascii_end()).unwrap();
        assert_eq!(
            sent,
            json!({"id": "waku-1", "type": "prompt", "message": "/host-bridge snapshot"})
        );
        assert!(harness.pending.lock().is_empty());
        assert!(harness.state.bridge_live.load(Ordering::Acquire));
    }

    #[test]
    fn chocopi_empty_snapshot_retires_work_that_is_gone() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        // Live before reconnecting, gone by the time the snapshot runs.
        harness.feed(
            LIFECYCLE
                .lines()
                .take(2)
                .collect::<Vec<_>>()
                .join("\n")
                .as_str(),
        );
        let writer = harness.command_prompt(HOST_BRIDGE_SNAPSHOT, true);
        let events = harness.feed_value(json!({
            "id": "waku-1", "type": "response", "command": "prompt",
            "success": true, "data": {"disposition": "handled"}
        }));
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["reconcile []"]
        );
        assert!(writer.join().unwrap().0.is_ok());
        assert!(!harness.state.bridge_live.load(Ordering::Acquire));
        // Progress cannot revive what the snapshot retired.
        let events = harness.feed(LIFECYCLE.lines().nth(2).unwrap());
        assert!(events.is_empty(), "{events:?}");
    }

    #[test]
    fn chocopi_failed_snapshot_keeps_the_previous_live_set() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        harness.feed(
            LIFECYCLE
                .lines()
                .take(2)
                .collect::<Vec<_>>()
                .join("\n")
                .as_str(),
        );
        let writer = harness.command_prompt(HOST_BRIDGE_SNAPSHOT, true);
        let events = harness.feed_value(json!({
            "id": "waku-1", "type": "response", "command": "prompt",
            "success": true, "data": {"disposition": "started"}
        }));
        assert!(events.is_empty(), "{events:?}");
        assert!(writer.join().unwrap().0.is_ok());
        // The next transition still reconciles against the remembered set.
        let events = harness.feed(LIFECYCLE.lines().nth(5).unwrap());
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            [
                "upsert shell:sh-1 Running",
                "reconcile [f67bce3e-2646-476,sh-1]"
            ]
        );
    }

    /// The writer stops waiting after its timeout and drops the request,
    /// but a late answer still settles what the snapshot streamed.
    #[test]
    fn chocopi_late_snapshot_response_still_settles() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let (sender, _receiver) = bounded(1);
        harness
            .pending
            .lock()
            .insert("waku-1".to_owned(), PendingResponse::Snapshot(sender));
        let lines = SNAPSHOT.lines().collect::<Vec<_>>();
        let events = harness.feed(&lines[..3].join("\n"));
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            [
                "upsert subagent:agent-live Running",
                "upsert shell:sh-live Running"
            ]
        );
        // The request timed out.
        harness.pending.lock().clear();
        let events = harness.feed(lines[3]);
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["reconcile [agent-live,sh-live]", "goal Active"]
        );
        assert_eq!(
            harness.state.bridge.as_ref().unwrap().snapshot_request(),
            None
        );
    }

    /// A command prompt answered mid-turn must not retire, or be mistaken
    /// for, the user's prompt marker.
    #[test]
    fn chocopi_command_prompts_leave_the_turn_marker_alone() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        send_prompt(&mut Vec::new(), &harness.pending, &mut 0, "hello").unwrap();
        let pending = harness.pending.clone();
        let writer = thread::spawn(move || {
            send_command_prompt(&mut Vec::new(), &pending, &mut 1, "/goal pause", false)
        });
        while harness.pending.lock().len() < 2 {
            thread::yield_now();
        }
        let events = harness.feed_value(json!({
            "id": "waku-2", "type": "response", "command": "prompt",
            "success": true, "data": {"disposition": "handled"}
        }));
        assert!(events.is_empty(), "{events:?}");
        assert!(writer.join().unwrap().is_ok());
        assert!(matches!(
            harness.pending.lock().get("waku-1"),
            Some(PendingResponse::Prompt)
        ));
        let events = harness.feed_value(json!({
            "id": "waku-1", "type": "response", "command": "prompt",
            "success": false, "error": "No model selected"
        }));
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            [
                "turn started",
                "error No model selected",
                "turn finished false"
            ]
        );
    }

    #[test]
    fn chocopi_dialogs_outside_a_turn_are_cancelled() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        // choco-pi-goal's resume confirm during `switch_session`.
        let events = harness.feed_value(json!({
            "type": "extension_ui_request", "id": "resume-goal", "method": "confirm",
            "title": "Resume paused goal?", "message": "Goal: ship"
        }));
        assert!(events.is_empty(), "{events:?}");
        assert!(matches!(
            harness.sent().as_slice(),
            [CommandMessage::CancelExtensionRequest(id)] if id == "resume-goal"
        ));
        assert!(harness.dialogs.lock().is_empty());
    }

    #[test]
    fn chocopi_handled_response_does_not_cut_short_a_started_run() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        send_prompt(&mut Vec::new(), &harness.pending, &mut 0, "/goal ship").unwrap();
        let events = harness.feed(
            &[
                json!({"type": "agent_start"}),
                json!({"id": "waku-1", "type": "response", "command": "prompt",
                    "success": true, "data": {"disposition": "handled"}}),
                json!({"type": "agent_settled"}),
            ]
            .map(|frame| frame.to_string())
            .join("\n"),
        );
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["turn started", "turn finished true"]
        );
        assert!(harness.pending.lock().is_empty());
    }

    /// A user-typed extension command (`/effort`) never starts a run; its
    /// handled response is what settles the turn.
    #[test]
    fn chocopi_locally_handled_user_commands_settle_their_turn() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        send_prompt(&mut Vec::new(), &harness.pending, &mut 0, "/effort").unwrap();
        let events = harness.feed_value(json!({
            "id": "waku-1", "type": "response", "command": "prompt",
            "success": true, "data": {"disposition": "handled"}
        }));
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["turn started", "turn finished true"]
        );
        assert!(harness.pending.lock().is_empty());

        // An accepted prompt that started the model still waits for its run.
        send_prompt(&mut Vec::new(), &harness.pending, &mut 1, "hi").unwrap();
        let events = harness.feed_value(json!({
            "id": "waku-2", "type": "response", "command": "prompt",
            "success": true, "data": {"disposition": "started"}
        }));
        assert!(events.is_empty(), "{events:?}");
    }

    #[test]
    fn chocopi_goal_operations_map_to_choco_pi_commands() {
        let set = |objective: Option<&str>, status, replace| GoalOperation::Set {
            objective: objective.map(str::to_owned),
            status,
            replace,
        };
        assert_eq!(
            goal_commands(&set(Some("Ship it"), None, false), true).unwrap(),
            ["/goal Ship it"]
        );
        assert_eq!(
            goal_commands(
                &set(Some("Ship it"), Some(ThreadGoalStatus::Active), true),
                true
            )
            .unwrap(),
            ["/goal clear", "/goal Ship it"]
        );
        assert_eq!(
            goal_commands(&set(None, Some(ThreadGoalStatus::Paused), false), true).unwrap(),
            ["/goal pause"]
        );
        assert_eq!(
            goal_commands(&set(None, Some(ThreadGoalStatus::Active), false), true).unwrap(),
            ["/goal resume"]
        );
        assert_eq!(
            goal_commands(&set(None, Some(ThreadGoalStatus::Complete), false), false).unwrap(),
            ["/host-bridge goal complete"]
        );
        assert_eq!(
            goal_commands(&GoalOperation::Clear, true).unwrap(),
            ["/goal clear"]
        );
        assert_eq!(
            goal_commands(&GoalOperation::Refresh, false).unwrap(),
            [HOST_BRIDGE_SNAPSHOT]
        );
        assert!(goal_commands(&set(None, Some(ThreadGoalStatus::Blocked), false), true).is_err());
        assert!(goal_commands(&GoalOperation::Clear, false).is_err());
    }

    #[test]
    fn chocopi_controls_are_inert_for_other_flavors() {
        for flavor in [PiFlavor::Pi, PiFlavor::OhMyPi] {
            let harness = Harness::new(flavor);
            let driver = harness.driver();
            driver.refresh_background_work();
            driver.stop_background_work(
                BackgroundWorkKey::new(BackgroundWorkKind::Subagent, "a"),
                "a".to_owned(),
            );
            driver.goal(GoalOperation::Refresh);
            assert!(harness.sent().is_empty(), "{flavor:?}");
        }
        let harness = Harness::new(PiFlavor::ChocoPi);
        let driver = harness.driver();
        driver.refresh_background_work();
        driver.goal(GoalOperation::Clear);
        assert!(matches!(
            harness.sent().as_slice(),
            [
                CommandMessage::RefreshBackgroundWork,
                CommandMessage::Goal(GoalOperation::Clear)
            ]
        ));
    }

    #[test]
    fn host_bridge_commands_need_the_registered_command() {
        let catalog = json!({"data": {"commands": [
            {"name": "goal", "source": "extension"},
            {"name": "host-bridge", "source": "extension"}
        ]}});
        assert!(catalog_has_command(&catalog, HOST_BRIDGE_COMMAND));
        assert!(catalog_has_command(&catalog, GOAL_COMMAND));
        assert!(!catalog_has_command(
            &json!({"data": {"commands": [{"name": "goal"}]}}),
            HOST_BRIDGE_COMMAND
        ));
        assert!(!catalog_has_command(&json!({}), HOST_BRIDGE_COMMAND));
    }

    /// Captured foreground `Agent` frames: progress is held to the debounce
    /// window, the newest held frame still lands before the end, and the
    /// progress record becomes a one-line detail rather than output.
    #[test]
    fn chocopi_debounces_agent_progress_without_losing_the_last_update() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let events = harness.feed(AGENT_UPDATES);
        let activities = events
            .iter()
            .map(|event| match event {
                DriverEvent::RichActivity(item) => item,
                other => panic!("unexpected {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(activities.len(), 4, "{activities:?}");
        assert_eq!(activities[0].title, "Foreground transport probe listing");
        assert_eq!(
            activities[1].detail.as_deref(),
            Some("0 tool uses · thinking…")
        );
        assert_eq!(activities[1].output, None);
        assert_eq!(
            activities[2].detail.as_deref(),
            Some(
                "1 tool uses · 14.2k token · The current working directory is empty. One `ls` call return…"
            )
        );
        assert!(!activities[2].complete);
        assert!(activities[3].complete);
        assert!(
            activities[3]
                .output
                .as_deref()
                .is_some_and(|output| output.contains("Agent completed in 3.9s"))
        );
        assert!(harness.state.tool_updates.is_empty());

        // Plain Pi passes every update through untouched.
        let mut harness = Harness::new(PiFlavor::Pi);
        let events = harness.feed(AGENT_UPDATES);
        assert_eq!(events.len(), 5);
        assert!(events.iter().all(
            |event| matches!(event, DriverEvent::RichActivity(item) if item.detail.is_none())
        ));
    }

    #[test]
    fn chocopi_held_progress_flushes_once_its_window_passes() {
        let mut harness = Harness::new(PiFlavor::ChocoPi);
        let lines = AGENT_UPDATES.lines().collect::<Vec<_>>();
        let events = harness.feed(&lines[..3].join("\n"));
        assert_eq!(events.len(), 2, "the second update is held");
        thread::sleep(TOOL_UPDATE_DEBOUNCE + Duration::from_millis(10));
        let events = harness.feed_value(json!({"type": "turn_start"}));
        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["activity false", "turn started"]
        );
    }

    #[test]
    fn chocopi_tool_titles_cover_its_tool_surface() {
        assert_eq!(tool_title("exec_command"), tr!("activity.run_command"));
        assert_eq!(tool_title("shell_start"), tr!("activity.run_command"));
        assert_eq!(tool_title("exec"), tr!("activity.run_command"));
        assert_eq!(tool_title("read_symbol"), tr!("activity.read_file"));
        assert_eq!(tool_title("ast_grep_search"), tr!("activity.search_files"));
        assert_eq!(tool_title("Agent"), tr!("background.subagent"));
    }

    /// Drives a real choco-pi through a background subagent. Ignored by
    /// default: it needs an installed, authenticated choco-pi profile and
    /// makes billable model calls. `WAKU_CHOCOPI_BIN` overrides the launcher
    /// (default `~/.local/pi-shim/pi`).
    #[test]
    #[ignore = "requires an installed, authenticated choco-pi and makes model calls"]
    fn chocopi_background_subagent_wakes_the_session() {
        let binary = std::env::var_os("WAKU_CHOCOPI_BIN")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/pi-shim/pi"))
            })
            .expect("set WAKU_CHOCOPI_BIN or HOME");
        let (events, event_rx) = crate::driver::test_event_channel();
        let driver = PiDriver::start(
            PiFlavor::ChocoPi,
            DriverStartOptions {
                binary,
                cwd: std::env::temp_dir(),
                mode: RuntimeMode::FullAccess,
                model: std::env::var("WAKU_CHOCOPI_MODEL").ok(),
                reasoning_effort: None,
                service_tier: None,
                context_window: None,
                agent_preset: None,
                computer_use_enabled: false,
                provider_cursor: None,
            },
            events,
        )
        .expect("the choco-pi RPC session should start");

        let mut connected = false;
        while let Ok(event) = event_rx.recv_timeout(Duration::from_secs(60)) {
            match event {
                DriverEvent::Connected { provider_cursor } => {
                    assert!(matches!(
                        provider_cursor,
                        Some(ProviderResumeCursor::ChocoPi { .. })
                    ));
                    connected = true;
                    break;
                }
                DriverEvent::Error(error) => panic!("choco-pi failed to initialize: {error}"),
                _ => {}
            }
        }
        assert!(connected, "choco-pi never reported its session");

        driver.prompt(
            "Use the Agent tool exactly once with run_in_background: true, subagent_type \
             explore, max_turns 2, to list the current directory with ls. Then reply \
             with exactly: STARTED. Do not wait for the agent."
                .into(),
        );
        let mut turns_started = 0;
        let mut subagent = None::<String>;
        let mut saw_running = false;
        let mut saw_completed = false;
        let deadline = Instant::now() + Duration::from_secs(300);
        while Instant::now() < deadline && turns_started < 2 {
            let Ok(event) = event_rx.recv_timeout(Duration::from_secs(30)) else {
                continue;
            };
            match event {
                DriverEvent::TurnStarted => {
                    turns_started += 1;
                    if turns_started == 2 {
                        assert!(saw_completed, "the wake must follow the completion");
                    }
                }
                DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item))
                    if item.key.kind == BackgroundWorkKind::Subagent && item.background =>
                {
                    let id = subagent.get_or_insert_with(|| item.key.provider_id.clone());
                    if *id == item.key.provider_id {
                        saw_running |= item.status == BackgroundWorkStatus::Running;
                        saw_completed |= item.status == BackgroundWorkStatus::Completed;
                    }
                }
                DriverEvent::ProcessExited => panic!("choco-pi exited"),
                _ => {}
            }
        }
        assert!(saw_running, "the background subagent never ran");
        assert!(saw_completed, "the background subagent never completed");
        assert_eq!(turns_started, 2, "completion should wake a second turn");
    }
}
