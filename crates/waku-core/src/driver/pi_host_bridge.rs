//! Decodes choco-pi's host bridge into Waku background work and goals.
//!
//! choco-pi records every subagent, managed shell, and goal transition as a
//! durable `choco-pi:host-bridge/<kind>` custom entry, which Pi streams as
//! `entry_appended`, and pushes transient progress through a `setStatus`
//! frame under its own status key. Both carry `schema: 1`; anything newer is
//! ignored rather than guessed at, so a newer choco-pi degrades to Pi-level
//! behavior instead of misreporting work.
//!
//! The state lives on the Pi reader thread. Output files are tailed there
//! too, in bounded increments, only when a transition or progress frame says
//! something may have changed — never on a timer and never from the UI.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use serde_json::Value;

use super::activity;
use crate::choco_pi_profile::{HOST_BRIDGE_SCHEMA, HostBridgeSpec};
use crate::model::{
    BackgroundWorkEvent, BackgroundWorkItem, BackgroundWorkKey, BackgroundWorkKind,
    BackgroundWorkStatus, DriverEvent, ThreadGoal, ThreadGoalStatus,
};

/// The most one tail pass reads from an output file. When more than this
/// has accumulated since the last pass, the gap is skipped and the item is
/// marked truncated rather than stalling the reader thread on a large file.
pub(super) const MAX_TAIL_READ_BYTES: u64 = 256 * 1024;

/// A single unterminated transcript line larger than this is abandoned: it
/// is a tool result too large to render as one row anyway.
const MAX_PARTIAL_LINE_BYTES: usize = 4 * MAX_TAIL_READ_BYTES as usize;

const MAX_SUBJECT_CHARS: usize = 120;

pub(super) struct HostBridgeState {
    spec: HostBridgeSpec,
    subagents: HashMap<String, BackgroundWorkItem>,
    shells: HashMap<String, BackgroundWorkItem>,
    live_subagents: Vec<String>,
    live_shells: Vec<String>,
    /// The `at` of the newest entry applied per item. Entries can arrive out
    /// of order (`started` before `created`), and an older one must not
    /// undo a newer transition.
    entry_at: HashMap<BackgroundWorkKey, u64>,
    tails: HashMap<BackgroundWorkKey, TailCursor>,
    snapshot: Option<Snapshot>,
}

/// An in-flight `/host-bridge snapshot`. Its entries are authoritative for
/// what is live, so per-entry reconciles wait for the command response and
/// one reconcile covers both kinds at once.
struct Snapshot {
    request_id: String,
    goal: Option<Option<ThreadGoal>>,
    saw_subagents: bool,
    saw_shells: bool,
    previous_subagents: Vec<String>,
    previous_shells: Vec<String>,
}

struct TailCursor {
    path: PathBuf,
    offset: u64,
    /// Subagent outputs are JSONL transcripts rendered line by line; shell
    /// outputs are raw text.
    transcript: bool,
    /// Bytes read but not yet emitted: an unterminated transcript line, or
    /// the start of a UTF-8 sequence split across reads.
    carry: Vec<u8>,
    /// After a skip the next bytes start mid-line; drop through the first
    /// newline instead of rendering half a record.
    skip_partial_line: bool,
}

impl HostBridgeState {
    pub(super) fn new(spec: HostBridgeSpec) -> Self {
        Self {
            spec,
            subagents: HashMap::new(),
            shells: HashMap::new(),
            live_subagents: Vec::new(),
            live_shells: Vec::new(),
            entry_at: HashMap::new(),
            tails: HashMap::new(),
            snapshot: None,
        }
    }

    pub(super) fn status_key(&self) -> &str {
        &self.spec.status_key
    }

    /// Whether any work the bridge reported is still running. Lets the
    /// writer skip periodic snapshots when there is nothing to reconcile.
    pub(super) fn has_live(&self) -> bool {
        self.subagents
            .values()
            .chain(self.shells.values())
            .any(|item| item.status.is_live())
    }

    /// Whether `entry` is a host-bridge record this build should read.
    pub(super) fn owns_entry(&self, entry: &Value) -> bool {
        self.entry_kind(entry).is_some()
    }

    fn entry_kind<'a>(&self, entry: &'a Value) -> Option<&'a str> {
        if entry.get("type").and_then(Value::as_str) != Some("custom") {
            return None;
        }
        let kind = entry
            .get("customType")
            .and_then(Value::as_str)?
            .strip_prefix(self.spec.entry_prefix.as_str())?;
        // The marker names the prefix without its separator; the entries
        // themselves are `<prefix>/<kind>`.
        Some(kind.strip_prefix('/').unwrap_or(kind))
    }

    pub(super) fn snapshot_request(&self) -> Option<&str> {
        self.snapshot
            .as_ref()
            .map(|snapshot| snapshot.request_id.as_str())
    }

    pub(super) fn begin_snapshot(&mut self, request_id: String) {
        let (previous_subagents, previous_shells) = match self.snapshot.take() {
            // A superseded snapshot never finished; keep the last settled
            // live sets as the fallback rather than its partial ones.
            Some(snapshot) => (snapshot.previous_subagents, snapshot.previous_shells),
            None => (
                std::mem::take(&mut self.live_subagents),
                std::mem::take(&mut self.live_shells),
            ),
        };
        self.live_subagents.clear();
        self.live_shells.clear();
        self.snapshot = Some(Snapshot {
            request_id,
            goal: None,
            saw_subagents: false,
            saw_shells: false,
            previous_subagents,
            previous_shells,
        });
    }

    /// The snapshot failed or was abandoned. A kind it never reported keeps
    /// its previous live set, so a failed refresh cannot mark work lost.
    pub(super) fn cancel_snapshot(&mut self) -> Vec<DriverEvent> {
        let Some(snapshot) = self.snapshot.take() else {
            return Vec::new();
        };
        if !snapshot.saw_subagents {
            self.live_subagents = snapshot.previous_subagents;
        }
        if !snapshot.saw_shells {
            self.live_shells = snapshot.previous_shells;
        }
        snapshot
            .goal
            .map(|goal| vec![DriverEvent::GoalUpdated(goal)])
            .unwrap_or_default()
    }

    /// The snapshot command was handled: everything it streamed is the whole
    /// live set, so anything not in it is gone.
    pub(super) fn finish_snapshot(&mut self) -> Vec<DriverEvent> {
        let Some(snapshot) = self.snapshot.take() else {
            return Vec::new();
        };
        let mut events = vec![self.reconcile()];
        if let Some(goal) = snapshot.goal {
            events.push(DriverEvent::GoalUpdated(goal));
        }
        events
    }

    pub(super) fn apply_entry(&mut self, entry: &Value) -> Vec<DriverEvent> {
        let Some(kind) = self.entry_kind(entry) else {
            return Vec::new();
        };
        let Some(data) = entry.get("data").filter(|data| supported_schema(data)) else {
            return Vec::new();
        };
        match kind {
            "subagent" => self.apply_subagent(data),
            "shell" => self.apply_shell(data),
            "goal" => self.apply_goal(data),
            // Workflows group subagents that already have their own entries;
            // Waku has no surface for the grouping itself.
            _ => Vec::new(),
        }
    }

    pub(super) fn apply_status(&mut self, text: &str) -> Vec<DriverEvent> {
        let Ok(frame) = serde_json::from_str::<Value>(text) else {
            return Vec::new();
        };
        if !supported_schema(&frame) {
            return Vec::new();
        }
        let at = frame.get("at").and_then(as_millis);
        let mut events = Vec::new();
        for progress in frame
            .get("subagents")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(id) = progress.get("id").and_then(Value::as_str) else {
                continue;
            };
            // Progress only refines work an entry introduced, and never
            // revives work an entry already settled.
            let Some(item) = self
                .subagents
                .get_mut(id)
                .filter(|item| item.status.is_live())
            else {
                continue;
            };
            if item.status == BackgroundWorkStatus::Starting
                && progress.get("status").and_then(Value::as_str) == Some("running")
            {
                item.status = BackgroundWorkStatus::Running;
            }
            item.detail = Some(progress_detail(progress));
            if let Some(duration) = progress.get("durationMs").and_then(as_millis) {
                item.duration_ms = Some(duration);
            }
            if let Some(at) = at {
                item.updated_at_ms = item.updated_at_ms.max(at);
            }
            let item = item.clone();
            events.push(upsert(item.clone()));
            self.tail(&item, &mut events);
        }
        for progress in frame
            .get("shells")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(item) = progress
                .get("id")
                .and_then(Value::as_str)
                .and_then(|id| self.shells.get(id))
                .filter(|item| item.status.is_live())
                .cloned()
            else {
                continue;
            };
            self.tail(&item, &mut events);
        }
        events
    }

    fn apply_subagent(&mut self, data: &Value) -> Vec<DriverEvent> {
        let Some(agent) = data.get("agent").filter(|agent| agent.is_object()) else {
            return Vec::new();
        };
        let Some(id) = non_empty_str(agent.get("id")) else {
            return Vec::new();
        };
        if !data
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(is_subagent_event)
        {
            return Vec::new();
        }
        let Some(status) = agent
            .get("status")
            .and_then(Value::as_str)
            .and_then(subagent_status)
        else {
            return Vec::new();
        };
        if !self.accepts(BackgroundWorkKind::Subagent, id, data.get("at"), status) {
            return Vec::new();
        }
        let title = non_empty_str(agent.get("description"))
            .or_else(|| non_empty_str(agent.get("role")))
            .map(str::to_owned)
            .unwrap_or_else(|| tr!("background.subagent"));
        let mut item = BackgroundWorkItem::new(BackgroundWorkKind::Subagent, id, title, status);
        item.role = non_empty_str(agent.get("role")).map(str::to_owned);
        item.model = non_empty_str(agent.get("model")).map(str::to_owned);
        item.parent_id = non_empty_str(agent.get("parentId")).map(str::to_owned);
        item.cwd = non_empty_str(agent.get("cwd")).map(str::to_owned);
        item.origin_activity_id = non_empty_str(agent.get("toolCallId")).map(str::to_owned);
        item.background = agent
            .get("background")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        item.control_id = Some(id.to_owned());
        item.can_stop = status.is_stoppable();
        let previous = self.subagents.get(id);
        item.detail = non_empty_str(agent.get("summary"))
            .map(str::to_owned)
            .or_else(|| previous.and_then(|previous| previous.detail.clone()));
        apply_times(
            &mut item,
            data.get("at"),
            agent.get("startedAt"),
            agent.get("finishedAt"),
        );
        if let Some(path) = non_empty_str(agent.get("outputFile")) {
            self.tails
                .entry(item.key.clone())
                .or_insert_with(|| TailCursor::new(PathBuf::from(path), true));
        }
        self.subagents.insert(id.to_owned(), item.clone());
        let live = live_ids(data);
        if let Some(snapshot) = self.snapshot.as_mut()
            && live.is_some()
        {
            snapshot.saw_subagents = true;
        }
        if let Some(live) = live {
            self.live_subagents = live;
        }
        include_transition(&mut self.live_subagents, id, status);
        self.transition_events(item)
    }

    fn apply_shell(&mut self, data: &Value) -> Vec<DriverEvent> {
        let Some(shell) = data.get("shell").filter(|shell| shell.is_object()) else {
            return Vec::new();
        };
        let Some(id) = non_empty_str(shell.get("id")) else {
            return Vec::new();
        };
        let exit_code = shell
            .get("exitCode")
            .and_then(|code| {
                code.as_i64()
                    .or_else(|| code.as_f64().map(|code| code as i64))
            })
            .and_then(|code| i32::try_from(code).ok());
        let status = match data.get("event").and_then(Value::as_str) {
            Some("started") => BackgroundWorkStatus::Running,
            // Only a clean exit is a success; a shell killed by a signal
            // reports no exit code at all.
            Some("exited") if exit_code == Some(0) => BackgroundWorkStatus::Completed,
            Some("exited") => BackgroundWorkStatus::Failed,
            Some("stopped") => BackgroundWorkStatus::Stopped,
            Some("lost") => BackgroundWorkStatus::Lost,
            _ => return Vec::new(),
        };
        if !self.accepts(BackgroundWorkKind::Process, id, data.get("at"), status) {
            return Vec::new();
        }
        let command = non_empty_str(shell.get("command")).map(str::to_owned);
        let title = non_empty_str(shell.get("name"))
            .map(str::to_owned)
            .or_else(|| command.clone())
            .unwrap_or_else(|| tr!("background.process"));
        let mut item = BackgroundWorkItem::new(BackgroundWorkKind::Process, id, title, status);
        item.command = command;
        item.cwd = non_empty_str(shell.get("cwd")).map(str::to_owned);
        item.origin_activity_id = non_empty_str(shell.get("toolCallId")).map(str::to_owned);
        item.exit_code = exit_code;
        item.background = true;
        item.control_id = Some(id.to_owned());
        item.can_stop = status.is_stoppable();
        apply_times(
            &mut item,
            data.get("at"),
            shell.get("startedAt"),
            shell.get("finishedAt"),
        );
        if let Some(path) = non_empty_str(shell.get("tailPath")) {
            self.tails
                .entry(item.key.clone())
                .or_insert_with(|| TailCursor::new(PathBuf::from(path), false));
        }
        self.shells.insert(id.to_owned(), item.clone());
        let live = live_ids(data);
        if let Some(snapshot) = self.snapshot.as_mut()
            && live.is_some()
        {
            snapshot.saw_shells = true;
        }
        if let Some(live) = live {
            self.live_shells = live;
        }
        include_transition(&mut self.live_shells, id, status);
        self.transition_events(item)
    }

    fn apply_goal(&mut self, data: &Value) -> Vec<DriverEvent> {
        let Some(goal) = data.get("goal") else {
            return Vec::new();
        };
        let goal = if goal.is_null() {
            None
        } else {
            let Some(goal) = parse_goal(goal) else {
                return Vec::new();
            };
            Some(goal)
        };
        if let Some(snapshot) = self.snapshot.as_mut() {
            snapshot.goal = Some(goal);
            return Vec::new();
        }
        vec![DriverEvent::GoalUpdated(goal)]
    }

    fn transition_events(&mut self, item: BackgroundWorkItem) -> Vec<DriverEvent> {
        let mut events = vec![upsert(item.clone())];
        self.tail(&item, &mut events);
        if self.snapshot.is_none() {
            events.push(self.reconcile());
        }
        events
    }

    /// Records an entry's `at` and says whether it is current. An entry
    /// older than the newest one applied is stale; one with the same `at`
    /// may refine the item but never move settled work back to live.
    fn accepts(
        &mut self,
        kind: BackgroundWorkKind,
        id: &str,
        at: Option<&Value>,
        status: BackgroundWorkStatus,
    ) -> bool {
        let Some(at) = at.and_then(as_millis) else {
            return true;
        };
        let key = BackgroundWorkKey::new(kind, id);
        if let Some(&recorded) = self.entry_at.get(&key) {
            let settled = match kind {
                BackgroundWorkKind::Subagent => self.subagents.get(id),
                BackgroundWorkKind::Process | BackgroundWorkKind::Monitor => self.shells.get(id),
            }
            .is_some_and(|item| !item.status.is_live());
            if at < recorded || (at == recorded && settled && status.is_live()) {
                return false;
            }
        }
        self.entry_at.insert(key, at);
        true
    }

    /// The live sets are a level signal. Mirror what the client does with a
    /// `ReconcileLive` — detached work missing from it is lost — so a later
    /// progress frame cannot resurrect work the client already settled.
    fn reconcile(&mut self) -> DriverEvent {
        for (live, items) in [
            (&self.live_subagents, &mut self.subagents),
            (&self.live_shells, &mut self.shells),
        ] {
            for (id, item) in items.iter_mut() {
                if item.background && item.status.is_live() && !live.contains(id) {
                    item.status = BackgroundWorkStatus::Lost;
                    item.can_stop = false;
                }
            }
        }
        let items = self
            .live_subagents
            .iter()
            .filter_map(|id| self.subagents.get(id))
            .chain(self.live_shells.iter().filter_map(|id| self.shells.get(id)))
            .filter(|item| item.status.is_live())
            .cloned()
            .collect();
        DriverEvent::BackgroundWork(BackgroundWorkEvent::ReconcileLive { items })
    }

    fn tail(&mut self, item: &BackgroundWorkItem, events: &mut Vec<DriverEvent>) {
        let Some(cursor) = self.tails.get_mut(&item.key) else {
            return;
        };
        let Some((text, truncated)) = cursor.read() else {
            return;
        };
        if truncated {
            // Output replaces the client's buffer only on an upsert, and the
            // skipped bytes mean what it holds no longer leads into this.
            let mut item = item.clone();
            item.output = Some(text);
            item.output_truncated = true;
            events.push(upsert(item));
        } else if !text.is_empty() {
            events.push(DriverEvent::BackgroundWork(
                BackgroundWorkEvent::OutputDelta {
                    key: item.key.clone(),
                    delta: text,
                },
            ));
        }
    }
}

impl TailCursor {
    fn new(path: PathBuf, transcript: bool) -> Self {
        Self {
            path,
            offset: 0,
            transcript,
            carry: Vec::new(),
            skip_partial_line: false,
        }
    }

    /// Reads what was appended since the last pass, at most
    /// [`MAX_TAIL_READ_BYTES`]. Returns the rendered text and whether bytes
    /// were skipped to stay within that bound.
    fn read(&mut self) -> Option<(String, bool)> {
        let mut file = File::open(&self.path).ok()?;
        let len = file.metadata().ok()?.len();
        if len < self.offset {
            // Replaced or truncated underneath us; start over.
            self.offset = 0;
            self.carry.clear();
            self.skip_partial_line = false;
        }
        let mut truncated = false;
        if len - self.offset > MAX_TAIL_READ_BYTES {
            self.offset = len - MAX_TAIL_READ_BYTES;
            self.carry.clear();
            self.skip_partial_line = self.transcript;
            truncated = true;
        }
        if len == self.offset {
            return None;
        }
        file.seek(SeekFrom::Start(self.offset)).ok()?;
        let mut bytes = Vec::new();
        file.by_ref()
            .take(MAX_TAIL_READ_BYTES)
            .read_to_end(&mut bytes)
            .ok()?;
        self.offset += bytes.len() as u64;
        if truncated && !self.transcript {
            // Skipping may land inside a UTF-8 sequence.
            let start = bytes
                .iter()
                .position(|byte| byte & 0b1100_0000 != 0b1000_0000)
                .unwrap_or(bytes.len());
            bytes.drain(..start);
        }
        self.carry.extend_from_slice(&bytes);
        let text = if self.transcript {
            self.render_transcript()
        } else {
            take_utf8(&mut self.carry)
        };
        (truncated || !text.is_empty()).then_some((text, truncated))
    }

    fn render_transcript(&mut self) -> String {
        let mut text = String::new();
        let Some(last_newline) = self.carry.iter().rposition(|byte| *byte == b'\n') else {
            if self.carry.len() > MAX_PARTIAL_LINE_BYTES {
                self.carry.clear();
                self.skip_partial_line = true;
            }
            return text;
        };
        let complete = self.carry.drain(..=last_newline).collect::<Vec<_>>();
        let mut lines = complete.split(|byte| *byte == b'\n');
        if std::mem::take(&mut self.skip_partial_line) {
            lines.next();
        }
        for line in lines {
            render_transcript_line(line, &mut text);
        }
        text
    }
}

/// Renders one transcript record the way Claude subagent output reads: the
/// assistant's narrative as-is and each tool call as one `› tool · subject`
/// line. Prompts and tool results stay out; they would drown the pane.
fn render_transcript_line(line: &[u8], text: &mut String) {
    if line.iter().all(u8::is_ascii_whitespace) {
        return;
    }
    let Ok(record) = serde_json::from_slice::<Value>(line) else {
        text.push_str(String::from_utf8_lossy(line).trim_end());
        text.push('\n');
        return;
    };
    let Some(message) = record.get("message") else {
        return;
    };
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return;
    }
    let Some(content) = message.get("content").and_then(Value::as_array) else {
        return;
    };
    for block in content {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(narrative) = block
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|narrative| !narrative.is_empty())
                {
                    text.push_str(narrative);
                    text.push_str("\n\n");
                }
            }
            Some("toolCall" | "tool_use") => {
                let name = non_empty_str(block.get("name")).unwrap_or("tool");
                let input = block.get("arguments").or_else(|| block.get("input"));
                match tool_subject(input) {
                    Some(subject) => text.push_str(&format!("› {name} · {subject}\n")),
                    None => text.push_str(&format!("› {name}\n")),
                }
            }
            _ => {}
        }
    }
}

fn tool_subject(input: Option<&Value>) -> Option<String> {
    activity::input_title(input)
        .or_else(|| {
            let input = input?;
            [
                "command",
                "cmd",
                "file_path",
                "path",
                "pattern",
                "query",
                "url",
                "description",
            ]
            .into_iter()
            .find_map(|key| input.get(key).and_then(Value::as_str))
            .map(str::to_owned)
        })
        .and_then(|subject| one_line(&subject))
}

pub(super) fn one_line(text: &str) -> Option<String> {
    let line = text.lines().map(str::trim).find(|line| !line.is_empty())?;
    if line.chars().count() <= MAX_SUBJECT_CHARS {
        return Some(line.to_owned());
    }
    let mut short = line.chars().take(MAX_SUBJECT_CHARS).collect::<String>();
    short.push('…');
    Some(short)
}

/// Emits the decodable prefix, keeping an incomplete trailing sequence for
/// the next read. Invalid bytes elsewhere are replaced, not dropped.
fn take_utf8(carry: &mut Vec<u8>) -> String {
    match std::str::from_utf8(carry) {
        Ok(text) => {
            let text = text.to_owned();
            carry.clear();
            text
        }
        Err(error) if error.error_len().is_none() => {
            let valid = error.valid_up_to();
            let text = String::from_utf8_lossy(&carry[..valid]).into_owned();
            carry.drain(..valid);
            text
        }
        Err(_) => {
            let text = String::from_utf8_lossy(carry).into_owned();
            carry.clear();
            text
        }
    }
}

fn supported_schema(data: &Value) -> bool {
    data.get("schema")
        .and_then(Value::as_u64)
        .is_some_and(|schema| (1..=HOST_BRIDGE_SCHEMA).contains(&schema))
}

/// The transitions schema 1 defines. The event only describes the edge;
/// an entry whose event this build does not know is skipped whole.
fn is_subagent_event(event: &str) -> bool {
    matches!(
        event,
        "created" | "started" | "completed" | "failed" | "stopped" | "lost" | "steered" | "resumed"
    )
}

/// The agent's own status decides where it stands, not the event: during
/// spawn `started` can precede `created`, and both say `running`. Waku has
/// no queued state; a queued agent is starting.
fn subagent_status(status: &str) -> Option<BackgroundWorkStatus> {
    Some(match status {
        "queued" => BackgroundWorkStatus::Starting,
        "running" => BackgroundWorkStatus::Running,
        "completed" => BackgroundWorkStatus::Completed,
        "failed" => BackgroundWorkStatus::Failed,
        "stopped" => BackgroundWorkStatus::Stopped,
        "lost" => BackgroundWorkStatus::Lost,
        _ => return None,
    })
}

fn parse_goal(goal: &Value) -> Option<ThreadGoal> {
    let status = match goal.get("status").and_then(Value::as_str)? {
        "active" => ThreadGoalStatus::Active,
        "paused" => ThreadGoalStatus::Paused,
        "budgetLimited" => ThreadGoalStatus::BudgetLimited,
        "complete" => ThreadGoalStatus::Complete,
        _ => return None,
    };
    Some(ThreadGoal {
        objective: goal.get("objective").and_then(Value::as_str)?.to_owned(),
        status,
        token_budget: goal.get("tokenBudget").and_then(as_integer),
        tokens_used: goal.get("tokensUsed").and_then(as_integer).unwrap_or(0),
        time_used_seconds: goal.get("activeSeconds").and_then(as_integer).unwrap_or(0),
    })
}

/// `"<toolUses> tool uses · <tokens> tokens · <activity>"`, the activity
/// left off while the agent has none.
fn progress_detail(progress: &Value) -> String {
    let tool_uses = progress.get("toolUses").and_then(as_integer).unwrap_or(0);
    let tokens = progress.get("tokens").and_then(as_integer).unwrap_or(0);
    let mut detail = format!("{tool_uses} tool uses · {tokens} tokens");
    if let Some(activity) = progress
        .get("activity")
        .and_then(Value::as_str)
        .and_then(one_line)
    {
        detail.push_str(" · ");
        detail.push_str(&activity);
    }
    detail
}

fn apply_times(
    item: &mut BackgroundWorkItem,
    at: Option<&Value>,
    started_at: Option<&Value>,
    finished_at: Option<&Value>,
) {
    let at = at.and_then(as_millis);
    let started_at = started_at.and_then(as_millis);
    let finished_at = finished_at.and_then(as_millis);
    if let Some(started_at) = started_at.or(at) {
        item.started_at_ms = started_at;
    }
    if let Some(updated_at) = at.or(finished_at) {
        item.updated_at_ms = updated_at.max(item.started_at_ms);
    }
    if let (Some(started_at), Some(finished_at)) = (started_at, finished_at) {
        item.duration_ms = Some(finished_at.saturating_sub(started_at));
    }
}

/// The entry's own edge is as current as its level signal; keep the two
/// consistent even if a publisher leaves queued work out of `live`.
fn include_transition(live: &mut Vec<String>, id: &str, status: BackgroundWorkStatus) {
    let present = live.iter().any(|live_id| live_id == id);
    if status.is_live() && !present {
        live.push(id.to_owned());
    } else if !status.is_live() && present {
        live.retain(|live_id| live_id != id);
    }
}

fn live_ids(data: &Value) -> Option<Vec<String>> {
    data.get("live").and_then(Value::as_array).map(|ids| {
        ids.iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    })
}

fn upsert(item: BackgroundWorkItem) -> DriverEvent {
    DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item))
}

fn non_empty_str(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

fn as_millis(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| {
        value
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(|value| value as u64)
    })
}

fn as_integer(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(|value| value as i64)
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::path::Path;

    use serde_json::json;

    use super::*;

    fn bridge() -> HostBridgeState {
        HostBridgeState::new(HostBridgeSpec::default())
    }

    fn subagent_entry(
        event: &str,
        status: &str,
        live: &[&str],
        output_file: Option<&str>,
    ) -> Value {
        json!({
            "type": "custom",
            "customType": "choco-pi:host-bridge/subagent",
            "data": {
                "schema": 1, "at": 2000, "event": event,
                "agent": {
                    "id": "agent-1", "parentId": "agent-0", "role": "explore",
                    "model": null, "description": "Survey", "background": true,
                    "cwd": null, "toolCallId": null, "outputFile": output_file,
                    "status": status, "startedAt": 1000,
                    "finishedAt": if event == "started" { Value::Null } else { json!(1500) },
                    "summary": null, "futureField": {"ignored": true}
                },
                "live": live,
            }
        })
    }

    fn shell_entry(event: &str, exit_code: Value, live: &[&str], tail_path: Option<&str>) -> Value {
        json!({
            "type": "custom",
            "customType": "choco-pi:host-bridge/shell",
            "data": {
                "schema": 1, "at": 2000, "event": event,
                "shell": {
                    "id": "sh-1", "name": null, "command": "cargo test", "cwd": "/w",
                    "pid": 9, "exitCode": exit_code, "startedAt": 1000,
                    "finishedAt": null, "toolCallId": "call-1", "tailPath": tail_path
                },
                "live": live,
            }
        })
    }

    fn items(events: &[DriverEvent]) -> Vec<(BackgroundWorkStatus, String)> {
        events
            .iter()
            .filter_map(|event| match event {
                DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item)) => {
                    Some((item.status, item.key.provider_id.clone()))
                }
                _ => None,
            })
            .collect()
    }

    fn reconciled(events: &[DriverEvent]) -> Option<Vec<String>> {
        events.iter().find_map(|event| match event {
            DriverEvent::BackgroundWork(BackgroundWorkEvent::ReconcileLive { items }) => Some(
                items
                    .iter()
                    .map(|item| item.key.provider_id.clone())
                    .collect(),
            ),
            _ => None,
        })
    }

    fn scratch_file(name: &str) -> PathBuf {
        let directory =
            std::env::temp_dir().join(format!("waku-chocopi-tail-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        directory.join(name)
    }

    fn append(path: &Path, bytes: &[u8]) {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }

    #[test]
    fn lost_subagents_and_shells_settle_as_lost() {
        let mut state = bridge();
        state.apply_entry(&subagent_entry("started", "running", &["agent-1"], None));
        let events = state.apply_entry(&at(subagent_entry("lost", "lost", &[], None), 2001));
        assert_eq!(
            items(&events),
            [(BackgroundWorkStatus::Lost, "agent-1".to_owned())]
        );
        assert_eq!(reconciled(&events), Some(Vec::new()));

        state.apply_entry(&shell_entry("started", Value::Null, &["sh-1"], None));
        let events = state.apply_entry(&at(shell_entry("lost", Value::Null, &[], None), 2001));
        assert_eq!(
            items(&events),
            [(BackgroundWorkStatus::Lost, "sh-1".to_owned())]
        );
        assert!(!state.has_live());
        // A shell that settles after it was reported lost still upserts.
        let events = state.apply_entry(&at(shell_entry("exited", json!(0), &[], None), 2002));
        assert_eq!(
            items(&events),
            [(BackgroundWorkStatus::Completed, "sh-1".to_owned())]
        );
    }

    fn at(mut entry: Value, at: u64) -> Value {
        entry["data"]["at"] = json!(at);
        entry
    }

    /// The agent's status decides; the event only names the edge.
    #[test]
    fn subagent_status_comes_from_the_agent_not_the_event() {
        let mut state = bridge();
        let cases = [
            ("created", "queued", BackgroundWorkStatus::Starting),
            ("steered", "running", BackgroundWorkStatus::Running),
            ("completed", "failed", BackgroundWorkStatus::Failed),
            ("resumed", "running", BackgroundWorkStatus::Running),
            ("stopped", "stopped", BackgroundWorkStatus::Stopped),
            ("lost", "lost", BackgroundWorkStatus::Lost),
        ];
        for (time, (event, status, expected)) in (3000..).zip(cases) {
            let events = state.apply_entry(&at(subagent_entry(event, status, &[], None), time));
            assert_eq!(
                items(&events),
                [(expected, "agent-1".to_owned())],
                "{event}/{status}"
            );
        }
        // A status this build does not know is not guessed from the event.
        let events = state.apply_entry(&at(subagent_entry("started", "paused", &[], None), 4000));
        assert!(events.is_empty(), "{events:?}");
    }

    /// choco-pi emits `started` before `created` during spawn; the later
    /// `created` still refines the item, and nothing older than what was
    /// applied moves settled work back to live.
    #[test]
    fn out_of_order_entries_never_regress_settled_work() {
        let mut state = bridge();
        let started = at(subagent_entry("started", "running", &["agent-1"], None), 10);
        let events = state.apply_entry(&started);
        assert_eq!(
            items(&events),
            [(BackgroundWorkStatus::Running, "agent-1".to_owned())]
        );
        let mut created = at(subagent_entry("created", "running", &["agent-1"], None), 12);
        created["data"]["agent"]["toolCallId"] = json!("call-9");
        let events = state.apply_entry(&created);
        let DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item)) = &events[0] else {
            panic!("expected an upsert");
        };
        assert_eq!(item.status, BackgroundWorkStatus::Running);
        assert_eq!(item.origin_activity_id.as_deref(), Some("call-9"));
        assert_eq!(reconciled(&events), Some(vec!["agent-1".to_owned()]));

        let completed = at(subagent_entry("completed", "completed", &[], None), 20);
        let events = state.apply_entry(&completed);
        assert_eq!(
            items(&events),
            [(BackgroundWorkStatus::Completed, "agent-1".to_owned())]
        );
        // A late live entry, older or from the same instant, changes nothing.
        for time in [15, 20] {
            let stale = at(
                subagent_entry("started", "running", &["agent-1"], None),
                time,
            );
            assert!(state.apply_entry(&stale).is_empty(), "at {time}");
        }
        assert!(!state.has_live());
        // A genuinely newer transition (a resumed agent) is applied.
        let resumed = at(subagent_entry("resumed", "running", &["agent-1"], None), 30);
        let events = state.apply_entry(&resumed);
        assert_eq!(
            items(&events),
            [(BackgroundWorkStatus::Running, "agent-1".to_owned())]
        );

        // Shells follow the same rule.
        state.apply_entry(&at(
            shell_entry("started", Value::Null, &["sh-1"], None),
            10,
        ));
        state.apply_entry(&at(shell_entry("exited", json!(1), &[], None), 20));
        let stale = at(shell_entry("started", Value::Null, &["sh-1"], None), 15);
        assert!(state.apply_entry(&stale).is_empty());
    }

    #[test]
    fn shell_exit_codes_decide_success() {
        let mut state = bridge();
        for (code, status) in [
            (json!(0), BackgroundWorkStatus::Completed),
            (json!(1), BackgroundWorkStatus::Failed),
            (Value::Null, BackgroundWorkStatus::Failed),
        ] {
            let events = state.apply_entry(&shell_entry("exited", code.clone(), &[], None));
            let DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item)) = &events[0] else {
                panic!("expected an upsert");
            };
            assert_eq!(item.status, status, "exit code {code}");
            assert_eq!(item.exit_code, code.as_i64().map(|code| code as i32));
            assert_eq!(item.title, "cargo test");
            assert_eq!(item.origin_activity_id.as_deref(), Some("call-1"));
        }
        let events = state.apply_entry(&shell_entry("stopped", Value::Null, &[], None));
        assert_eq!(items(&events)[0].0, BackgroundWorkStatus::Stopped);
    }

    #[test]
    fn subagent_metadata_maps_onto_the_item() {
        let mut state = bridge();
        let events = state.apply_entry(&subagent_entry("created", "queued", &["agent-1"], None));
        let DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item)) = &events[0] else {
            panic!("expected an upsert");
        };
        assert_eq!(item.status, BackgroundWorkStatus::Starting);
        assert_eq!(item.parent_id.as_deref(), Some("agent-0"));
        assert_eq!(item.model, None);
        assert_eq!(item.title, "Survey");
        assert_eq!(item.started_at_ms, 1000);
        assert_eq!(item.duration_ms, Some(500));
        // A resumed agent can still be queued.
        let events = state.apply_entry(&subagent_entry("resumed", "queued", &["agent-1"], None));
        assert_eq!(items(&events)[0].0, BackgroundWorkStatus::Starting);
    }

    #[test]
    fn unknown_events_kinds_and_schemas_are_ignored() {
        let mut state = bridge();
        assert!(
            state
                .apply_entry(&subagent_entry("teleported", "running", &[], None))
                .is_empty()
        );
        let mut workflow = subagent_entry("created", "running", &[], None);
        workflow["customType"] = json!("choco-pi:host-bridge/workflow");
        assert!(state.apply_entry(&workflow).is_empty());
        let mut newer = subagent_entry("started", "running", &["agent-1"], None);
        newer["data"]["schema"] = json!(2);
        assert!(state.apply_entry(&newer).is_empty());
        let mut unversioned = subagent_entry("started", "running", &["agent-1"], None);
        unversioned["data"]
            .as_object_mut()
            .unwrap()
            .remove("schema");
        assert!(state.apply_entry(&unversioned).is_empty());
        let mut foreign = subagent_entry("started", "running", &["agent-1"], None);
        foreign["customType"] = json!("subagents:record");
        assert!(!state.owns_entry(&foreign));
        assert!(state.apply_status("not json").is_empty());
        assert!(
            state
                .apply_status(
                    &json!({"schema": 2, "at": 1, "subagents": [], "shells": []}).to_string()
                )
                .is_empty()
        );
        assert!(!state.has_live());
    }

    #[test]
    fn goal_entries_report_the_goal_or_its_absence() {
        let mut state = bridge();
        let goal = |goal: Value| {
            json!({"type": "custom", "customType": "choco-pi:host-bridge/goal",
                "data": {"schema": 1, "at": 1, "goal": goal}})
        };
        let events = state.apply_entry(&goal(json!({
            "objective": "Finish", "status": "budgetLimited", "tokenBudget": null,
            "tokensUsed": 10, "activeSeconds": 3
        })));
        assert!(matches!(
            events.as_slice(),
            [DriverEvent::GoalUpdated(Some(ThreadGoal {
                status: ThreadGoalStatus::BudgetLimited,
                token_budget: None,
                tokens_used: 10,
                time_used_seconds: 3,
                ..
            }))]
        ));
        for (wire, status) in [
            ("active", ThreadGoalStatus::Active),
            ("paused", ThreadGoalStatus::Paused),
            ("complete", ThreadGoalStatus::Complete),
        ] {
            let events = state.apply_entry(&goal(json!({
                "objective": "x", "status": wire, "tokenBudget": 5,
                "tokensUsed": 0, "activeSeconds": 0
            })));
            assert!(
                matches!(events.as_slice(), [DriverEvent::GoalUpdated(Some(goal))] if goal.status == status)
            );
        }
        assert!(matches!(
            state.apply_entry(&goal(Value::Null)).as_slice(),
            [DriverEvent::GoalUpdated(None)]
        ));
        assert!(
            state
                .apply_entry(&goal(json!({"objective": "x", "status": "blocked"})))
                .is_empty()
        );
    }

    #[test]
    fn subagent_transcripts_tail_incrementally() {
        let path = scratch_file("agent-1.output");
        append(
            &path,
            concat!(
                r#"{"type":"user","message":{"role":"user","content":"List the directory"}}"#,
                "\n",
                r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"toolCall","id":"t1","name":"ls","arguments":{"path":"."}}]}}"#,
                "\n",
                r#"{"type":"toolResult","message":{"role":"toolResult","content":[{"type":"text","text":"(empty)"}]}}"#,
                "\n",
                r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"The dir"#,
            )
            .as_bytes(),
        );
        let mut state = bridge();
        let file = path.to_str().unwrap();
        let events = state.apply_entry(&subagent_entry(
            "started",
            "running",
            &["agent-1"],
            Some(file),
        ));
        let deltas = |events: &[DriverEvent]| {
            events
                .iter()
                .filter_map(|event| match event {
                    DriverEvent::BackgroundWork(BackgroundWorkEvent::OutputDelta {
                        delta, ..
                    }) => Some(delta.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        // The item is established before its output.
        assert!(matches!(
            events.as_slice(),
            [
                DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(_)),
                DriverEvent::BackgroundWork(BackgroundWorkEvent::OutputDelta { .. }),
                DriverEvent::BackgroundWork(BackgroundWorkEvent::ReconcileLive { .. }),
            ]
        ));
        assert_eq!(deltas(&events), ["› ls · .\n"]);

        append(&path, "ectory is empty.\"}]}}\n".as_bytes());
        let frame = json!({"schema": 1, "at": 3000, "shells": [], "subagents": [
            {"id": "agent-1", "status": "running", "toolUses": 1, "tokens": 50,
             "turnCount": 1, "maxTurns": 2, "durationMs": 2000, "activity": null}
        ]})
        .to_string();
        let events = state.apply_status(&frame);
        assert_eq!(deltas(&events), ["The directory is empty.\n\n"]);
        let DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item)) = &events[0] else {
            panic!("expected the progress upsert first");
        };
        assert_eq!(item.detail.as_deref(), Some("1 tool uses · 50 tokens"));
        // Nothing new, nothing emitted.
        assert_eq!(deltas(&state.apply_status(&frame)), Vec::<String>::new());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn oversized_output_is_skipped_and_marked_truncated() {
        let path = scratch_file("shell.log");
        let line = "é".repeat(100) + "\n";
        let mut body = String::new();
        while body.len() <= MAX_TAIL_READ_BYTES as usize + 4096 {
            body.push_str(&line);
        }
        append(&path, body.as_bytes());
        let mut state = bridge();
        let events = state.apply_entry(&shell_entry(
            "started",
            Value::Null,
            &["sh-1"],
            Some(path.to_str().unwrap()),
        ));
        let truncated = events
            .iter()
            .find_map(|event| match event {
                DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(item))
                    if item.output_truncated =>
                {
                    Some(item.output.clone().unwrap())
                }
                _ => None,
            })
            .expect("the skipped prefix should mark the output truncated");
        assert!(truncated.len() <= MAX_TAIL_READ_BYTES as usize);
        assert!(!truncated.starts_with('\u{fffd}'), "no split character");
        assert!(truncated.ends_with("é\n"));

        append(&path, b"done\n");
        let events = state.apply_status(
            &json!({"schema": 1, "at": 1, "subagents": [],
                "shells": [{"id": "sh-1", "status": "running", "bytes": 5}]})
            .to_string(),
        );
        assert!(matches!(
            events.as_slice(),
            [DriverEvent::BackgroundWork(BackgroundWorkEvent::OutputDelta { delta, .. })] if delta == "done\n"
        ));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_missing_output_file_is_not_an_error() {
        let mut state = bridge();
        let events = state.apply_entry(&subagent_entry(
            "started",
            "running",
            &["agent-1"],
            Some("/nonexistent/waku/agent.output"),
        ));
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn snapshots_collect_until_the_response() {
        let mut state = bridge();
        state.apply_entry(&shell_entry("started", Value::Null, &["sh-1"], None));
        state.begin_snapshot("waku-9".to_owned());
        let events = state.apply_entry(&subagent_entry("started", "running", &["agent-1"], None));
        assert_eq!(
            reconciled(&events),
            None,
            "no per-entry reconcile mid-snapshot"
        );
        let events = state.finish_snapshot();
        // The shell was not re-announced, so it is gone.
        assert_eq!(reconciled(&events), Some(vec!["agent-1".to_owned()]));
        assert_eq!(state.snapshot_request(), None);

        // A cancelled snapshot keeps kinds it never heard about.
        state.apply_entry(&at(
            shell_entry("started", Value::Null, &["sh-1"], None),
            2001,
        ));
        state.begin_snapshot("waku-10".to_owned());
        state.apply_entry(&subagent_entry("completed", "completed", &[], None));
        state.cancel_snapshot();
        let events = state.apply_entry(&at(
            shell_entry("started", Value::Null, &["sh-1"], None),
            2002,
        ));
        assert_eq!(reconciled(&events), Some(vec!["sh-1".to_owned()]));
    }
}
