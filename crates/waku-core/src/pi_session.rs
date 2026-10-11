//! Pi and Oh My Pi native JSONL session discovery and transcript import.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, anyhow, bail};
use serde_json::Value;
use uuid::Uuid;

use crate::model::{
    AgentTurn, Message, MessageRole, ProviderKind, ProviderResumeCursor, ProviderSessionHistory,
    ProviderSessionSummary, TurnStatus,
};

#[derive(Debug)]
struct NativeEntry {
    id: String,
    parent_id: Option<String>,
    kind: String,
    role: Option<String>,
    text: Option<String>,
    stop_reason: Option<String>,
    timestamp: u64,
}

#[derive(Debug)]
struct ParsedSession {
    session_id: String,
    cwd: PathBuf,
    title: Option<String>,
    created_at: u64,
    updated_at: u64,
    entries: Vec<NativeEntry>,
    /// A choco-pi subagent transcript; hidden from the resume picker.
    is_subagent: bool,
}

/// choco-pi writes this custom entry when it spawns a subagent session.
const CHOCO_PI_SUBAGENT_CUSTOM_TYPE: &str = "choco-pi:subagent";

/// Heuristic backfill for subagent sessions written before choco-pi recorded
/// [`CHOCO_PI_SUBAGENT_CUSTOM_TYPE`]: it mirrors choco-pi's `agent-runner.ts`
/// naming `${name}#${agentId.slice(0, 8)}`, i.e. `^[A-Za-z0-9_.-]+#[0-9a-f]{8}$`.
/// A user could pick such a name by hand, so this is a heuristic, not proof.
fn is_subagent_session_name(name: &str) -> bool {
    let Some((agent, id)) = name.split_once('#') else {
        return false;
    };
    !agent.is_empty()
        && agent
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        && id.len() == 8
        && id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn text_content(value: &Value) -> Option<String> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n\n"),
        _ => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}

fn seconds(value: &Value) -> u64 {
    value
        .as_u64()
        .map(|value| {
            if value > 100_000_000_000 {
                value / 1000
            } else {
                value
            }
        })
        .or_else(|| {
            value
                .as_i64()
                .and_then(|value| u64::try_from(value).ok())
                .map(|value| {
                    if value > 100_000_000_000 {
                        value / 1000
                    } else {
                        value
                    }
                })
        })
        .or_else(|| {
            value
                .as_str()
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .and_then(|value| u64::try_from(value.timestamp()).ok())
        })
        .unwrap_or_default()
}

fn file_timestamp(path: &Path) -> u64 {
    path.metadata()
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|value| value.as_secs())
        .unwrap_or_default()
}

fn parse_session(path: &Path) -> anyhow::Result<ParsedSession> {
    let file = fs::File::open(path)
        .with_context(|| format!("could not open native session {}", path.display()))?;
    let mut session_id = None;
    let mut cwd = None;
    let mut title = None;
    let mut created_at = 0;
    let mut updated_at = file_timestamp(path);
    let mut entries = Vec::new();
    let mut has_subagent_marker = false;
    let mut last_session_info_name: Option<String> = None;

    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if kind == "custom"
            && value.get("customType").and_then(Value::as_str)
                == Some(CHOCO_PI_SUBAGENT_CUSTOM_TYPE)
        {
            has_subagent_marker = true;
        }
        if kind == "session_info" {
            last_session_info_name = value
                .get("name")
                .and_then(Value::as_str)
                .map(|name| name.trim().to_owned());
        }
        if kind == "title" {
            if let Some(value) = value
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                title = Some(value.to_owned());
            }
            updated_at = updated_at.max(seconds(value.get("updatedAt").unwrap_or(&Value::Null)));
            continue;
        }
        if kind == "session" {
            session_id = value
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned);
            cwd = value.get("cwd").and_then(Value::as_str).map(PathBuf::from);
            created_at = seconds(value.get("timestamp").unwrap_or(&Value::Null));
            updated_at = updated_at.max(created_at);
            if title.is_none() {
                title = value
                    .get("title")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
            }
            continue;
        }
        if matches!(kind, "session_info" | "title_change") {
            let key = if kind == "session_info" {
                "name"
            } else {
                "title"
            };
            title = value
                .get(key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .or(title);
        }

        let Some(id) = value
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        let entry_timestamp = seconds(value.get("timestamp").unwrap_or(&Value::Null));
        let message_timestamp =
            seconds(value.pointer("/message/timestamp").unwrap_or(&Value::Null));
        let timestamp = message_timestamp.max(entry_timestamp);
        updated_at = updated_at.max(timestamp);
        entries.push(NativeEntry {
            id: id.to_owned(),
            parent_id: value
                .get("parentId")
                .and_then(Value::as_str)
                .map(str::to_owned),
            kind: kind.to_owned(),
            role: value
                .pointer("/message/role")
                .and_then(Value::as_str)
                .map(str::to_owned),
            text: value.pointer("/message/content").and_then(text_content),
            stop_reason: value
                .pointer("/message/stopReason")
                .and_then(Value::as_str)
                .map(str::to_owned),
            timestamp,
        });
    }

    let session_id = session_id.ok_or_else(|| anyhow!("native session has no header ID"))?;
    let cwd = cwd.ok_or_else(|| anyhow!("native session has no working directory"))?;
    if !cwd.is_absolute() {
        bail!("native session working directory is not absolute");
    }
    let created_at = if created_at == 0 {
        updated_at
    } else {
        created_at
    };
    let is_subagent = has_subagent_marker
        || last_session_info_name
            .as_deref()
            .is_some_and(is_subagent_session_name);
    Ok(ParsedSession {
        session_id,
        cwd,
        title,
        created_at,
        updated_at: updated_at.max(created_at),
        entries,
        is_subagent,
    })
}

fn active_chain(session: &ParsedSession, provider: ProviderKind) -> Vec<&NativeEntry> {
    let by_id = session
        .entries
        .iter()
        .map(|entry| (entry.id.as_str(), entry))
        .collect::<HashMap<_, _>>();
    let Some(mut current) = session.entries.last() else {
        return Vec::new();
    };
    let mut chain = Vec::new();
    loop {
        chain.push(current);
        let Some(parent) = current.parent_id.as_deref() else {
            break;
        };
        let Some(next) = by_id.get(parent).copied() else {
            break;
        };
        current = next;
    }
    chain.reverse();
    if provider == ProviderKind::OhMyPi
        && let Some(boundary) = chain
            .iter()
            .rposition(|entry| entry.kind == "reset_boundary")
    {
        chain.drain(..=boundary);
    }
    chain
}

fn title_from_prompt(prompt: &str) -> String {
    let mut title = prompt
        .split_whitespace()
        .take(8)
        .collect::<Vec<_>>()
        .join(" ");
    if title.chars().count() > 58 {
        title = format!("{}…", title.chars().take(57).collect::<String>());
    }
    title
}

fn summary_from_session(
    provider: ProviderKind,
    path: &Path,
    session: &ParsedSession,
) -> Option<ProviderSessionSummary> {
    let first_prompt = active_chain(session, provider)
        .into_iter()
        .find(|entry| entry.kind == "message" && entry.role.as_deref() == Some("user"))
        .and_then(|entry| entry.text.as_deref())
        .map(title_from_prompt)
        .filter(|title| !title.is_empty());
    let title = session
        .title
        .clone()
        .filter(|title| !title.trim().is_empty())
        .or(first_prompt)?;
    let cursor = match provider {
        ProviderKind::Pi => ProviderResumeCursor::Pi {
            session_id: session.session_id.clone(),
            session_file: Some(path.to_path_buf()),
        },
        ProviderKind::ChocoPi => ProviderResumeCursor::ChocoPi {
            session_id: session.session_id.clone(),
            session_file: Some(path.to_path_buf()),
        },
        ProviderKind::OhMyPi => ProviderResumeCursor::OhMyPi {
            session_id: session.session_id.clone(),
            session_file: Some(path.to_path_buf()),
        },
        _ => return None,
    };
    Some(ProviderSessionSummary {
        cursor,
        title,
        cwd: session.cwd.clone(),
        created_at: session.created_at,
        updated_at: session.updated_at,
    })
}

fn expand_home(home: &Path, path: PathBuf) -> PathBuf {
    if path == Path::new("~") {
        return home.to_path_buf();
    }
    path.strip_prefix("~")
        .ok()
        .map(|suffix| home.join(suffix))
        .unwrap_or(path)
}

fn configured_agent_dir(home: &Path) -> Option<PathBuf> {
    std::env::var_os("PI_CODING_AGENT_DIR")
        .filter(|value| !value.is_empty())
        .map(|value| expand_home(home, PathBuf::from(value)))
}

/// Pi's agent directory: `PI_CODING_AGENT_DIR` or `~/.pi/agent`. choco-pi's
/// installer follows the same rule when it writes its profile marker.
pub(crate) fn pi_agent_dir() -> Option<PathBuf> {
    let home = dirs::home_dir()?;
    Some(configured_agent_dir(&home).unwrap_or_else(|| home.join(".pi/agent")))
}

fn session_roots(provider: ProviderKind) -> anyhow::Result<Vec<PathBuf>> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("home directory could not be located"))?;
    let expand_home = |path: PathBuf| expand_home(&home, path);
    let configured_agent = configured_agent_dir(&home);
    match provider {
        ProviderKind::Pi | ProviderKind::ChocoPi => {
            if let Some(path) =
                std::env::var_os("PI_CODING_AGENT_SESSION_DIR").filter(|value| !value.is_empty())
            {
                return Ok(vec![expand_home(PathBuf::from(path))]);
            }
            let agent = configured_agent.unwrap_or_else(|| home.join(".pi/agent"));
            let default = agent.join("sessions");
            let settings = agent.join("settings.json");
            let configured = fs::read(&settings)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|value| value.get("sessionDir")?.as_str().map(PathBuf::from))
                .map(&expand_home)
                .filter(|path| path.is_absolute());
            Ok(configured.into_iter().chain([default]).collect())
        }
        ProviderKind::OhMyPi => {
            let mut roots = vec![
                configured_agent
                    .unwrap_or_else(|| home.join(".omp/agent"))
                    .join("sessions"),
            ];
            let profiles = home.join(".omp/profiles");
            if let Ok(entries) = fs::read_dir(profiles) {
                roots.extend(
                    entries
                        .flatten()
                        .map(|entry| entry.path().join("agent/sessions")),
                );
            }
            Ok(roots)
        }
        _ => bail!("{} is not a Pi session provider", provider.display_name()),
    }
}

fn session_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return files;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
            files.push(path);
            continue;
        }
        if !path.is_dir() {
            continue;
        }
        if let Ok(children) = fs::read_dir(path) {
            files.extend(
                children.flatten().map(|entry| entry.path()).filter(|path| {
                    path.extension().and_then(|value| value.to_str()) == Some("jsonl")
                }),
            );
        }
    }
    files
}

pub fn list_provider_sessions(
    provider: ProviderKind,
    limit: usize,
) -> anyhow::Result<Vec<ProviderSessionSummary>> {
    Ok(list_provider_sessions_in(
        provider,
        &session_roots(provider)?,
        limit,
    ))
}

fn list_provider_sessions_in(
    provider: ProviderKind,
    roots: &[PathBuf],
    limit: usize,
) -> Vec<ProviderSessionSummary> {
    let mut seen = HashSet::new();
    let mut sessions = Vec::new();
    for root in roots {
        for path in session_files(root) {
            let Ok(parsed) = parse_session(&path) else {
                continue;
            };
            if !seen.insert(parsed.session_id.clone()) {
                continue;
            }
            // Subagent transcripts stay loadable by file but are not resumable
            // user conversations.
            if parsed.is_subagent {
                continue;
            }
            if let Some(summary) = summary_from_session(provider, &path, &parsed) {
                sessions.push(summary);
            }
        }
    }
    sessions.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    sessions.truncate(limit);
    sessions
}

fn status_from_reason(reason: Option<&str>) -> TurnStatus {
    match reason {
        Some("error") => TurnStatus::Failed,
        Some("aborted" | "cancelled" | "length") => TurnStatus::Interrupted,
        _ => TurnStatus::Completed,
    }
}

fn history_from_session(provider: ProviderKind, session: &ParsedSession) -> ProviderSessionHistory {
    let mut history = ProviderSessionHistory::default();
    for entry in active_chain(session, provider) {
        if entry.kind == "message" && entry.role.as_deref() == Some("user") {
            let turn_id = Uuid::new_v4();
            history.turns.push(AgentTurn {
                id: turn_id,
                turn_count: history.turns.len() + 1,
                status: TurnStatus::Interrupted,
                provider_turn_started: true,
                provider_resume_at: None,
                started_at: entry.timestamp,
                completed_at: None,
                checkpoint: None,
            });
            if let Some(text) = entry.text.as_deref() {
                let mut message = Message::new_for_turn(MessageRole::User, text, turn_id);
                message.created_at = entry.timestamp;
                history.messages.push(message);
            }
            continue;
        }
        if entry.kind != "message" || entry.role.as_deref() != Some("assistant") {
            continue;
        }
        let Some(turn) = history.turns.last_mut() else {
            continue;
        };
        turn.status = status_from_reason(entry.stop_reason.as_deref());
        turn.completed_at = Some(entry.timestamp.max(turn.started_at));
        let Some(text) = entry.text.as_deref() else {
            continue;
        };
        if let Some(previous) = history.messages.last_mut().filter(|message| {
            message.role == MessageRole::Assistant && message.turn_id == Some(turn.id)
        }) {
            if !previous.content.is_empty() {
                previous.content.push_str("\n\n");
            }
            previous.content.push_str(text);
            previous.created_at = previous.created_at.max(entry.timestamp);
        } else {
            let mut message = Message::new_for_turn(MessageRole::Assistant, text, turn.id);
            message.created_at = entry.timestamp;
            history.messages.push(message);
        }
    }
    history
}

pub fn provider_session_history(
    provider: ProviderKind,
    session_id: &str,
    session_file: &Path,
    visible_turn_limit: usize,
) -> anyhow::Result<ProviderSessionHistory> {
    let session = parse_session(session_file)?;
    if session.session_id != session_id {
        bail!(
            "{} session file belongs to a different session",
            provider.display_name()
        );
    }
    let mut history = history_from_session(provider, &session);
    let retained = history
        .turns
        .iter()
        .rev()
        .take(visible_turn_limit)
        .map(|turn| turn.id)
        .collect::<HashSet<_>>();
    history
        .messages
        .retain(|message| message.turn_id.is_some_and(|id| retained.contains(&id)));
    Ok(history)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_session(lines: &[&str]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("waku-pi-session-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();
        path
    }

    #[test]
    fn imports_the_active_pi_branch_without_reasoning_or_tools() {
        let header = serde_json::json!({
            "type": "session",
            "version": 3,
            "id": "session-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "cwd": std::env::temp_dir(),
        })
        .to_string();
        let path = write_session(&[
            &header,
            r#"{"type":"message","id":"u1","parentId":null,"timestamp":"2026-01-01T00:00:01Z","message":{"role":"user","content":"one","timestamp":1767225601000}}"#,
            r#"{"type":"message","id":"a1","parentId":"u1","timestamp":"2026-01-01T00:00:02Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"private"},{"type":"text","text":"answer"}],"stopReason":"stop"}}"#,
            r#"{"type":"message","id":"old","parentId":"u1","timestamp":"2026-01-01T00:00:03Z","message":{"role":"user","content":"abandoned"}}"#,
            r#"{"type":"message","id":"u2","parentId":"a1","timestamp":"2026-01-01T00:00:04Z","message":{"role":"user","content":"two"}}"#,
        ]);
        let session = parse_session(&path).unwrap();
        let history = history_from_session(ProviderKind::Pi, &session);
        assert_eq!(history.turns.len(), 2);
        assert_eq!(history.messages.len(), 3);
        assert_eq!(history.messages[1].content, "answer");
        assert!(
            history
                .messages
                .iter()
                .all(|message| message.content != "abandoned")
        );
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn chocopi_host_bridge_tail_preserves_active_messages_and_history() {
        let header = serde_json::json!({"type":"session","version":3,"id":"bridge-session","cwd":std::env::temp_dir()}).to_string();
        let user = r#"{"type":"message","id":"u1","parentId":null,"message":{"role":"user","content":"question"}}"#;
        let answer = r#"{"type":"message","id":"a1","parentId":"u1","message":{"role":"assistant","content":"answer","stopReason":"stop"}}"#;
        let abandoned = r#"{"type":"message","id":"other","parentId":"u1","message":{"role":"assistant","content":"abandoned"}}"#;
        let path = write_session(&[&header, user, abandoned, answer]);
        let baseline = parse_session(&path).unwrap();
        let history = history_from_session(ProviderKind::ChocoPi, &baseline);
        let bridge = r#"{"type":"custom","id":"bridge1","parentId":"a1","customType":"choco-pi:host-bridge/subagent","data":{"schema":1,"event":"completed","live":[]}}"#;
        let bridge2 = r#"{"type":"custom","id":"bridge2","parentId":"bridge1","customType":"choco-pi:host-bridge/subagent","data":{"schema":1,"event":"lost","live":[]}}"#;
        fs::write(
            &path,
            [&header, user, abandoned, answer, bridge, bridge2].join("\n"),
        )
        .unwrap();
        let session = parse_session(&path).unwrap();
        let chain = active_chain(&session, ProviderKind::ChocoPi);
        assert_eq!(
            chain
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["u1", "a1", "bridge1", "bridge2"]
        );
        assert_eq!(
            chain
                .iter()
                .filter(|entry| entry.kind == "message")
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            active_chain(&baseline, ProviderKind::ChocoPi)
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>()
        );
        let after = history_from_session(ProviderKind::ChocoPi, &session);
        assert_eq!(
            after
                .messages
                .iter()
                .map(|message| (&message.role, &message.content, message.created_at))
                .collect::<Vec<_>>(),
            history
                .messages
                .iter()
                .map(|message| (&message.role, &message.content, message.created_at))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            after
                .turns
                .iter()
                .map(|turn| (
                    turn.turn_count,
                    &turn.status,
                    turn.started_at,
                    turn.completed_at
                ))
                .collect::<Vec<_>>(),
            history
                .turns
                .iter()
                .map(|turn| (
                    turn.turn_count,
                    &turn.status,
                    turn.started_at,
                    turn.completed_at
                ))
                .collect::<Vec<_>>()
        );
        let summary = summary_from_session(ProviderKind::ChocoPi, &path, &session).unwrap();
        assert!(
            matches!(summary.cursor, ProviderResumeCursor::ChocoPi { session_id, session_file: Some(file) } if session_id == "bridge-session" && file == path)
        );
        assert_eq!(
            session_roots(ProviderKind::ChocoPi).unwrap(),
            session_roots(ProviderKind::Pi).unwrap()
        );
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn oh_my_pi_reset_hides_the_earlier_transcript_and_uses_the_title_slot() {
        let header = serde_json::json!({
            "type": "session",
            "version": 3,
            "id": "session-2",
            "timestamp": "2026-01-01T00:00:00Z",
            "cwd": std::env::temp_dir(),
        })
        .to_string();
        let path = write_session(&[
            r#"{"type":"title","v":1,"title":"Current title","updatedAt":"2026-01-01T00:00:05Z","pad":""}"#,
            &header,
            r#"{"type":"message","id":"u1","parentId":null,"timestamp":"2026-01-01T00:00:01Z","message":{"role":"user","content":"old"}}"#,
            r#"{"type":"reset_boundary","id":"r1","parentId":"u1","timestamp":"2026-01-01T00:00:02Z"}"#,
            r#"{"type":"message","id":"u2","parentId":"r1","timestamp":"2026-01-01T00:00:03Z","message":{"role":"user","content":"new"}}"#,
        ]);
        let session = parse_session(&path).unwrap();
        let summary = summary_from_session(ProviderKind::OhMyPi, &path, &session).unwrap();
        let history = history_from_session(ProviderKind::OhMyPi, &session);
        assert_eq!(summary.title, "Current title");
        assert_eq!(history.turns.len(), 1);
        assert_eq!(history.messages[0].content, "new");
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    fn write_root_session(root: &Path, id: &str, extra: &[&str], parent: Option<&str>) {
        let mut header = serde_json::json!({
            "type": "session",
            "version": 3,
            "id": id,
            "timestamp": "2026-01-01T00:00:00Z",
            "cwd": std::env::temp_dir(),
        });
        if let Some(parent) = parent {
            header["parentSession"] = Value::from(parent);
        }
        let user = r#"{"type":"message","id":"u1","parentId":null,"timestamp":"2026-01-01T00:00:01Z","message":{"role":"user","content":"do the thing"}}"#;
        let header = header.to_string();
        let lines = [header.as_str(), user]
            .into_iter()
            .chain(extra.iter().copied())
            .collect::<Vec<_>>();
        fs::write(
            root.join(format!("{id}.jsonl")),
            format!("{}\n", lines.join("\n")),
        )
        .unwrap();
    }

    /// Lists a root holding a plain control session plus one session under test.
    fn list_with(id: &str, extra: &[&str], parent: Option<&str>) -> Vec<ProviderSessionSummary> {
        let root = std::env::temp_dir().join(format!("waku-pi-root-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        write_root_session(&root, "control", &[], None);
        write_root_session(&root, id, extra, parent);
        let sessions =
            list_provider_sessions_in(ProviderKind::ChocoPi, std::slice::from_ref(&root), 50);
        fs::remove_dir_all(&root).unwrap();
        sessions
    }

    fn listed_ids(sessions: &[ProviderSessionSummary]) -> Vec<&str> {
        let mut ids = sessions
            .iter()
            .map(|summary| match &summary.cursor {
                ProviderResumeCursor::ChocoPi { session_id, .. } => session_id.as_str(),
                _ => panic!("unexpected cursor"),
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn subagent_marker_hides_the_session_but_history_still_loads() {
        let marker = r#"{"type":"custom","id":"m1","parentId":"u1","customType":"choco-pi:subagent","data":{"schemaVersion":1,"agentId":"620b15e5-0000","agentType":"implementer","parentSessionId":"p"}}"#;
        let sessions = list_with("sub", &[marker], Some("/tmp/parent.jsonl"));
        assert_eq!(listed_ids(&sessions), ["control"]);

        let path = write_session(&[
            &serde_json::json!({"type":"session","version":3,"id":"sub","cwd":std::env::temp_dir()})
                .to_string(),
            r#"{"type":"message","id":"u1","parentId":null,"message":{"role":"user","content":"task"}}"#,
            marker,
        ]);
        assert!(parse_session(&path).unwrap().is_subagent);
        let history = provider_session_history(ProviderKind::ChocoPi, "sub", &path, 10).unwrap();
        assert_eq!(history.messages[0].content, "task");
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn legacy_subagent_name_hides_the_session() {
        let name = r#"{"type":"session_info","id":"i1","parentId":"u1","timestamp":"2026-01-01T00:00:02Z","name":"implementer#620b15e5"}"#;
        let sessions = list_with("sub", &[name], Some("/tmp/parent.jsonl"));
        assert_eq!(listed_ids(&sessions), ["control"]);
    }

    #[test]
    fn later_user_rename_overrides_a_subagent_like_name() {
        let first = r#"{"type":"session_info","id":"i1","parentId":"u1","timestamp":"2026-01-01T00:00:02Z","name":"implementer#620b15e5"}"#;
        let second = r#"{"type":"session_info","id":"i2","parentId":"i1","timestamp":"2026-01-01T00:00:03Z","name":"My task"}"#;
        let sessions = list_with("renamed", &[first, second], None);
        assert_eq!(listed_ids(&sessions), ["control", "renamed"]);
        assert!(sessions.iter().any(|summary| summary.title == "My task"));
    }

    #[test]
    fn branched_user_session_with_parent_stays_listed() {
        let name = r#"{"type":"session_info","id":"i1","parentId":"u1","timestamp":"2026-01-01T00:00:02Z","name":"Refactor parser"}"#;
        let sessions = list_with("branch", &[name], Some("/tmp/parent.jsonl"));
        assert_eq!(listed_ids(&sessions), ["branch", "control"]);
    }

    #[test]
    fn subagent_name_heuristic_is_strict() {
        assert!(is_subagent_session_name("implementer#620b15e5"));
        assert!(is_subagent_session_name("my_agent.v2-x#d0f0a123"));
        assert!(!is_subagent_session_name("implementer#620B15E5"));
        assert!(!is_subagent_session_name("implementer#620b15e"));
        assert!(!is_subagent_session_name("#620b15e5"));
        assert!(!is_subagent_session_name("fix bug#620b15e5"));
        assert!(!is_subagent_session_name("a#b#620b15e5"));
    }
}
