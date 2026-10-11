//! The choco-pi profile marker that `install-profile.ts` writes under Pi's
//! agent directory. Its presence is what tells a choco-pi install apart from
//! plain Pi, since both launch a `pi` binary.
//!
//! Read only when probing or starting a session, never on a frame path.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The marker schema this build understands.
pub const PROFILE_SCHEMA: u64 = 1;
/// The newest host-bridge schema this build can parse. A newer marker keeps
/// the provider installed but turns the bridge off, so the session degrades
/// to Pi-level behavior instead of misreading entries.
pub const HOST_BRIDGE_SCHEMA: u64 = 1;
pub const DEFAULT_ENTRY_PREFIX: &str = "choco-pi:host-bridge";
pub const DEFAULT_STATUS_KEY: &str = "choco-pi.host-bridge";
/// The `setStatus` key choco-pi publishes structured `/status` and
/// `/context` reports under, when the marker does not name one.
pub const DEFAULT_REPORT_STATUS_KEY: &str = "choco-pi.report";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChocoPiProfile {
    pub schema: u64,
    pub checkout: PathBuf,
    pub revision: Option<String>,
    /// The Pi build choco-pi was installed against. A machine can carry
    /// another Pi on `PATH`, so this wins over `find_executable("pi")`.
    pub pi_launcher: Option<PathBuf>,
    pub pi_version: Option<String>,
    pub host_bridge: Option<HostBridgeSpec>,
    /// `hostBridge.reportStatusKey`. Reports carry their own schema, so the
    /// key holds even when the bridge itself is too new for this build.
    pub report_status_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostBridgeSpec {
    pub schema: u64,
    pub entry_prefix: String,
    pub status_key: String,
}

impl HostBridgeSpec {
    pub fn supported(&self) -> bool {
        self.schema >= 1 && self.schema <= HOST_BRIDGE_SCHEMA
    }
}

impl Default for HostBridgeSpec {
    fn default() -> Self {
        Self {
            schema: HOST_BRIDGE_SCHEMA,
            entry_prefix: DEFAULT_ENTRY_PREFIX.to_owned(),
            status_key: DEFAULT_STATUS_KEY.to_owned(),
        }
    }
}

impl ChocoPiProfile {
    /// The bridge to use, or `None` when the marker's bridge is newer than
    /// this build and the session should behave like plain Pi.
    pub fn supported_host_bridge(&self) -> Option<&HostBridgeSpec> {
        self.host_bridge.as_ref().filter(|spec| spec.supported())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawProfile {
    schema: Option<u64>,
    checkout: Option<PathBuf>,
    revision: Option<String>,
    pi: Option<RawPi>,
    host_bridge: Option<RawHostBridge>,
}

#[derive(Deserialize)]
struct RawPi {
    version: Option<String>,
    launcher: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawHostBridge {
    schema: Option<u64>,
    entry_prefix: Option<String>,
    status_key: Option<String>,
    report_status_key: Option<String>,
}

pub fn profile_path(agent_dir: &Path) -> PathBuf {
    agent_dir.join("choco-pi").join("profile.json")
}

/// Resolve ChocoPi's launcher at a probe/start boundary, never on a frame path.
pub fn resolve_launcher(
    binary_override: Option<&str>,
    profile: Option<&ChocoPiProfile>,
) -> Option<PathBuf> {
    resolve_launcher_with(binary_override, profile, || {
        crate::command_env::find_executable("pi")
    })
}

pub(crate) fn resolve_launcher_with(
    binary_override: Option<&str>,
    profile: Option<&ChocoPiProfile>,
    fallback: impl FnOnce() -> Option<PathBuf>,
) -> Option<PathBuf> {
    if let Some(binary) = binary_override {
        return crate::command_env::resolve_binary_override(binary);
    }
    if let Some(launcher) = profile.and_then(|profile| profile.pi_launcher.as_ref())
        && executable_file(launcher)
    {
        return Some(launcher.clone());
    }
    fallback()
}

fn executable_file(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Checkout resources are scanned only on background discovery paths.
pub(crate) fn checkout_skill_roots(profile: &ChocoPiProfile) -> Vec<PathBuf> {
    let mut roots = vec![profile.checkout.join(".pi/skills")];
    // Pi packages auto-discover their skills directories; choco-pi's settings
    // load the packages under .pi/packages (including MCP and LSP skills).
    if let Ok(packages) = std::fs::read_dir(profile.checkout.join(".pi/packages")) {
        let mut package_roots = packages
            .filter_map(Result::ok)
            .map(|entry| entry.path().join("skills"))
            .filter(|path| path.is_dir())
            .collect::<Vec<_>>();
        package_roots.sort();
        roots.extend(package_roots);
    }
    roots
}

/// Read the marker from Pi's resolved agent directory.
pub fn read_profile() -> Option<ChocoPiProfile> {
    read_profile_from(&crate::pi_session::pi_agent_dir()?)
}

/// A marker counts only when it parses, declares the schema this build knows,
/// and names a checkout that still exists. Anything else is treated as absent
/// so a stale marker never hides plain Pi.
pub fn read_profile_from(agent_dir: &Path) -> Option<ChocoPiProfile> {
    let bytes = std::fs::read(profile_path(agent_dir)).ok()?;
    let profile = parse_profile(&bytes)?;
    profile.checkout.is_dir().then_some(profile)
}

/// Parse the marker without touching the filesystem. Unknown fields are
/// ignored so newer installers stay readable.
pub fn parse_profile(bytes: &[u8]) -> Option<ChocoPiProfile> {
    let raw = serde_json::from_slice::<RawProfile>(bytes).ok()?;
    if raw.schema? != PROFILE_SCHEMA {
        return None;
    }
    let checkout = raw.checkout.filter(|path| path.is_absolute())?;
    let (pi_version, pi_launcher) = raw
        .pi
        .map(|pi| (pi.version, pi.launcher.filter(|path| path.is_absolute())))
        .unwrap_or_default();
    let report_status_key = raw
        .host_bridge
        .as_ref()
        .and_then(|bridge| bridge.report_status_key.clone())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_REPORT_STATUS_KEY.to_owned());
    let host_bridge = raw.host_bridge.map(|bridge| {
        let defaults = HostBridgeSpec::default();
        HostBridgeSpec {
            // A bridge block without a schema cannot be trusted to match.
            schema: bridge.schema.unwrap_or(u64::MAX),
            entry_prefix: bridge
                .entry_prefix
                .filter(|value| !value.is_empty())
                .unwrap_or(defaults.entry_prefix),
            status_key: bridge
                .status_key
                .filter(|value| !value.is_empty())
                .unwrap_or(defaults.status_key),
        }
    });
    Some(ChocoPiProfile {
        schema: PROFILE_SCHEMA,
        checkout,
        revision: raw.revision,
        pi_launcher,
        pi_version,
        host_bridge,
        report_status_key,
    })
}
