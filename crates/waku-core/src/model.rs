//! Daemon-only provider discovery layered over shared protocol models.

use std::path::Path;

pub use waku_protocol::model::*;

pub fn provider_probe(provider: ProviderKind, binary_override: Option<&str>) -> ProviderProbe {
    let profile = matches!(provider, ProviderKind::Pi | ProviderKind::ChocoPi)
        .then(crate::choco_pi_profile::read_profile)
        .flatten();
    provider_probe_with(provider, binary_override, profile.as_ref(), || {
        crate::command_env::find_executable(provider.command())
    })
}

fn provider_probe_with(
    provider: ProviderKind,
    binary_override: Option<&str>,
    profile: Option<&crate::choco_pi_profile::ChocoPiProfile>,
    fallback: impl FnOnce() -> Option<std::path::PathBuf>,
) -> ProviderProbe {
    let path = if provider == ProviderKind::Pi && binary_override.is_none() && profile.is_some() {
        None
    } else if provider == ProviderKind::ChocoPi {
        crate::choco_pi_profile::resolve_launcher_with(binary_override, profile, fallback)
    } else {
        match binary_override {
            Some(binary) => crate::command_env::resolve_binary_override(binary),
            None => fallback(),
        }
    };
    ProviderProbe {
        provider,
        installed: path.is_some() && (provider != ProviderKind::ChocoPi || profile.is_some()),
        path,
        models: crate::model_catalog::fallback_models(provider),
        agent_presets: crate::model_catalog::fallback_agent_presets(provider),
    }
}

/// Detect a provider and hydrate its catalog from the daemon-owned cache.
///
/// This is the fast half of stale-while-revalidate: clients can render the
/// last successful catalog immediately, then request live discovery to replace
/// it. Cache I/O stays in the daemon instead of leaking host filesystem access
/// into desktop or Web clients.
pub fn cached_provider_probe(
    provider: ProviderKind,
    binary_override: Option<&str>,
) -> ProviderProbe {
    let cached = crate::model_catalog::cached_models(provider);
    apply_cached_models(provider_probe(provider, binary_override), cached)
}

fn apply_cached_models(
    mut probe: ProviderProbe,
    cached_models: Option<Vec<ProviderModel>>,
) -> ProviderProbe {
    if probe.provider.supports_model_discovery()
        && let Some(models) = cached_models
    {
        probe.models = models;
    }
    probe
}

pub fn discover_provider_models(mut probe: ProviderProbe) -> ProviderProbe {
    if probe.provider.supports_model_discovery()
        && let Some(path) = probe.path.as_deref()
    {
        let (models, agent_presets) = crate::model_catalog::discover_catalog(probe.provider, path);
        probe.models = models;
        probe.agent_presets = agent_presets;
    }
    probe
}

/// Run `<cli> --version` on the daemon host and extract its first version-like
/// token. Provider CLIs decorate this output differently, so clients receive a
/// normalized value rather than subprocess output.
pub fn probe_provider_version(binary: &Path) -> Option<String> {
    let mut command = crate::command_env::command(binary);
    let command = command.arg("--version").stdin(std::process::Stdio::null());
    let output = crate::command_env::output(command).ok()?;
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    parse_cli_version(&combined)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chocopi_probe_requires_valid_marker_and_resolves_launcher() {
        use crate::choco_pi_profile::{profile_path, read_profile_from};
        let root = std::env::temp_dir().join(format!("waku-profile-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("choco-pi")).unwrap();
        let launcher = root.join("pi");
        std::fs::write(&launcher, "#!/bin/sh\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let probe = |profile: Option<crate::choco_pi_profile::ChocoPiProfile>| {
            provider_probe_with(ProviderKind::ChocoPi, None, profile.as_ref(), || {
                Some(launcher.clone())
            })
        };
        assert!(!probe(read_profile_from(&root)).installed, "absent marker");
        for (schema, checkout, expected) in [
            (1, root.clone(), true),
            (2, root.clone(), false),
            (1, root.join("missing"), false),
        ] {
            std::fs::write(
                profile_path(&root),
                serde_json::json!({"schema":schema,"checkout":checkout,"pi":{"launcher":launcher}})
                    .to_string(),
            )
            .unwrap();
            assert_eq!(probe(read_profile_from(&root)).installed, expected);
        }
        std::fs::write(
            profile_path(&root),
            serde_json::json!({"schema":1,"checkout":root,"pi":{"launcher":launcher}}).to_string(),
        )
        .unwrap();
        let mut profile = read_profile_from(&root).unwrap();
        let preferred = provider_probe_with(ProviderKind::ChocoPi, None, Some(&profile), || None);
        assert!(preferred.installed);
        assert_eq!(preferred.path, Some(launcher.clone()));
        let hidden = provider_probe_with(ProviderKind::Pi, None, Some(&profile), || {
            Some(launcher.clone())
        });
        assert!(!hidden.installed);
        assert_eq!(hidden.path, None);
        let override_probe =
            provider_probe_with(ProviderKind::Pi, launcher.to_str(), Some(&profile), || None);
        assert!(override_probe.installed);
        assert_eq!(override_probe.path, Some(launcher.clone()));
        profile.pi_launcher = Some(root.join("missing-launcher"));
        assert!(
            !provider_probe_with(ProviderKind::ChocoPi, None, Some(&profile), || None).installed
        );
        assert_eq!(probe(Some(profile.clone())).path, Some(launcher.clone()));
        profile.pi_launcher = None;
        assert!(
            !provider_probe_with(ProviderKind::ChocoPi, None, Some(&profile), || None).installed
        );
        assert_eq!(probe(Some(profile.clone())).path, Some(launcher.clone()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            profile.pi_launcher = Some(launcher.clone());
            std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(
                !provider_probe_with(ProviderKind::ChocoPi, None, Some(&profile), || None)
                    .installed
            );
            let fallback = root.join("fallback-pi");
            assert_eq!(
                provider_probe_with(ProviderKind::ChocoPi, None, Some(&profile), || Some(
                    fallback.clone()
                ))
                .path,
                Some(fallback)
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn chocopi_override_wins_over_profile_and_does_not_fall_back() {
        let root = std::env::temp_dir().join(format!("waku-override-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let binary = root.join("pi");
        std::fs::write(&binary, "launcher").unwrap();
        let profile = crate::choco_pi_profile::parse_profile(
            serde_json::json!({"schema":1,"checkout":root,"pi":{"launcher":"/ignored"}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        let probe = provider_probe_with(
            ProviderKind::ChocoPi,
            binary.to_str(),
            Some(&profile),
            || panic!("override must win"),
        );
        assert!(probe.installed);
        assert_eq!(probe.path, Some(binary));
        assert!(
            !provider_probe_with(
                ProviderKind::ChocoPi,
                root.join("missing").to_str(),
                Some(&profile),
                || panic!("invalid override must not fall back")
            )
            .installed
        );
        assert!(provider_probe_with(ProviderKind::Pi, None, None, || Some(root.clone())).installed);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cached_catalog_replaces_fallback_before_live_discovery() {
        let probe = ProviderProbe {
            provider: ProviderKind::Codex,
            installed: true,
            path: Some("/usr/bin/codex".into()),
            models: crate::model_catalog::fallback_models(ProviderKind::Codex),
            agent_presets: Vec::new(),
        };
        let cached = vec![ProviderModel::new("cached-model", "Cached model").default()];

        let probe = apply_cached_models(probe, Some(cached));

        assert_eq!(probe.models.len(), 1);
        assert_eq!(probe.models[0].id, "cached-model");
    }
}
