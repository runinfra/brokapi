//! The body of a gateway process: what `brokapi gateway run <name>` does, and what the service starts.

use std::path::Path;

use brokapi::core::ShutdownMode;

use crate::{Gateway, GatewayConfig, GatewayError};

use super::config::Config;
use super::{logs, proc};

/// Why a gateway process could not run.
#[derive(Debug, thiserror::Error)]
pub enum GatewayRunError {
    /// The config does not allow it: no such gateway, no token, a wrong setting.
    #[error("{0}")]
    Config(String),
    /// Another process of this gateway runs.
    #[error("the gateway {name} already runs (pid {pid})")]
    Running {
        /// The gateway.
        name: String,
        /// The process.
        pid: u32,
    },
    /// The gateway did not start.
    #[error("{0}")]
    Start(#[from] GatewayError),
    /// A file could not be written.
    #[error("{what}: {source}")]
    Io {
        /// What was being done.
        what: String,
        /// The error of the system.
        source: std::io::Error,
    },
}

/// Waits for the request to stop: SIGTERM or SIGINT on Unix, Ctrl+C elsewhere. The loss of the terminal (SIGHUP) is
/// ignored, a gateway outlives the terminal it was started in.
pub(crate) async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut term), Ok(mut int), Ok(mut hup)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
            signal(SignalKind::hangup()),
        ) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::spawn(async move { while hup.recv().await.is_some() {} });
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Runs the gateway `name` of the config until it is asked to stop. With `log_file` the log goes to that file (rotated by the
/// policy of the config), without it to the error output.
pub async fn run_gateway(
    config: &Config,
    name: &str,
    log_file: Option<&Path>,
) -> Result<(), GatewayRunError> {
    let _ = logs::init(
        log_file.map(|p| (p, &config.service.logs)),
        log_file.is_none(),
    );
    let result = run(config, name).await;
    if let Err(e) = &result {
        // Into the log too: it is where the service looks for the reason when the process ends at once.
        tracing::error!("{e}");
    }
    result
}

async fn run(config: &Config, name: &str) -> Result<(), GatewayRunError> {
    let spec = config.gateway(name).ok_or_else(|| {
        let known: Vec<_> = config.gateways.iter().map(|g| g.name.as_str()).collect();
        GatewayRunError::Config(format!(
            "the config {} has no gateway named {name:?} (it has: {})",
            config.path.display(),
            if known.is_empty() {
                "none".into()
            } else {
                known.join(", ")
            }
        ))
    })?;
    let broker = spec.broker_config().map_err(GatewayRunError::Config)?;
    let limiter = spec.limiter().map_err(GatewayRunError::Config)?;

    let layout = config.layout();
    let dir = layout.gateway_dir(name);
    super::config::make_private_dir(&dir).map_err(|source| GatewayRunError::Io {
        what: format!("making {}", dir.display()),
        source,
    })?;
    let pid_file = layout.gateway_pid(name);
    if let Some(pid) = proc::read_pid(&pid_file)
        && pid != std::process::id()
        && proc::is_brokapi(pid)
    {
        return Err(GatewayRunError::Running {
            name: name.to_owned(),
            pid,
        });
    }

    let mut gw = GatewayConfig::new(name, &dir);
    gw.ring = spec.ring_config();
    if let Some(n) = spec.bot_buffer {
        gw.bot_buffer = n;
    }
    if let Some(grace) = spec.bot_grace {
        gw.bot_grace = grace;
    }
    let gateway = Gateway::start(gw, broker, limiter).await?;
    std::fs::write(&pid_file, std::process::id().to_string()).map_err(|source| {
        GatewayRunError::Io {
            what: format!("writing {}", pid_file.display()),
            source,
        }
    })?;
    tracing::info!(
        gateway = name,
        broker = %spec.broker,
        pid = std::process::id(),
        address = %gateway.address(),
        "the gateway runs"
    );
    stop_signal().await;
    tracing::info!(gateway = name, "stopping on request");
    gateway.shutdown(ShutdownMode::Immediate).await;
    let _ = std::fs::remove_file(&pid_file);
    tracing::info!(gateway = name, "stopped");
    Ok(())
}
