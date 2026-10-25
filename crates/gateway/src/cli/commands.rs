//! The commands, one function each.

use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use brokapi::core::ShutdownMode;
use brokapi_client::{Broker, Client, Settings};
use serde_json::{Value, json};

use super::fmt::{self, duration, gateways_table, is_tool, table};
use super::read::{self, Read};
use super::{Cli, Command, DirectCommand, Fail, GatewayCommand, Res};
use crate::RemoteClient;
use crate::service::config::{
    Config, ConfigError, GatewaySpec, find_config, make_private_dir, sample,
};
use crate::service::{
    CallError, Service, ServiceClient, ServiceError, ServiceOptions, admin, logs, pb, proc,
    registry, run_gateway,
};

struct Ctx {
    config_arg: Option<PathBuf>,
    json: bool,
}

pub(super) async fn run(cli: Cli) -> Res {
    let ctx = Ctx {
        config_arg: cli.config,
        json: cli.json,
    };
    match cli.command {
        Command::Init { path, force } => init(&ctx, path, force),
        Command::Check { offline, names } => check(&ctx, offline, &names).await,
        Command::Up { detach } => up(&ctx, detach).await,
        Command::Down { leave_gateways } => down(&ctx, leave_gateways).await,
        Command::Services => services(&ctx).await,
        Command::Ps => ps(&ctx).await,
        Command::Top { interval, count } => top(&ctx, interval, count).await,
        Command::Start { wait, names } => start(&ctx, &names, wait, false).await,
        Command::Restart { wait, names } => start(&ctx, &names, wait, true).await,
        Command::Stop {
            cancel_orders,
            names,
        } => stop(&ctx, &names, cancel_orders).await,
        Command::Add { name } => add_remove(&ctx, &name, true).await,
        Command::Remove { name } => add_remove(&ctx, &name, false).await,
        Command::Reload => reload(&ctx).await,
        Command::Status { name } => status(&ctx, &name).await,
        Command::Logs {
            name,
            follow,
            lines,
        } => show_logs(&ctx, name.as_deref(), follow, lines).await,
        Command::Gateway {
            command: GatewayCommand::Run { name, log_file },
        } => gateway_run(&ctx, &name, log_file.as_deref()).await,
        Command::Portfolio { gateway } => {
            through_gateway(&ctx, &gateway, Read::Portfolio, None, None).await
        }
        Command::Orders { gateway } => {
            through_gateway(&ctx, &gateway, Read::Orders, None, None).await
        }
        Command::Stops { gateway } => {
            through_gateway(&ctx, &gateway, Read::Stops, None, None).await
        }
        Command::Book {
            gateway,
            ticker,
            board,
            depth,
        } => through_gateway(&ctx, &gateway, Read::Book { depth }, Some(ticker), board).await,
        Command::Quote {
            gateway,
            ticker,
            board,
        } => through_gateway(&ctx, &gateway, Read::Quote, Some(ticker), board).await,
        Command::Direct { broker, from, what } => {
            direct(&ctx, &broker, from.as_deref(), what).await
        }
        Command::CancelAll { gateway } => cancel_all(&ctx, &gateway).await,
    }
}

// ------------------------------------------------------------------------------------------------------ helpers

impl Ctx {
    fn config(&self) -> Result<Config, Fail> {
        let path = find_config(self.config_arg.as_deref()).map_err(config_fail)?;
        Config::load(&path).map_err(config_fail)
    }

    /// How to write the start command for this config, for the messages.
    fn up_hint(&self, config: &Config) -> String {
        match &self.config_arg {
            Some(_) => format!("brokapi -c {} up", config.path.display()),
            None => "brokapi up".to_owned(),
        }
    }

    async fn service(&self, config: &Config) -> Result<ServiceClient, Fail> {
        ServiceClient::connect(&config.state_dir)
            .await
            .map_err(|e| self.call_fail(config, e))
    }

    fn call_fail(&self, config: &Config, e: CallError) -> Fail {
        match e {
            CallError::NotRunning(dir) => Fail::NoService(format!(
                "no service is running for {} (state directory {dir}). Start it with `{}`",
                config.path.display(),
                self.up_hint(config)
            )),
            CallError::Refused(status) => match status.code() {
                tonic::Code::NotFound => Fail::NoGateway(status.message().to_owned()),
                tonic::Code::Unauthenticated => Fail::Failed(
                    "the service refused the access key: is service.key in the state directory the one of this service?".into(),
                ),
                _ => Fail::Failed(status.message().to_owned()),
            },
        }
    }

    fn print_json(&self, value: &Value) {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_default()
        );
    }

    /// Prints the JSON with `--json`, else the text.
    fn emit(&self, value: &Value, text: impl FnOnce() -> String) {
        if self.json {
            self.print_json(value);
        } else {
            print!("{}", text());
        }
    }
}

fn config_fail(e: ConfigError) -> Fail {
    Fail::Usage(e.to_string())
}

fn gateway_json(g: &pb::GatewayInfo) -> Value {
    let h = g.health.as_ref();
    json!({
        "name": g.name,
        "broker": g.broker,
        "state": g.state,
        "pid": (g.pid != 0).then_some(g.pid),
        "uptime_s": g.uptime_ns as f64 / 1e9,
        "restarts": g.restarts,
        "last_error": g.last_error,
        "restart_in_s": (g.restart_in_ns != 0).then_some(g.restart_in_ns as f64 / 1e9),
        "cpu_percent": g.cpu_percent,
        "memory_bytes": g.memory_bytes,
        "account": g.account,
        "sandbox": g.sandbox,
        "adopted": g.adopted,
        "address": g.address,
        "state_dir": g.state_dir,
        "bots": fmt::bots_connected(g),
        "events_per_sec": h.map(|h| h.events_per_sec),
        "events_total": h.map(|h| h.events_total),
        "active_orders": h.map(|h| h.open_orders),
        "limit_used_percent": fmt::limit_used(g),
        "health": h.map(|h| json!({
            "state": gw_state(h.state),
            "detail": h.detail,
            "version": h.version,
            "epoch": h.epoch,
            "last_seq": h.last_seq,
            "bots": h.bots.iter().map(|b| json!({
                "name": b.name, "calls": b.calls, "streaming": b.streaming,
                "queued": b.queued, "lagged": b.lagged, "last_seen_ns": b.last_seen_ns,
            })).collect::<Vec<_>>(),
            "subscriptions": h.subscriptions.iter().map(|s| json!({
                "sub": s.sub, "what": s.what, "owners": s.owners,
            })).collect::<Vec<_>>(),
            "limits": h.limits.iter().map(|l| json!({
                "bucket": l.bucket, "available": l.available, "capacity": l.capacity,
            })).collect::<Vec<_>>(),
        })),
    })
}

fn gw_state(v: i32) -> &'static str {
    match crate::pb::GatewayState::try_from(v) {
        Ok(crate::pb::GatewayState::Ready) => "ready",
        Ok(crate::pb::GatewayState::Starting) => "starting",
        Ok(crate::pb::GatewayState::Stopping) => "stopping",
        _ => "unknown",
    }
}

// ------------------------------------------------------------------------------------------------------ init, check

fn init(ctx: &Ctx, path: Option<PathBuf>, force: bool) -> Res {
    let path = path
        .or_else(|| ctx.config_arg.clone())
        .unwrap_or_else(|| PathBuf::from("brokapi.toml"));
    if path.exists() && !force {
        return Err(Fail::Failed(format!(
            "{} is already there; --force replaces it",
            path.display()
        )));
    }
    let failed = |what: &str, e: std::io::Error| Fail::Failed(format!("{what}: {e}"));
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)
            .map_err(|e| failed(&format!("making {}", dir.display()), e))?;
    }
    let stem = path
        .file_stem()
        .map_or_else(|| "brokapi".into(), |s| s.to_string_lossy().into_owned());
    let state_name = format!("{stem}.state");
    std::fs::write(&path, sample(&state_name))
        .map_err(|e| failed(&format!("writing {}", path.display()), e))?;
    let abs = std::path::absolute(&path).unwrap_or(path.clone());
    let state_dir = abs.parent().unwrap_or(Path::new(".")).join(&state_name);
    make_private_dir(&state_dir)
        .map_err(|e| failed(&format!("making {}", state_dir.display()), e))?;
    ctx.emit(
        &json!({"config": abs, "state_dir": state_dir}),
        || {
            format!(
                "wrote {}\nstate directory {}\nNext: put the account and the token of a broker in the file, then `brokapi check`, then `brokapi up`.\n",
                abs.display(),
                state_dir.display()
            )
        },
    );
    Ok(())
}

async fn check(ctx: &Ctx, offline: bool, names: &[String]) -> Res {
    let config = ctx.config()?;
    for n in names {
        if config.gateway(n).is_none() {
            return Err(Fail::NoGateway(format!(
                "the config {} has no gateway named {n:?}",
                config.path.display()
            )));
        }
    }
    let mut rows = Vec::new();
    let mut problems = 0;
    let state_ok = make_private_dir(&config.state_dir).is_ok() && {
        let probe = config.state_dir.join(".write-test");
        let ok = std::fs::write(&probe, b"").is_ok();
        let _ = std::fs::remove_file(probe);
        ok
    };
    if !state_ok {
        problems += 1;
    }
    let shared = shared_tokens(&config.gateways);
    for spec in config
        .gateways
        .iter()
        .filter(|g| names.is_empty() || names.contains(&g.name))
    {
        let mut notes: Vec<String> = Vec::new();
        if let Some(others) = shared.get(&spec.name) {
            notes.push(format!(
                "warning: {} uses the same token as {}: the broker sees one client, but each gateway counts the rate \
                 limit on its own, so together they can go over it. Keep one gateway per token, or share the buckets \
                 between them in [gateway.limits]",
                spec.name,
                others.join(", ")
            ));
        }
        #[cfg(unix)]
        if let Some(file) = &spec.token_file
            && let Ok(meta) = std::fs::metadata(file)
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                notes.push(format!(
                    "warning: {} can be read by other users (chmod 600)",
                    file.display()
                ));
            }
        }
        let built = spec
            .broker_config()
            .and_then(|c| spec.limiter().map(|l| (c, l)));
        let (config_result, access_result) = match built {
            Err(e) => (Err(e), None),
            Ok((broker_config, limiter)) => {
                let access = if offline {
                    None
                } else {
                    Some(read_access(&spec.name, broker_config, limiter).await)
                };
                (Ok(()), access)
            }
        };
        if config_result.is_err() || matches!(access_result, Some(Err(_))) {
            problems += 1;
        }
        rows.push((
            spec.name.clone(),
            spec.broker.clone(),
            config_result,
            access_result,
            notes,
        ));
    }
    let value = json!({
        "config": config.path,
        "state_dir": config.state_dir,
        "state_dir_writable": state_ok,
        "gateways": rows.iter().map(|(n, b, c, a, notes)| json!({
            "name": n, "broker": b,
            "config": c.as_ref().err().map_or("ok".to_owned(), Clone::clone),
            "access": match a { None => "not checked".to_owned(), Some(Ok(())) => "ok".to_owned(), Some(Err(e)) => e.clone() },
            "notes": notes,
        })).collect::<Vec<_>>(),
        "ok": problems == 0,
    });
    ctx.emit(&value, || {
        let mut out = format!("config {}\n", config.path.display());
        out.push_str(&format!(
            "state directory {} {}\n\n",
            config.state_dir.display(),
            if state_ok {
                "(writable)"
            } else {
                "(CANNOT WRITE)"
            }
        ));
        let mut t = vec![
            ["GATEWAY", "BROKER", "CONFIG", "ACCESS"]
                .map(String::from)
                .to_vec(),
        ];
        for (n, b, c, a, _) in &rows {
            t.push(vec![
                n.clone(),
                b.clone(),
                c.as_ref()
                    .err()
                    .map_or("ok".into(), |e| format!("FAILED: {e}")),
                match a {
                    None => "-".into(),
                    Some(Ok(())) => "ok".into(),
                    Some(Err(e)) => format!("FAILED: {e}"),
                },
            ]);
        }
        out.push_str(&table(&t));
        for (_, _, _, _, notes) in &rows {
            for note in notes {
                out.push_str(&format!("{note}\n"));
            }
        }
        out
    });
    if problems > 0 {
        return Err(Fail::Failed(format!("{problems} problem(s) found")));
    }
    Ok(())
}

/// For every gateway that shares its token with others (the same broker, contour and token, wherever the token is
/// read from): the names of the others. The tokens are compared in memory and never printed.
fn shared_tokens(gateways: &[GatewaySpec]) -> std::collections::HashMap<String, Vec<String>> {
    let mut by_token: std::collections::HashMap<(String, bool, String), Vec<String>> =
        std::collections::HashMap::new();
    for g in gateways {
        if let Ok(token) = g.token() {
            by_token
                .entry((g.broker.clone(), g.sandbox, token))
                .or_default()
                .push(g.name.clone());
        }
    }
    let mut out = std::collections::HashMap::new();
    for group in by_token.into_values().filter(|names| names.len() > 1) {
        for name in &group {
            let others = group.iter().filter(|n| *n != name).cloned().collect();
            out.insert(name.clone(), others);
        }
    }
    out
}

/// Connects to the broker and reads the account: nothing is placed.
async fn read_access(
    name: &str,
    config: brokapi_client::BrokerConfig,
    limiter: Option<brokapi_client::Limiter>,
) -> Result<(), String> {
    let client = Client::start(config, format!("check-{name}"), limiter, 64)
        .await
        .map_err(|e| e.to_string())?;
    use brokapi::core::{OrderExecutor, PortfolioSource};
    let result = async {
        client.portfolio().await?;
        client.active_orders().await?;
        Ok::<(), brokapi::core::Error>(())
    }
    .await;
    client.close(ShutdownMode::Immediate).await;
    result.map_err(|e| e.to_string())
}

// ------------------------------------------------------------------------------------------------------ the service

async fn up(ctx: &Ctx, detach: bool) -> Res {
    let config = ctx.config()?;
    let exe = std::env::current_exe()
        .map_err(|e| Fail::Failed(format!("cannot find this program: {e}")))?;
    if detach {
        return up_detached(ctx, &config, &exe).await;
    }
    make_private_dir(&config.state_dir)
        .and_then(|()| std::fs::create_dir_all(config.layout().logs()))
        .map_err(|e| Fail::Failed(format!("cannot make {}: {e}", config.state_dir.display())))?;
    let _ = logs::init(
        Some((&config.layout().service_log(), &config.service.logs)),
        std::io::stderr().is_terminal() || std::env::var_os("BROKAPI_LOG_STDERR").is_some(),
    );
    let service = Service::start(config.clone(), ServiceOptions { exe })
        .await
        .map_err(|e| match e {
            ServiceError::Occupied { .. } => Fail::Failed(e.to_string()),
            other => Fail::Failed(format!("cannot start the service: {other}")),
        })?;
    let text = format!(
        "service runs, pid {}\nconfig {}\nstate directory {}\n{} gateway(s)\n",
        std::process::id(),
        config.path.display(),
        config.state_dir.display(),
        config.gateways.len()
    );
    if ctx.json {
        ctx.print_json(&json!({
            "pid": std::process::id(),
            "config": config.path,
            "state_dir": config.state_dir,
            "gateways": config.gateways.len(),
        }));
    } else {
        print!("{text}");
    }
    let _ = std::io::stdout().flush();
    let leave = tokio::select! {
        () = crate::service::stop_signal() => false,
        leave = service.shutdown_requested() => leave,
    };
    service.stop(leave).await;
    Ok(())
}

async fn up_detached(ctx: &Ctx, config: &Config, exe: &Path) -> Res {
    if ServiceClient::connect(&config.state_dir).await.is_ok() {
        return Err(Fail::Failed(format!(
            "a service already runs for {} (see `brokapi services`)",
            config.path.display()
        )));
    }
    let mut cmd = tokio::process::Command::new(exe);
    cmd.arg("-c").arg(&config.path).arg("up");
    proc::detach(&mut cmd);
    let mut child = cmd
        .spawn()
        .map_err(|e| Fail::Failed(format!("cannot start {}: {e}", exe.display())))?;
    let log = config.layout().service_log();
    for _ in 0..300 {
        if let Ok(client) = ServiceClient::connect(&config.state_dir).await
            && let Ok(info) = client.info().await
        {
            ctx.emit(
                &json!({"pid": info.pid, "config": info.config, "state_dir": info.state_dir, "gateways": info.gateways}),
                || {
                    format!(
                        "service started in the background, pid {}\nlog {}\n",
                        info.pid,
                        log.display()
                    )
                },
            );
            return Ok(());
        }
        if let Ok(Some(status)) = child.try_wait() {
            let tail = logs::tail_lines(&log, 5).unwrap_or_default().join("\n");
            return Err(Fail::Failed(format!(
                "the service ended at once ({status}). The end of {}:\n{tail}",
                log.display()
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Fail::Failed(format!(
        "the service did not answer in 15 seconds, see {}",
        log.display()
    )))
}

async fn down(ctx: &Ctx, leave: bool) -> Res {
    let config = ctx.config()?;
    let client = ctx.service(&config).await?;
    let pid = client.info().await.map(|i| i.pid).ok();
    client
        .shutdown(leave)
        .await
        .map_err(|e| ctx.call_fail(&config, e))?;
    let limit = config.service.stop_timeout + Duration::from_secs(10);
    let started = std::time::Instant::now();
    while started.elapsed() < limit {
        let gone = match pid {
            Some(pid) => !proc::is_alive(pid),
            None => ServiceClient::connect(&config.state_dir).await.is_err(),
        };
        if gone {
            ctx.emit(
                &json!({"stopped": true, "gateways_left_running": leave}),
                || {
                    if leave {
                        "service stopped, the gateways go on\n".to_owned()
                    } else {
                        "service stopped\n".to_owned()
                    }
                },
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Fail::Failed(
        "the service was asked to stop and has not stopped yet".into(),
    ))
}

async fn services(ctx: &Ctx) -> Res {
    let entries = registry::list();
    let mut rows = Vec::new();
    for e in &entries {
        let info = match ServiceClient::connect(&e.state_dir).await {
            Ok(c) => c.info().await.ok(),
            Err(_) => None,
        };
        rows.push((e.clone(), info));
    }
    let value =
        json!(rows.iter().map(|(e, info)| json!({
        "config": e.config, "state_dir": e.state_dir, "pid": e.pid, "address": e.address,
        "answers": info.is_some(),
        "gateways": info.as_ref().map(|i| i.gateways),
        "running": info.as_ref().map(|i| i.running),
        "uptime_s": info.as_ref().map(|i| i.uptime_ns as f64 / 1e9),
    })).collect::<Vec<_>>());
    ctx.emit(&value, || {
        if rows.is_empty() {
            return "no service is running on this machine\n".to_owned();
        }
        let mut t = vec![
            ["PID", "GATEWAYS", "UPTIME", "CONFIG", "STATE DIRECTORY"]
                .map(String::from)
                .to_vec(),
        ];
        for (e, info) in &rows {
            t.push(vec![
                e.pid.to_string(),
                info.as_ref().map_or("not answering".into(), |i| {
                    format!("{}/{} running", i.running, i.gateways)
                }),
                info.as_ref().map_or("-".into(), |i| duration(i.uptime_ns)),
                e.config.display().to_string(),
                e.state_dir.display().to_string(),
            ]);
        }
        table(&t)
    });
    Ok(())
}

async fn ps(ctx: &Ctx) -> Res {
    let config = ctx.config()?;
    let client = ctx.service(&config).await?;
    let list = client
        .list(Vec::new())
        .await
        .map_err(|e| ctx.call_fail(&config, e))?;
    ctx.emit(
        &json!(list.iter().map(gateway_json).collect::<Vec<_>>()),
        || {
            if list.is_empty() {
                "the service has no gateways\n".to_owned()
            } else {
                gateways_table(&list, false)
            }
        },
    );
    Ok(())
}

async fn top(ctx: &Ctx, interval: f64, count: Option<u32>) -> Res {
    let config = ctx.config()?;
    let client = ctx.service(&config).await?;
    let tty = std::io::stdout().is_terminal() && !ctx.json;
    let pause = Duration::from_secs_f64(interval.clamp(0.1, 3600.0));
    let mut frame = 0u32;
    loop {
        let info = client.info().await.map_err(|e| ctx.call_fail(&config, e))?;
        let list = client
            .list(Vec::new())
            .await
            .map_err(|e| ctx.call_fail(&config, e))?;
        if ctx.json {
            // One object per line, so that a program can read the stream.
            println!(
                "{}",
                json!({"service": {"pid": info.pid, "uptime_s": info.uptime_ns as f64 / 1e9},
                       "gateways": list.iter().map(gateway_json).collect::<Vec<_>>()})
            );
        } else {
            if tty {
                // Home, then clear to the end of the screen: the picture is redrawn in place.
                print!("\x1b[H\x1b[J");
            }
            println!(
                "brokapi top - service pid {}, up {}, {} of {} gateway(s) running   (Ctrl+C to leave)\n",
                info.pid,
                duration(info.uptime_ns),
                info.running,
                info.gateways
            );
            print!("{}", gateways_table(&list, true));
            if !tty {
                println!();
            }
        }
        let _ = std::io::stdout().flush();
        frame += 1;
        if count.is_some_and(|n| frame >= n) {
            return Ok(());
        }
        tokio::select! {
            () = tokio::time::sleep(pause) => {}
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

fn one_line(g: &pb::GatewayInfo) -> String {
    match g.pid {
        0 => format!("{}: {}", g.name, g.state),
        pid => format!("{}: {} (pid {pid})", g.name, g.state),
    }
}

async fn wait_running(client: &ServiceClient, name: &str) -> Result<pb::GatewayInfo, Fail> {
    for _ in 0..1200 {
        let list = client
            .list(vec![name.to_owned()])
            .await
            .map_err(|e| Fail::Failed(e.to_string()))?;
        if let Some(g) = list.into_iter().next() {
            match g.state.as_str() {
                "running" => return Ok(g),
                "failed" | "stopped" => {
                    return Err(Fail::Failed(format!(
                        "{} is {}: {}",
                        name, g.state, g.last_error
                    )));
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Fail::Failed(format!(
        "{name} did not take commands in 60 seconds"
    )))
}

async fn start(ctx: &Ctx, names: &[String], wait: bool, restart: bool) -> Res {
    let config = ctx.config()?;
    let client = ctx.service(&config).await?;
    let mut done = Vec::new();
    for name in names {
        let mut info = if restart {
            client.restart(name).await
        } else {
            client.start(name).await
        }
        .map_err(|e| ctx.call_fail(&config, e))?;
        if wait {
            info = wait_running(&client, name).await?;
        }
        done.push(info);
    }
    ctx.emit(
        &json!(done.iter().map(gateway_json).collect::<Vec<_>>()),
        || done.iter().map(|g| one_line(g) + "\n").collect(),
    );
    Ok(())
}

async fn stop(ctx: &Ctx, names: &[String], cancel: bool) -> Res {
    let config = ctx.config()?;
    let client = ctx.service(&config).await?;
    let mut done = Vec::new();
    for name in names {
        done.push(
            client
                .stop(name, cancel)
                .await
                .map_err(|e| ctx.call_fail(&config, e))?,
        );
    }
    let value = json!(
        done.iter()
            .map(|r| {
                let mut v = r.gateway.as_ref().map(gateway_json).unwrap_or(Value::Null);
                if cancel {
                    v["cancelled_orders"] = r.cancelled_orders.into();
                    v["cancelled_stops"] = r.cancelled_stops.into();
                    v["cancel_errors"] = json!(r.errors);
                }
                v
            })
            .collect::<Vec<_>>()
    );
    ctx.emit(&value, || {
        let mut out = String::new();
        for r in &done {
            if let Some(g) = &r.gateway {
                out.push_str(&one_line(g));
                if cancel {
                    out.push_str(&format!(
                        ", cancelled {} order(s) and {} stop order(s)",
                        r.cancelled_orders, r.cancelled_stops
                    ));
                }
                out.push('\n');
            }
            for e in &r.errors {
                out.push_str(&format!("  not cancelled: {e}\n"));
            }
        }
        out
    });
    if done.iter().any(|r| !r.errors.is_empty()) {
        return Err(Fail::Failed("some orders were not cancelled".into()));
    }
    Ok(())
}

async fn add_remove(ctx: &Ctx, name: &str, add: bool) -> Res {
    let config = ctx.config()?;
    let client = ctx.service(&config).await?;
    let info = if add {
        client.add(name).await
    } else {
        client.remove(name).await
    }
    .map_err(|e| ctx.call_fail(&config, e))?;
    ctx.emit(&gateway_json(&info), || {
        if add {
            one_line(&info) + "\n"
        } else {
            format!("{name}: removed from the service (the config file is not changed)\n")
        }
    });
    Ok(())
}

async fn reload(ctx: &Ctx) -> Res {
    let config = ctx.config()?;
    let client = ctx.service(&config).await?;
    let r = client
        .reload()
        .await
        .map_err(|e| ctx.call_fail(&config, e))?;
    ctx.emit(
        &json!({"started": r.started, "stopped": r.stopped, "restarted": r.restarted, "unchanged": r.unchanged}),
        || {
            let line = |label: &str, v: &Vec<String>| {
                if v.is_empty() {
                    String::new()
                } else {
                    format!("{label}: {}\n", v.join(", "))
                }
            };
            let text = line("started", &r.started)
                + &line("stopped", &r.stopped)
                + &line("restarted", &r.restarted)
                + &line("unchanged", &r.unchanged);
            if text.is_empty() { "no gateways\n".into() } else { text }
        },
    );
    Ok(())
}

async fn status(ctx: &Ctx, name: &str) -> Res {
    let config = ctx.config()?;
    let client = ctx.service(&config).await?;
    let list = client
        .list(vec![name.to_owned()])
        .await
        .map_err(|e| ctx.call_fail(&config, e))?;
    let g = list
        .first()
        .ok_or_else(|| Fail::NoGateway(format!("no gateway named {name:?}")))?;
    ctx.emit(&gateway_json(g), || status_text(g));
    Ok(())
}

fn status_text(g: &pb::GatewayInfo) -> String {
    let mut out = String::new();
    let mut kv = |k: &str, v: String| out.push_str(&format!("{k:<14}{v}\n"));
    kv(
        "gateway",
        format!(
            "{} ({}{}{})",
            g.name,
            g.broker,
            if g.sandbox { ", sandbox" } else { "" },
            if g.account.is_empty() {
                String::new()
            } else {
                format!(", account {}", g.account)
            }
        ),
    );
    kv("state", g.state.clone());
    kv(
        "process",
        if g.pid == 0 {
            "-".into()
        } else {
            format!(
                "pid {}, up {}, {:.1}% cpu, {}{}",
                g.pid,
                duration(g.uptime_ns),
                g.cpu_percent,
                fmt::memory(g.memory_bytes),
                if g.adopted {
                    ", taken over from an earlier service"
                } else {
                    ""
                }
            )
        },
    );
    kv("restarts", g.restarts.to_string());
    if !g.last_error.is_empty() {
        kv("last error", g.last_error.clone());
    }
    if g.restart_in_ns > 0 {
        kv("restart in", duration(g.restart_in_ns));
    }
    if !g.address.is_empty() {
        kv("listens at", g.address.trim().to_owned());
    }
    kv("directory", g.state_dir.clone());
    if let Some(h) = &g.health {
        kv(
            "gateway says",
            format!(
                "{}{}",
                gw_state(h.state),
                if h.detail.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", h.detail)
                }
            ),
        );
        kv("version", h.version.clone());
        kv(
            "events",
            format!(
                "{} so far, {:.1} per second, last number {}",
                h.events_total, h.events_per_sec, h.last_seq
            ),
        );
        kv(
            "journal",
            format!("{} order(s) not known to have ended", h.open_orders),
        );
        let mut tail = String::new();
        if !h.bots.is_empty() {
            let mut t = vec![
                ["BOT", "CONNECTED", "CALLS", "QUEUED", "CUT OFF"]
                    .map(String::from)
                    .to_vec(),
            ];
            for b in &h.bots {
                t.push(vec![
                    if is_tool(&b.name) {
                        format!("{} (tool)", b.name)
                    } else {
                        b.name.clone()
                    },
                    if b.streaming {
                        "yes".into()
                    } else {
                        "no".into()
                    },
                    b.calls.to_string(),
                    b.queued.to_string(),
                    b.lagged.to_string(),
                ]);
            }
            tail.push_str(&format!("\nbots\n{}", table(&t)));
        }
        if !h.subscriptions.is_empty() {
            let mut t = vec![["SUB", "WHAT", "HELD BY"].map(String::from).to_vec()];
            for s in &h.subscriptions {
                t.push(vec![s.sub.to_string(), s.what.clone(), s.owners.join(", ")]);
            }
            tail.push_str(&format!("\nsubscriptions\n{}", table(&t)));
        }
        if !h.limits.is_empty() {
            let mut t = vec![["LIMIT BUCKET", "LEFT", "OF"].map(String::from).to_vec()];
            for l in &h.limits {
                t.push(vec![
                    l.bucket.clone(),
                    l.available.to_string(),
                    l.capacity.to_string(),
                ]);
            }
            tail.push_str(&format!("\nrate limit of the broker\n{}", table(&t)));
        }
        out.push_str(&tail);
    }
    out
}

async fn show_logs(ctx: &Ctx, name: Option<&str>, follow: bool, lines: usize) -> Res {
    let config = ctx.config()?;
    let layout = config.layout();
    let path = match name {
        Some(n) => {
            if config.gateway(n).is_none() {
                return Err(Fail::NoGateway(format!(
                    "the config {} has no gateway named {n:?}",
                    config.path.display()
                )));
            }
            layout.gateway_log(n)
        }
        None => layout.service_log(),
    };
    let show = |line: &str| {
        if ctx.json {
            println!("{}", json!({"line": line}));
        } else {
            println!("{line}");
        }
    };
    if !path.exists() && !follow {
        if !ctx.json {
            println!("no log yet: {}", path.display());
        }
        return Ok(());
    }
    let mut follower = logs::Follower::at_end(&path);
    for line in logs::tail_lines(&path, lines).map_err(|e| Fail::Failed(e.to_string()))? {
        show(&line);
    }
    if !follow {
        return Ok(());
    }
    loop {
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(250)) => {
                let _ = std::io::stdout().flush();
                for line in follower.poll().map_err(|e| Fail::Failed(e.to_string()))? {
                    show(&line);
                }
            }
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

async fn gateway_run(ctx: &Ctx, name: &str, log_file: Option<&Path>) -> Res {
    let config = ctx.config()?;
    run_gateway(&config, name, log_file)
        .await
        .map_err(|e| match e {
            crate::service::GatewayRunError::Config(m) => Fail::Usage(m),
            other => Fail::Failed(other.to_string()),
        })
}

// ------------------------------------------------------------------------------------------------------ reading

async fn connect_gateway(ctx: &Ctx, config: &Config, name: &str) -> Result<RemoteClient, Fail> {
    if config.gateway(name).is_none() {
        let known: Vec<_> = config.gateways.iter().map(|g| g.name.as_str()).collect();
        return Err(Fail::NoGateway(format!(
            "the config {} has no gateway named {name:?} (it has: {})",
            config.path.display(),
            if known.is_empty() {
                "none".into()
            } else {
                known.join(", ")
            }
        )));
    }
    admin::connect_gateway(&config.layout(), name, admin::TOOL_BOT)
        .await
        .map_err(|_| {
            Fail::NoGateway(format!(
                "the gateway {name} is not running. Start the service with `{}`, or the gateway with `brokapi start {name}`",
                ctx.up_hint(config)
            ))
        })
}

async fn through_gateway(
    ctx: &Ctx,
    gateway: &str,
    what: Read,
    ticker: Option<String>,
    board: Option<String>,
) -> Res {
    let config = ctx.config()?;
    let client = connect_gateway(ctx, &config, gateway).await?;
    let instrument = match &ticker {
        Some(t) => Some(
            client
                .resolve_instrument(t, board.as_deref())
                .await
                .map_err(|e| Fail::Failed(format!("{t}: {e}")))?,
        ),
        None => None,
    };
    let value = read::read(&client, &what, instrument.as_ref())
        .await
        .map_err(|e| Fail::Failed(e.to_string()))?;
    ctx.emit(&value, || read::render(&what, &value));
    Ok(())
}

async fn direct(ctx: &Ctx, broker: &str, from: Option<&str>, what: DirectCommand) -> Res {
    let _: Broker = broker
        .parse()
        .map_err(|e: brokapi::core::Error| Fail::Usage(e.to_string()))?;
    let (settings, limiter) = match from {
        Some(name) => {
            let config = ctx.config()?;
            let spec = config.gateway(name).ok_or_else(|| {
                Fail::NoGateway(format!(
                    "the config {} has no gateway named {name:?}",
                    config.path.display()
                ))
            })?;
            if spec.broker != broker {
                return Err(Fail::Usage(format!(
                    "the gateway {name} is a {} gateway, not {broker}",
                    spec.broker
                )));
            }
            (
                Settings(spec.settings_json().map_err(Fail::Usage)?),
                spec.limiter().map_err(Fail::Usage)?,
            )
        }
        None => (Settings(serde_json::Map::new()), None),
    };
    let client = Client::connect(broker, "brokapi-direct", &settings, limiter, 64)
        .await
        .map_err(|e| Fail::Failed(e.to_string()))?;
    let (read_what, ticker, board) = match what {
        DirectCommand::Portfolio => (Read::Portfolio, None, None),
        DirectCommand::Orders => (Read::Orders, None, None),
        DirectCommand::Stops => (Read::Stops, None, None),
        DirectCommand::Book {
            ticker,
            board,
            depth,
        } => (Read::Book { depth }, Some(ticker), board),
        DirectCommand::Quote { ticker, board } => (Read::Quote, Some(ticker), board),
    };
    let result = async {
        let instrument = match &ticker {
            Some(t) => Some(
                client
                    .instrument(t, board.as_deref())
                    .await
                    .map_err(|e| format!("{t}: {e}"))?,
            ),
            None => None,
        };
        read::read(&client, &read_what, instrument.as_ref())
            .await
            .map_err(|e| e.to_string())
    }
    .await;
    client.close(ShutdownMode::Immediate).await;
    let value = result.map_err(Fail::Failed)?;
    ctx.emit(&value, || read::render(&read_what, &value));
    Ok(())
}

async fn cancel_all(ctx: &Ctx, gateway: &str) -> Res {
    let config = ctx.config()?;
    let client = connect_gateway(ctx, &config, gateway).await?;
    let report = admin::cancel_all(&client)
        .await
        .map_err(|e| Fail::Failed(format!("cannot cancel the orders of {gateway}: {e}")))?;
    ctx.emit(&json!(report), || {
        let mut out = format!(
            "{gateway}: cancelled {} order(s) and {} stop order(s)\n",
            report.orders, report.stops
        );
        for e in &report.errors {
            out.push_str(&format!("  not cancelled: {e}\n"));
        }
        out
    });
    if report.errors.is_empty() {
        Ok(())
    } else {
        Err(Fail::Failed(format!(
            "{} order(s) were not cancelled",
            report.errors.len()
        )))
    }
}
