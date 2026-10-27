//! The Go bot of the book (`go/examples/bot`) against a gateway on the imitation of BCS: it places an order, the gateway is
//! killed and started again under it, the bot connects again, meets the Gap, subscribes again and reconciles, and sees the
//! fill through the new process.
//!
//! It needs `go` in PATH. Without it the test says so and passes, unless `BROKAPI_GO=1` (the CI job with Go sets it): there
//! a missing `go` is a failure.

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use brokapi::core::{Price, Qty};
use brokapi_client::Broker;
use brokapi_gateway::{Gateway, GatewayConfig};
use common::{Mock, roomy_limits};
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{ChildStdout, Command};

fn module() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../go")
}

/// `go`, or `None` when it is not there and it is not required.
fn go() -> Option<&'static str> {
    let found = std::process::Command::new("go")
        .arg("version")
        .output()
        .is_ok_and(|o| o.status.success());
    if found {
        return Some("go");
    }
    assert!(
        std::env::var("BROKAPI_GO").as_deref() != Ok("1"),
        "BROKAPI_GO=1 and there is no go in PATH"
    );
    eprintln!("no go in PATH: the Go bot is not run (BROKAPI_GO=1 makes it a failure)");
    None
}

async fn start(mock: &Mock, dir: &Path) -> Gateway {
    let gateway = Gateway::start(
        GatewayConfig::new("bcs-main", dir),
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .expect("the gateway starts");
    gateway.ready().await;
    gateway
}

/// The next line of the bot that starts with `prefix`, the lines before it are printed for the log of the test.
async fn line(out: &mut Lines<BufReader<ChildStdout>>, prefix: &str) -> String {
    let wait = async {
        while let Some(l) = out.next_line().await.expect("the output of the bot") {
            println!("bot: {l}");
            if l.starts_with(prefix) {
                return l;
            }
        }
        panic!("the bot ended before printing {prefix:?}");
    };
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .unwrap_or_else(|_| panic!("the bot did not print {prefix:?} in time"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_go_bot_survives_a_new_gateway_process_and_sees_the_fill() {
    let Some(go) = go() else { return };
    let work = tempfile::tempdir().unwrap();
    // Built before the gateway starts: the time of the build is not in the waits below.
    let bin = work
        .path()
        .join(if cfg!(windows) { "bot.exe" } else { "bot" });
    let built = Command::new(go)
        .args(["build", "-o"])
        .arg(&bin)
        .arg("./examples/bot")
        .current_dir(module())
        .status()
        .await
        .expect("go build runs");
    assert!(built.success(), "the Go bot builds");

    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let first = start(&mock, dir.path()).await;

    let mut bot = Command::new(&bin)
        .arg("-dir")
        .arg(dir.path())
        .env("BROKAPI_LIVE", "1")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("the bot starts");
    let mut out = BufReader::new(bot.stdout.take().unwrap()).lines();
    let placed = line(&mut out, "placed ").await;
    let cid = placed.trim_start_matches("placed ").to_string();
    brokapi_mocks::time::wait_until("the order stands", || first.health().open_orders == 1).await;

    // The gateway dies and a new process takes its place in the same directory.
    first.kill().await;
    let second = start(&mock, dir.path()).await;
    line(&mut out, "gap REASON_NEW_EPOCH").await;

    // The order is filled at the broker: the bot hears it through the new process and ends.
    let (server, _) = mock.bcs();
    server.fill(&cid, Qty::from(1), Price::from(100));
    let ended = line(&mut out, "the order ended:").await;
    assert_eq!(ended, "the order ended: ORDER_STATE_KIND_FILLED");
    let status = tokio::time::timeout(Duration::from_secs(10), bot.wait())
        .await
        .expect("the bot exits after the order ended")
        .unwrap();
    assert!(status.success(), "the bot exits with 0");
    second
        .shutdown(brokapi::core::ShutdownMode::Immediate)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_go_bot_without_brokapi_live_reads_and_places_nothing() {
    let Some(go) = go() else { return };
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let gateway = start(&mock, dir.path()).await;
    let out = Command::new(go)
        .args(["run", "./examples/bot", "-dir"])
        .arg(dir.path())
        .env_remove("BROKAPI_LIVE")
        .current_dir(module())
        .output()
        .await
        .expect("go run runs");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "the bot exits with 0: {text}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains("read only: set BROKAPI_LIVE=1"), "{text}");
    assert_eq!(gateway.health().open_orders, 0, "no order was placed");
    gateway
        .shutdown(brokapi::core::ShutdownMode::Immediate)
        .await;
}
