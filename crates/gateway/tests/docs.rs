//! The Gateway section of the book (docs/src/gateway/) is true.
//!
//! - The reference of the commands (`gateway/commands.md`) holds the help of the program, as the program prints it. When a
//!   command or a flag changes the page changes with it: `BROKAPI_UPDATE_DOCS=1 cargo test -p brokapi-gateway --test docs`
//!   rewrites the generated part, and without that variable the test fails when the page is behind.
//! - Every command of the program is in the table of the page, and every `toml` block of the section is a config that the
//!   program accepts.
//! - The quick start (`gateway/quickstart.md`) is run command by command on an imitation of T-Bank: the commands are the ones
//!   written in the page, and what they print has the shape of the output shown there.
//! - The bot of the page "Подключить бота" is `examples/bot.rs`, run here against a gateway on an imitation of BCS.

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use brokapi::core::Event;
use brokapi_client::Broker;
use brokapi_gateway::cli::Cli;
use brokapi_gateway::service::config::{Config, sample};
use brokapi_gateway::{RemoteClient, Resume};
use clap::CommandFactory;
use common::{Mock, Rig};

#[path = "../examples/bot.rs"]
mod bot;

const EXE: &str = env!("CARGO_BIN_EXE_brokapi");
const BOOK: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/src/gateway");

fn page(name: &str) -> String {
    std::fs::read_to_string(Path::new(BOOK).join(name))
        .unwrap_or_else(|e| panic!("docs/src/gateway/{name}: {e}"))
}

/// The fenced blocks of a page: (the language written after the fence, the text).
fn blocks(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut open: Option<(String, Vec<&str>)> = None;
    for line in text.lines() {
        match (&mut open, line.trim_start().strip_prefix("```")) {
            (None, Some(lang)) => open = Some((lang.trim().to_owned(), Vec::new())),
            (Some(_), Some(rest)) if rest.trim().is_empty() => {
                let (lang, body) = open.take().unwrap();
                out.push((lang, body.join("\n")));
            }
            (Some((_, body)), _) => body.push(line),
            _ => {}
        }
    }
    out
}

// ------------------------------------------------------------------------------------------------ the commands

/// `brokapi --help` and the help of every command, as the program prints them.
fn help_of_every_command() -> Vec<(String, String)> {
    fn help(path: &[String]) -> String {
        let mut args = vec!["brokapi".to_owned()];
        args.extend(path.iter().cloned());
        args.push("--help".into());
        let mut command = Cli::command().term_width(100);
        command.build();
        match command.try_get_matches_from(args) {
            Err(e) if e.kind() == clap::error::ErrorKind::DisplayHelp => {
                e.to_string().trim_end().to_owned()
            }
            other => panic!("`--help` did not print the help: {other:?}"),
        }
    }
    fn walk(command: &clap::Command, path: &mut Vec<String>, out: &mut Vec<(String, String)>) {
        let title = if path.is_empty() {
            "brokapi".to_owned()
        } else {
            format!("brokapi {}", path.join(" "))
        };
        out.push((title, help(path)));
        for sub in command.get_subcommands() {
            if sub.get_name() == "help" {
                continue;
            }
            path.push(sub.get_name().to_owned());
            walk(sub, path, out);
            path.pop();
        }
    }
    let mut out = Vec::new();
    walk(&Cli::command(), &mut Vec::new(), &mut out);
    out
}

const HELP_BEGIN: &str = "<!-- help:begin -->";
const HELP_END: &str = "<!-- help:end -->";

fn generated_help() -> String {
    let mut text = String::new();
    for (title, help) in help_of_every_command() {
        text.push_str(&format!(
            "### {title}\n\n```text\n$ {title} --help\n{help}\n```\n\n"
        ));
    }
    text.trim_end().to_owned() + "\n"
}

#[test]
fn the_reference_of_the_commands_is_what_the_program_prints() {
    let path = Path::new(BOOK).join("commands.md");
    let current = std::fs::read_to_string(&path).expect("docs/src/gateway/commands.md");
    let (head, rest) = current
        .split_once(HELP_BEGIN)
        .expect("commands.md has the line <!-- help:begin -->");
    let (_, tail) = rest
        .split_once(HELP_END)
        .expect("commands.md has the line <!-- help:end -->");
    let wanted = format!(
        "{head}{HELP_BEGIN}\n\n{}\n{HELP_END}{tail}",
        generated_help()
    );
    if std::env::var_os("BROKAPI_UPDATE_DOCS").is_some() {
        std::fs::write(&path, wanted).unwrap();
        return;
    }
    assert!(
        current == wanted,
        "docs/src/gateway/commands.md is not what the program prints. Rewrite it: \
         BROKAPI_UPDATE_DOCS=1 cargo test -p brokapi-gateway --test docs"
    );
}

#[test]
fn every_command_is_in_the_table_of_the_page() {
    let page = page("commands.md");
    let table = page
        .split(HELP_BEGIN)
        .next()
        .expect("the part before the generated help");
    let mut missing = Vec::new();
    for (title, _) in help_of_every_command().into_iter().skip(1) {
        // `brokapi direct portfolio` is written in the table as `brokapi direct <broker> portfolio`: look for the first
        // two words and the last one.
        let words: Vec<&str> = title.split(' ').collect();
        let first = words[..2].join(" ");
        if !table.contains(&first) || (words.len() > 2 && !table.contains(words[words.len() - 1])) {
            missing.push(title);
        }
    }
    assert!(
        missing.is_empty(),
        "commands that the table of commands.md does not mention: {missing:?}"
    );
}

#[test]
fn the_exit_codes_of_the_page_are_the_ones_of_the_program() {
    let page = page("commands.md");
    // The help shows them in the generated part; the table before it explains them. Both must name every code.
    let table = page.split(HELP_BEGIN).next().unwrap();
    for (code, word) in [
        ("0", "успех"),
        ("1", "не удалась"),
        ("2", "неверный вызов"),
        ("3", "нет службы"),
        ("4", "нет такого гейтвея"),
    ] {
        assert!(
            table.contains(&format!("| `{code}` |")) && table.to_lowercase().contains(word),
            "the table of exit codes in commands.md lacks {code} ({word})"
        );
    }
    let help = brokapi_gateway::cli::EXIT_CODES;
    for code in 0..=4 {
        assert!(
            help.contains(&format!("  {code}  ")),
            "code {code} in --help"
        );
    }
}

// ------------------------------------------------------------------------------------------------ the config

/// Every page of the section.
fn section_pages() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(BOOK)
        .expect("docs/src/gateway")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".md"))
        .collect();
    names.sort();
    names
}

#[test]
fn every_toml_block_of_the_section_is_a_config_the_program_accepts() {
    let mut count = 0;
    for name in section_pages() {
        for (lang, body) in blocks(&page(&name)) {
            if lang != "toml" {
                continue;
            }
            count += 1;
            Config::parse(&body, Path::new("/etc/brokapi/brokapi.toml")).unwrap_or_else(|e| {
                panic!("{name}: a toml block is not a valid config: {e}\n{body}")
            });
        }
    }
    assert!(count >= 2, "the section shows configs ({count} found)");
}

#[test]
fn the_sample_that_init_writes_has_the_gateway_of_the_quick_start() {
    let sample = sample("brokapi.state");
    let quick = page("quickstart.md");
    let (_, config) = blocks(&quick)
        .into_iter()
        .find(|(lang, _)| lang == "toml")
        .expect("the quick start shows the gateway section");
    for line in config.lines().filter(|l| !l.trim().is_empty()) {
        assert!(
            sample.contains(line.trim()),
            "the quick start shows `{line}`, which `brokapi init` does not write"
        );
    }
}

// ------------------------------------------------------------------------------------------------ the quick start

struct Out {
    code: i32,
    stdout: String,
    stderr: String,
}

/// Runs the program with the output in files, not pipes (a service started in the background must not hold a pipe), and a
/// limit on the real clock.
fn run(args: &[&str], cwd: &Path, registry: &Path, env: &[(String, String)]) -> Out {
    let stdout = tempfile::tempfile().unwrap();
    let stderr = tempfile::tempfile().unwrap();
    let mut child = Command::new(EXE)
        .args(args)
        .current_dir(cwd)
        .env("BROKAPI_REGISTRY_DIR", registry)
        .env_remove("BROKAPI_CONFIG")
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::null())
        .stdout(stdout.try_clone().unwrap())
        .stderr(stderr.try_clone().unwrap())
        .spawn()
        .expect("the program runs");
    let until = Instant::now() + Duration::from_secs(60);
    let code = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status.code().unwrap_or(-1);
        }
        if Instant::now() >= until {
            let _ = child.kill();
            let _ = child.wait();
            panic!("`brokapi {}` did not end in 60 seconds", args.join(" "));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let read = |mut file: std::fs::File| {
        use std::io::{Read, Seek};
        let mut text = String::new();
        file.rewind().unwrap();
        file.read_to_string(&mut text).unwrap();
        text
    };
    Out {
        code,
        stdout: read(stdout),
        stderr: read(stderr),
    }
}

/// A line as its shape: words with a digit or a slash in them (numbers, times, paths) become `~`, spaces are one.
fn shape(line: &str) -> String {
    line.split_whitespace()
        .map(|w| {
            if w.chars().any(|c| c.is_ascii_digit() || c == '/') {
                "~"
            } else {
                w
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The commands of a page and the output written under each: a `text` block where a line `$ ...` starts a command and the
/// lines up to the next one are what it prints.
fn transcript(text: &str) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for (lang, body) in blocks(text) {
        if lang != "text" {
            continue;
        }
        for line in body.lines() {
            if let Some(command) = line.strip_prefix("$ ") {
                out.push((command.trim().to_owned(), Vec::new()));
            } else if let Some((_, lines)) = out.last_mut() {
                lines.push(line.to_owned());
            }
        }
    }
    out
}

/// Plays the transcript of a page against the imitation of T-Bank: every `$ brokapi ...` command of its `text` blocks is run in a
/// directory of its own, and what it prints must have the shape of what the page shows. `from_scratch`: the page starts with
/// `brokapi init` (the quick start). Otherwise the service of the quick start is already running when the page begins.
async fn play(page_name: &str, from_scratch: bool) -> usize {
    let mock = Mock::start(Broker::Tbank).await;
    let mut settings = mock.settings().0;
    let token = settings
        .remove("token")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    let account = settings
        .remove("account_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    settings.remove("sandbox");

    let (_, section) = blocks(&page("quickstart.md"))
        .into_iter()
        .find(|(lang, _)| lang == "toml")
        .expect("the quick start shows the gateway section");
    let mut config = String::from(
        "state_dir = \"state\"\n[service]\ncheck_interval = \"100ms\"\n[service.restart]\ninitial = \"50ms\"\n\n",
    );
    assert!(section.contains("account = \"2000000000\""));
    config.push_str(&section.replace("2000000000", &account));
    config.push_str("\n[gateway.settings]\n");
    for (key, value) in settings {
        config.push_str(&format!("{key} = {value}\n"));
    }

    let dir = tempfile::tempdir().unwrap();
    let registry = dir.path().join("registry");
    let cwd = dir.path().to_owned();
    let mut env: Vec<(String, String)> = vec![("TBANK_TOKEN".into(), token.clone())];
    let wait_running = |cwd: &Path, registry: &Path, env: &[(String, String)]| {
        // The gateway may still be starting: the page says to repeat the command, the test waits.
        let until = Instant::now() + Duration::from_secs(45);
        loop {
            let out = run(&["ps", "--json"], cwd, registry, env);
            if out.stdout.contains("\"state\": \"running\"") {
                return;
            }
            assert!(
                Instant::now() < until,
                "the gateway did not start: {}",
                out.stdout
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    if !from_scratch {
        std::fs::write(cwd.join("brokapi.toml"), &config).unwrap();
        let out = run(&["up", "-d"], &cwd, &registry, &env);
        assert_eq!(out.code, 0, "{}{}", out.stdout, out.stderr);
        tokio::task::block_in_place(|| wait_running(&cwd, &registry, &env));
    }
    let mut ran = 0;
    for (command, shown) in transcript(&page(page_name)) {
        if let Some(assign) = command.strip_prefix("export ") {
            // `export TBANK_TOKEN=...`: the token of the imitation is already in its place
            let name = assign.split('=').next().unwrap().to_owned();
            env.retain(|(k, _)| *k != name);
            env.push((name, token.clone()));
            continue;
        }
        let args: Vec<&str> = command
            .strip_prefix("brokapi ")
            .unwrap_or_else(|| panic!("{page_name} shows a command that is not brokapi: {command}"))
            .split_whitespace()
            .collect();
        if from_scratch && args[0] == "ps" {
            tokio::task::block_in_place(|| wait_running(&cwd, &registry, &env));
        }
        let out = tokio::task::block_in_place(|| run(&args, &cwd, &registry, &env));
        assert_eq!(
            out.code, 0,
            "`{command}` failed: {}{}",
            out.stdout, out.stderr
        );
        if from_scratch && args[0] == "init" {
            // The step between the commands: the reader writes the account and the token of their own.
            std::fs::write(cwd.join("brokapi.toml"), &config).unwrap();
        }
        let stdout = out.stdout.replace(&account, "2000000000");
        let mut real: Vec<String> = stdout.lines().map(shape).collect();
        let mut wanted: Vec<String> = shown.iter().map(|l| shape(l)).collect();
        // A line `...` in the page stands for the rest of the output (a long table that the page cuts short).
        if let Some(at) = shown.iter().position(|l| l.trim() == "...") {
            wanted.truncate(at);
            real.truncate(at);
        }
        assert_eq!(
            real, wanted,
            "`{command}` prints something else than {page_name} shows"
        );
        ran += 1;
    }
    // Whatever the page did, nothing is left running.
    let _ = run(&["down"], &cwd, &registry, &env);
    let out = run(&["services"], &cwd, &registry, &env);
    assert!(
        out.stdout.contains("no service is running"),
        "{}",
        out.stdout
    );
    ran
}

#[tokio::test(flavor = "multi_thread")]
async fn the_quick_start_runs_command_by_command() {
    let ran = play("quickstart.md", true).await;
    assert!(ran >= 6, "the quick start shows the commands ({ran} run)");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_examples_of_the_commands_page_print_what_the_page_shows() {
    let ran = play("commands.md", false).await;
    assert!(ran >= 5, "the page shows the commands ({ran} run)");
}

// ------------------------------------------------------------------------------------------------ the bot

#[tokio::test(flavor = "multi_thread")]
async fn the_bot_of_the_book_places_and_cancels_through_a_gateway() {
    let rig = Rig::start(Broker::Bcs).await;
    let seen = bot::run(rig.dir.path(), true).await.expect("the bot runs");
    assert!(seen.cid.is_some(), "an order was placed");
    assert!(
        matches!(
            seen.states.last(),
            Some(brokapi::core::OrderState::Cancelled { .. })
        ),
        "the bot saw the order cancelled: {:?}",
        seen.states
    );
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn follow_meets_a_gap_by_subscribing_again_and_reconciling() {
    let rig = Rig::start(Broker::Bcs).await;
    let epoch = rig.gateway.health().epoch;
    let reader = RemoteClient::connect_dir(rig.dir.path(), "my-bot")
        .await
        .unwrap();
    // A number from another run of the gateway: the first thing on the stream is a gap.
    let wrong = Resume {
        epoch: epoch + 2,
        last_seq: 5,
    };
    let task = {
        let reader = reader.clone();
        tokio::spawn(async move {
            bot::follow(&reader, Some(wrong), &[], |e| {
                matches!(e.payload, Event::Order(_))
            })
            .await
        })
    };
    brokapi_mocks::time::wait_until("the bot holds its subscription", || {
        rig.gateway
            .health()
            .subscriptions
            .iter()
            .any(|s| s.owners.iter().any(|o| o == "my-bot"))
    })
    .await;
    common::bcs_cycle(&reader).await;
    let gaps = tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("follow ends when it has seen an order event")
        .unwrap()
        .expect("follow does not fail");
    assert_eq!(gaps, 1, "one gap, which the bot met");
    rig.stop().await;
}
