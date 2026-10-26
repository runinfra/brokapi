//! The commands of the program `brokapi`.
//!
//! `brokapi up` starts the service, which runs the gateways. The other commands talk to the service (`ps`, `top`, `start`,
//! `stop`, ...), to a gateway (`portfolio`, `orders`, `cancel-all`, ...) or straight to a broker (`direct`).

mod commands;
mod fmt;
mod read;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

/// What the exit code of a command means. Printed at the end of `brokapi --help`.
pub const EXIT_CODES: &str = "\
Exit codes:
  0  success
  1  the command failed: a broker or a gateway refused, a check found a problem, a file could not be written
  2  wrong usage or a config that cannot be used
  3  no service is running for this config (start it with `brokapi up`)
  4  no such gateway, or the gateway is not running

The config is looked for in: -c FILE, the variable BROKAPI_CONFIG, ./brokapi.toml, then brokapi.toml in the config
directory of the user (~/.config/brokapi on Linux). Two config files are two independent services, each with its own
state directory and its own socket, so any number of services can run on one machine.";

/// Why a command did not succeed, and so the exit code.
#[derive(Debug)]
pub enum Fail {
    /// Exit code 1.
    Failed(String),
    /// Exit code 2.
    Usage(String),
    /// Exit code 3.
    NoService(String),
    /// Exit code 4.
    NoGateway(String),
}

impl Fail {
    /// The exit code.
    pub fn code(&self) -> u8 {
        match self {
            Fail::Failed(_) => 1,
            Fail::Usage(_) => 2,
            Fail::NoService(_) => 3,
            Fail::NoGateway(_) => 4,
        }
    }

    /// The message.
    pub fn message(&self) -> &str {
        match self {
            Fail::Failed(m) | Fail::Usage(m) | Fail::NoService(m) | Fail::NoGateway(m) => m,
        }
    }
}

pub(crate) type Res = Result<(), Fail>;

/// One program: the service, and the commands that run it.
#[derive(Parser, Debug)]
#[command(
    name = "brokapi",
    // The program is `brokapi.exe` on Windows, and clap would print that in `Usage:`. The help names the program the same on
    // every system, as the book does.
    bin_name = "brokapi",
    // With the commit when built from git (build.rs): a build of `main` and the release before it have the same number.
    version = env!("BROKAPI_VERSION"),
    about = "The brokapi gateway: one process per broker account that many bots share. `brokapi up` runs the service that keeps the gateways alive, the other commands look after them.",
    after_help = EXIT_CODES,
    disable_help_subcommand = true
)]
pub struct Cli {
    /// The config file (default: $BROKAPI_CONFIG, ./brokapi.toml, then the config directory of the user).
    #[arg(
        short = 'c',
        long,
        global = true,
        env = "BROKAPI_CONFIG",
        value_name = "FILE"
    )]
    pub config: Option<PathBuf>,
    /// Print JSON instead of text (every command that prints something).
    #[arg(long, global = true)]
    pub json: bool,
    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The commands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Write a sample config and make the state directory.
    Init {
        /// Where to write the config (default ./brokapi.toml).
        path: Option<PathBuf>,
        /// Replace the file if it is there.
        #[arg(long)]
        force: bool,
    },
    /// Check the config and the access of every gateway. Only reads: no order is placed.
    Check {
        /// Do not connect to the brokers, check the file and the tokens only.
        #[arg(long)]
        offline: bool,
        /// Only these gateways.
        names: Vec<String>,
    },
    /// Start the service of this config: it runs the gateways and keeps them alive.
    Up {
        /// Run in the background (a process without a terminal) and return when the service answers.
        #[arg(short, long)]
        detach: bool,
    },
    /// Stop the service of this config, and its gateways.
    Down {
        /// Leave the gateway processes running. A service started later takes them over.
        #[arg(long)]
        leave_gateways: bool,
    },
    /// List the services running on this machine, whatever their config.
    Services,
    /// List the gateways of the service: state, pid, uptime, restarts, bots, events, orders, limit use, last error.
    Ps {
        /// Exit with 1 when a gateway is not running (starting, restarting, stopped or failed): for cron and monitoring.
        #[arg(long)]
        check: bool,
    },
    /// Like `ps`, with CPU and memory, refreshed in place until Ctrl+C.
    Top {
        /// Seconds between refreshes.
        #[arg(short, long, default_value_t = 1.0)]
        interval: f64,
        /// Stop after this many refreshes (default: until Ctrl+C).
        #[arg(short = 'n', long)]
        count: Option<u32>,
    },
    /// Start gateways that are stopped or failed.
    Start {
        /// Wait until they take commands.
        #[arg(long)]
        wait: bool,
        /// The gateways.
        #[arg(required = true)]
        names: Vec<String>,
    },
    /// Stop gateways and keep them stopped. Orders at the exchange are not touched, unless asked.
    Stop {
        /// Cancel the active orders and stop orders of the account first.
        #[arg(long)]
        cancel_orders: bool,
        /// The gateways.
        #[arg(required = true)]
        names: Vec<String>,
    },
    /// Stop gateways and start them again.
    Restart {
        /// Wait until they take commands.
        #[arg(long)]
        wait: bool,
        /// The gateways.
        #[arg(required = true)]
        names: Vec<String>,
    },
    /// Read the config again and start this gateway of it, without stopping the service.
    Add {
        /// The gateway.
        name: String,
    },
    /// Stop a gateway and forget it, without stopping the service. `add` or `reload` bring it back from the config.
    Remove {
        /// The gateway.
        name: String,
    },
    /// Read the config again: start the new gateways, stop the removed ones, restart the changed ones.
    Reload,
    /// Everything about one gateway: connection, journal, subscriptions, bots, limits.
    Status {
        /// The gateway.
        name: String,
    },
    /// Show the log of a gateway, or of the service without a name.
    Logs {
        /// The gateway.
        name: Option<String>,
        /// Keep showing what is added.
        #[arg(short, long)]
        follow: bool,
        /// How many lines from the end.
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: usize,
    },
    /// Run one gateway in this terminal, without the service. The service starts its gateways this way.
    Gateway {
        /// What to do with a gateway.
        #[command(subcommand)]
        command: GatewayCommand,
    },
    /// Money and positions of the account behind a gateway.
    Portfolio {
        /// The gateway.
        gateway: String,
    },
    /// Active orders of the account behind a gateway.
    Orders {
        /// The gateway.
        gateway: String,
    },
    /// Active stop orders of the account behind a gateway.
    Stops {
        /// The gateway.
        gateway: String,
    },
    /// The order book of an instrument, through a gateway.
    Book {
        /// The gateway.
        gateway: String,
        /// The ticker, like SBER.
        ticker: String,
        /// The board or market, like TQBR (default: the broker's choice).
        #[arg(long)]
        board: Option<String>,
        /// How many levels on each side.
        #[arg(long, default_value_t = 5)]
        depth: u32,
    },
    /// Best bid, best ask and the last trade of an instrument, through a gateway.
    Quote {
        /// The gateway.
        gateway: String,
        /// The ticker, like SBER.
        ticker: String,
        /// The board or market, like TQBR.
        #[arg(long)]
        board: Option<String>,
    },
    /// Read straight from a broker, without a service or a gateway: to try a token, or for a one-off question.
    Direct {
        /// The broker: bcs, tbank, finam, alor or bybit.
        broker: String,
        /// Take the settings and the token of this gateway of the config (its broker must be the same). Without it
        /// the settings come from the environment variables of the library (BCS_REFRESH_TOKEN, TBANK_TOKEN, ...) and
        /// a .env file in the directory.
        #[arg(long, global = true, value_name = "GATEWAY")]
        from: Option<String>,
        /// What to read.
        #[command(subcommand)]
        what: DirectCommand,
    },
    /// Emergency: cancel every active order and stop order of the account behind a gateway.
    CancelAll {
        /// The gateway.
        gateway: String,
    },
}

/// The commands of `brokapi gateway`.
#[derive(Subcommand, Debug)]
pub enum GatewayCommand {
    /// Run the gateway until Ctrl+C or SIGTERM.
    Run {
        /// The gateway.
        name: String,
        /// Write the log to this file instead of the terminal.
        #[arg(long, value_name = "FILE")]
        log_file: Option<PathBuf>,
    },
}

/// What `brokapi direct` can read.
#[derive(Subcommand, Debug)]
pub enum DirectCommand {
    /// Money and positions.
    Portfolio,
    /// Active orders.
    Orders,
    /// Active stop orders.
    Stops,
    /// The order book.
    Book {
        /// The ticker.
        ticker: String,
        /// The board.
        #[arg(long)]
        board: Option<String>,
        /// How many levels on each side.
        #[arg(long, default_value_t = 5)]
        depth: u32,
    },
    /// Best bid, best ask, last trade.
    Quote {
        /// The ticker.
        ticker: String,
        /// The board.
        #[arg(long)]
        board: Option<String>,
    },
}

/// Parses the command line, runs the command and returns the exit code.
pub fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("brokapi: cannot start the runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match runtime.block_on(commands::run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(fail) => {
            eprintln!("brokapi: {}", fail.message());
            ExitCode::from(fail.code())
        }
    }
}
