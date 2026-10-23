//! The program `brokapi`: the gateway service and the commands that run it. See `brokapi --help`.

fn main() -> std::process::ExitCode {
    brokapi_gateway::cli::main()
}
