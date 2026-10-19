//! Compiles the official T-Bank `.proto` files without `protoc` (pure-Rust `protox`), so neither the
//! build server nor Windows users need to install it. Clients are generated for EVERY service and
//! method of every `.proto` (the library covers the whole API, see the Coverage page of the book); all messages
//! and enums are kept. With feature `server` the server traits are generated too, with default stubs
//! (`Unimplemented`), so a mock implements only the methods a test needs.

use std::env;
use std::path::PathBuf;

const PROTOS: &[&str] = &[
    "common.proto",
    "instruments.proto",
    "marketdata.proto",
    "operations.proto",
    "orders.proto",
    "sandbox.proto",
    "signals.proto",
    "stoporders.proto",
    "users.proto",
];

fn main() {
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=build.rs");

    let fds = protox::compile(PROTOS, ["proto"]).expect("compile T-Bank .proto files");

    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let server = env::var_os("CARGO_FEATURE_SERVER").is_some();
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(server)
        .generate_default_stubs(true)
        .out_dir(out)
        .compile_fds(fds)
        .expect("generate gRPC code");
}
