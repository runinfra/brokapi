//! Compiles the `.proto` files of the gateway without `protoc` (pure-Rust `protox`), so neither the build server nor
//! a user on Windows needs a protobuf compiler. Client and server code are both generated: the server is the gateway,
//! the client is `RemoteClient` and any Rust bot.
//!
//! The files live in `out/proto/` of the repository, next to the crates, because they are the contract for bots in
//! every language and not a detail of this crate.

use std::env;
use std::path::PathBuf;

const PROTOS: &[&str] = &["brokapi/gateway/v1/gateway.proto"];

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    // `../../proto` in the repository, `proto/` when the crate is packaged alone.
    let proto = ["../../proto", "proto"]
        .iter()
        .map(|p| manifest.join(p))
        .find(|p| p.join(PROTOS[0]).exists())
        .expect("the .proto files of the gateway");
    println!("cargo:rerun-if-changed={}", proto.display());
    println!("cargo:rerun-if-changed=build.rs");

    let fds = protox::compile(PROTOS, [&proto]).expect("compile the gateway .proto files");

    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    // The descriptors without the comments, for tools that read the API by its names (`FILE_DESCRIPTOR_SET`).
    let mut bare = fds.clone();
    for file in &mut bare.file {
        file.source_code_info = None;
    }
    std::fs::write(
        out.join("descriptor.bin"),
        prost::Message::encode_to_vec(&bare),
    )
    .expect("write the file descriptor set");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(out)
        .compile_fds(fds)
        .expect("generate gRPC code");
}
