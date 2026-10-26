//! Compiles the `.proto` files of the gateway without `protoc` (pure-Rust `protox`), so neither the build server nor
//! a user on Windows needs a protobuf compiler. Client and server code are both generated: the server is the gateway,
//! the client is `RemoteClient` and any Rust bot.
//!
//! The files live in `proto/` of this crate (`crates/gateway/proto/`): they are the contract for bots in every language,
//! and they have to be inside the crate, or a package from crates.io would have nothing to compile.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const PROTOS: &[&str] = &[
    "brokapi/gateway/v1/gateway.proto",
    "brokapi/service/v1/service.proto",
];

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let proto = manifest.join("proto");
    assert!(
        proto.join(PROTOS[0]).exists(),
        "the .proto files of the gateway are not in {}",
        proto.display()
    );
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
    // Two passes. The gateway first. Then the service, whose messages use the types of the gateway: they are named by
    // their path in this crate (`crate::pb`) instead of being generated a second time.
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(&out)
        .compile_fds(fds.clone())
        .expect("generate gRPC code of the gateway");
    // (In a directory of its own: this pass writes an empty file for the package it does not generate.)
    let service_out = out.join("service");
    std::fs::create_dir_all(&service_out).expect("make the output directory of the service");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .extern_path(".brokapi.gateway.v1", "crate::pb")
        .out_dir(&service_out)
        .compile_fds(fds)
        .expect("generate gRPC code of the service");

    version(&manifest);
}

/// `BROKAPI_VERSION`: the version of the crate, and the commit when the program is built from a git checkout
/// (`1.2.0 (commit 1a2b3c4d5)`). A build from `main` and the release before it have the same version number,
/// and the book on the site follows `main`: the commit is what tells them apart.
///
/// A package from crates.io (it has `.cargo_vcs_info.json`) is a release as it is: the number alone. So is a build where
/// git is not there.
fn version(manifest: &Path) {
    let number = env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION");
    let commit = if manifest.join(".cargo_vcs_info.json").exists() {
        None
    } else {
        git(manifest, &["rev-parse", "--short=9", "HEAD"])
    };
    if let Some(dir) = git(manifest, &["rev-parse", "--absolute-git-dir"]) {
        // A new commit changes `HEAD` (a detached one) or the file of the branch, `packed-refs` after `git gc`.
        let dir = PathBuf::from(dir);
        println!("cargo:rerun-if-changed={}", dir.join("HEAD").display());
        println!(
            "cargo:rerun-if-changed={}",
            dir.join("packed-refs").display()
        );
        if let Some(branch) = git(manifest, &["symbolic-ref", "-q", "HEAD"]) {
            println!("cargo:rerun-if-changed={}", dir.join(branch).display());
        }
    }
    let version = match commit {
        Some(c) => format!("{number} (commit {c})"),
        None => number,
    };
    println!("cargo:rustc-env=BROKAPI_VERSION={version}");
}

/// The output of a git command in `dir`, `None` when there is no git or no repository.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .ok()?;
    let text = String::from_utf8(out.stdout).ok()?.trim().to_owned();
    (out.status.success() && !text.is_empty()).then_some(text)
}
