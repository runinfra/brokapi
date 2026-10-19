//! Compiles the vendored official Finam .proto files without `protoc` (pure-Rust `protox`),
//! so neither CI nor Windows users need a protobuf compiler installed.

use std::path::PathBuf;

const SERVICES: &[&str] = &[
    "grpc/tradeapi/v1/auth/auth_service.proto",
    "grpc/tradeapi/v1/accounts/accounts_service.proto",
    "grpc/tradeapi/v1/orders/orders_service.proto",
    "grpc/tradeapi/v1/marketdata/marketdata_service.proto",
    "grpc/tradeapi/v1/assets/assets_service.proto",
    "grpc/tradeapi/v1/metrics/usage_metrics_service.proto",
    "grpc/tradeapi/v1/reports/reports_service.proto",
    "grpc/tradeapi/v1/corporateactions/corporate_actions_service.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("proto");
    println!("cargo:rerun-if-changed={}", proto_dir.display());

    let mut compiler = protox::Compiler::new([&proto_dir])?;
    compiler.include_imports(true);
    compiler.open_files(SERVICES)?;
    let mut fds = compiler.file_descriptor_set();

    // Keep only what is needed to resolve types: the Trade API, google.type (Decimal, Money...) and
    // the well-known types (mapped to `prost_types`, no code generated). google.api / gateway options
    // are descriptor-only noise.
    fds.file.retain(|f| {
        let name = f.name();
        name.starts_with("grpc/tradeapi/")
            || name.starts_with("google/type/")
            || name.starts_with("google/protobuf/")
    });

    let mut config = tonic_prost_build::Config::new();
    config.include_file("all.rs");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds_with_config(fds, config)?;
    Ok(())
}
