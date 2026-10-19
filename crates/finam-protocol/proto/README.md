# Vendored Finam Trade API protos

Source: https://github.com/FinamWeb/finam-trade-api, directory `proto/`,
commit `52c95c8672542c2f89fbee2a4016774e73a0caa4`. Files are copied unmodified.

Included: the services the connector uses (auth, accounts, orders, marketdata, assets, metrics),
`side.proto`, `trade.proto`, and their imports that are not built into `protox`
(`google/api`, `google/type`, `grpc/gateway/protoc_gen_openapiv2/options`).
Not included: reports and corporate actions services (unused).

Compiled by `build.rs` with `protox` (pure Rust, no `protoc` needed). To update, copy the new files
over these and rebuild; record the new commit here.
