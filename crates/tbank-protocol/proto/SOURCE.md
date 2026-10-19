# Origin of the .proto files

Official T-Invest API contracts, copied unchanged.

- Repository: https://github.com/RussianInvestments/investAPI (mirror of https://opensource.tbank.ru/invest/invest-contracts)
- Path in the repository: `src/docs/contracts/`
- Commit: `3eaf23a25f598fe483c913184acdbd9132bc68d2`
- Copied: 2026-10-19
- Files: `common`, `instruments`, `marketdata`, `operations`, `orders`, `sandbox`, `stoporders`, `users`
  (`signals.proto` is not needed and not copied).
- `google/api/field_behavior.proto` comes from the same repository (googleapis annotation imported by the contracts).

To update: replace the files with a newer commit of the same path, change the commit above, run
`cargo build -p brokapi-tbank-protocol`. `build.rs` lists the services and methods that get generated code.
