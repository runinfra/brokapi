//! The Rust version the book asks for is the `rust-version` of the crate.
//!
//! The code of the book uses `let` chains (Rust 1.88). When the minimum version of the crate changes, the pages that name
//! it must change too. Skipped when the book is not there (the crate as published has no `docs/`).

use std::path::Path;

fn rust_version() -> String {
    let manifest =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();
    let line = manifest
        .lines()
        .find(|l| l.trim_start().starts_with("rust-version"))
        .expect("rust-version in the workspace manifest");
    line.split('"')
        .nth(1)
        .expect("a quoted version")
        .to_string()
}

fn pages(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            pages(&path, out);
        } else if path.extension().is_some_and(|e| e == "md") {
            out.push(path);
        }
    }
}

#[test]
fn the_book_names_the_rust_version_of_the_crate() {
    let docs = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/src");
    if !docs.exists() {
        return;
    }
    let want = rust_version();
    let mut files = Vec::new();
    pages(&docs, &mut files);
    let mut named = 0;
    for file in files {
        // The changelog tells the history, and may name an old version.
        if file.file_name().is_some_and(|n| n == "changelog.md") {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap();
        let mut rest = text.as_str();
        while let Some(at) = rest.find("Rust 1.") {
            let tail = &rest[at + "Rust ".len()..];
            let version: String = tail
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            let version = version.trim_end_matches('.');
            assert_eq!(
                version,
                want,
                "{} names Rust {version}, the crate needs {want}",
                file.display()
            );
            named += 1;
            rest = &rest[at + 6..];
        }
    }
    assert!(
        named >= 3,
        "the quickstart and the how-to pages name the version"
    );
}
