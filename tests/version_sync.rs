//! One version number, in one place: `version` in `[workspace.package]` of the root `Cargo.toml`.
//!
//! The crates take it with `version.workspace = true`. What cannot take it is checked here, so a release that moves the
//! number in one place and forgets another fails the tests instead of shipping two versions:
//!
//! - the versions of the internal crates in `[workspace.dependencies]` (cargo needs them written out to publish),
//! - the Python extension (`crates/python` is a workspace of its own, it cannot inherit),
//! - the book and the Python package never write the number: pages use `{{brokapi_version}}`, `__version__` comes from the
//!   metadata of the package.
//!
//! Skipped when the book or the Python package is not there (the crate as published has no `docs/`).

use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The text of one `[table]` of a manifest, up to the next table.
fn table<'a>(manifest: &'a str, name: &str) -> &'a str {
    let header = format!("[{name}]\n");
    let start = manifest
        .find(&header)
        .unwrap_or_else(|| panic!("no [{name}]"))
        + header.len();
    let end = manifest[start..]
        .find("\n[")
        .map_or(manifest.len(), |i| start + i);
    &manifest[start..end]
}

fn quoted_version(line: &str) -> Option<&str> {
    let rest = line.split("version").nth(1)?;
    rest.split('"').nth(1)
}

fn workspace_version() -> String {
    let manifest = read(&root().join("Cargo.toml"));
    let line = table(&manifest, "workspace.package")
        .lines()
        .find(|l| l.starts_with("version"))
        .expect("version in [workspace.package]");
    quoted_version(line).expect("a quoted version").to_string()
}

/// 0.1.0-alpha.3 -> 0.1.0a3, the form of PyPI.
fn pep440(version: &str) -> String {
    match version.split_once('-') {
        None => version.to_string(),
        Some((base, pre)) => {
            let (kind, number) = pre.split_once('.').expect("alpha.N, beta.N or rc.N");
            let kind = match kind {
                "alpha" => "a",
                "beta" => "b",
                "rc" => "rc",
                other => panic!("unknown pre-release {other}"),
            };
            format!("{base}{kind}{number}")
        }
    }
}

#[test]
fn pep440_form_of_a_version() {
    assert_eq!(pep440("0.1.0-alpha.3"), "0.1.0a3");
    assert_eq!(pep440("0.2.0-beta.1"), "0.2.0b1");
    assert_eq!(pep440("1.0.0-rc.2"), "1.0.0rc2");
    assert_eq!(pep440("1.0.0"), "1.0.0");
}

#[test]
fn internal_crates_are_pinned_to_the_workspace_version() {
    let want = workspace_version();
    let manifest = read(&root().join("Cargo.toml"));
    let mut seen = 0;
    for line in table(&manifest, "workspace.dependencies").lines() {
        // brokapi-conformance and brokapi-mocks are not published and carry no version. `brokapi` is the root crate.
        if (line.starts_with("brokapi-") || line.starts_with("brokapi ="))
            && line.contains("version")
        {
            let got = quoted_version(line).expect("a quoted version");
            assert_eq!(got, want, "{line}");
            seen += 1;
        }
    }
    assert!(seen >= 14, "the published crates are pinned, saw {seen}");
}

#[test]
fn every_crate_takes_the_workspace_version() {
    let crates = root().join("crates");
    for entry in std::fs::read_dir(&crates).unwrap() {
        let manifest = entry.unwrap().path().join("Cargo.toml");
        if manifest.ends_with("python/Cargo.toml") {
            continue; // a workspace of its own, see the next test
        }
        let text = read(&manifest);
        assert!(
            table(&text, "package").contains("version.workspace = true"),
            "{} writes its own version",
            manifest.display()
        );
    }
}

#[test]
fn the_python_extension_has_the_workspace_version() {
    let manifest = root().join("crates/python/Cargo.toml");
    if !manifest.exists() {
        return;
    }
    let text = read(&manifest);
    let line = table(&text, "package")
        .lines()
        .find(|l| l.starts_with("version"))
        .expect("version in [package]");
    assert_eq!(
        quoted_version(line).unwrap(),
        workspace_version(),
        "crates/python/Cargo.toml: maturin takes the version of the Python package from here"
    );
}

fn files(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|n| n == "book" || n == "target")
            {
                continue;
            }
            files(&path, extensions, out);
        } else if path
            .extension()
            .is_some_and(|e| extensions.iter().any(|x| e == *x))
        {
            out.push(path);
        }
    }
}

/// Finds `0.1.0-alpha.3` or `0.1.0a3` written by hand: three numbers with a pre-release tail.
fn hand_written_versions(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if !(bytes[i].is_ascii_digit()
            && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'.')))
        {
            i += 1;
            continue;
        }
        let mut j = i;
        let mut dots = 0;
        while j < bytes.len()
            && (bytes[j].is_ascii_digit()
                || (bytes[j] == b'.'
                    && dots < 2
                    && j + 1 < bytes.len()
                    && bytes[j + 1].is_ascii_digit()))
        {
            if bytes[j] == b'.' {
                dots += 1;
            }
            j += 1;
        }
        let tail = &text[j..];
        let pre = dots == 2
            && (tail.starts_with("-alpha.")
                || tail.starts_with("-beta.")
                || tail.starts_with("-rc.")
                || (tail.starts_with('a') || tail.starts_with('b') || tail.starts_with("rc"))
                    && tail
                        .trim_start_matches(|c: char| c.is_ascii_alphabetic())
                        .starts_with(|c: char| c.is_ascii_digit()));
        if pre {
            let end = tail
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '-'))
                .unwrap_or(tail.len());
            found.push(text[i..j + end].to_string());
        }
        i = j.max(i + 1);
    }
    found
}

#[test]
fn detector_of_hand_written_versions() {
    assert_eq!(
        hand_written_versions(r#"version = "0.1.0-alpha.3","#),
        ["0.1.0-alpha.3"]
    );
    assert_eq!(hand_written_versions("prints `0.1.0a3`."), ["0.1.0a3"]);
    assert_eq!(
        hand_written_versions("0.2.0-rc.1 and 1.0.0b2"),
        ["0.2.0-rc.1", "1.0.0b2"]
    );
    assert!(hand_written_versions("Rust 1.88, tonic 0.14, 1.2.3, a10.2.3abc").is_empty());
    assert!(hand_written_versions("{{brokapi_version}} and {{brokapi_version_py}}").is_empty());
}

#[test]
fn the_number_is_not_written_by_hand() {
    let mut places = vec![root().join("src"), root().join("README.md")];
    for dir in [
        "docs/src",
        "docs/theme",
        "crates/python/python",
        "crates/python/README.md",
    ] {
        places.push(root().join(dir));
    }
    let mut checked = 0;
    for place in places.into_iter().filter(|p| p.exists()) {
        let mut list = Vec::new();
        if place.is_dir() {
            files(&place, &["rs", "md", "hbs", "py", "toml"], &mut list);
        } else {
            list.push(place);
        }
        for file in list {
            // The changelog tells the history: it names the versions that were released.
            if file.file_name().is_some_and(|n| n == "changelog.md") {
                continue;
            }
            let found = hand_written_versions(&read(&file));
            assert!(
                found.is_empty(),
                "{} writes the version by hand ({found:?}): use {{{{brokapi_version}}}} (see docs/brokapi_version.py)",
                file.display()
            );
            checked += 1;
        }
    }
    assert!(checked > 20, "looked at {checked} files");
}

#[test]
fn the_book_has_the_places_for_the_number() {
    let docs = root().join("docs");
    if !docs.exists() {
        return;
    }
    // The strip of every page takes the number from the build (docs/brokapi_version.py stamp).
    assert!(read(&docs.join("theme/index.hbs")).contains("@@BROKAPI_VERSION@@"));
    assert!(read(&docs.join("src/rust/quickstart.md")).contains("{{brokapi_version}}"));
    assert!(read(&docs.join("src/python/quickstart.md")).contains("{{brokapi_version_py}}"));
}
