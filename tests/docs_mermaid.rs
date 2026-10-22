//! Diagram labels in the book that Mermaid cannot parse.
//!
//! A label in square brackets ends at the first `(` or `)`: `B[start, MD (market)]` is a syntax error, and the page shows
//! "Syntax error in text" instead of the diagram. The build does not notice (Mermaid draws in the browser), so it is
//! caught here. Such a label goes in quotes: `B["start, MD (market)"]`. `[(...)]` is a different shape (a cylinder) and is
//! fine. Skipped when the book is not there (the crate as published has no `docs/`).

use std::path::Path;

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

/// Labels `[...]` of one diagram line that hold a bracket and are neither quoted nor a cylinder `[(...)]`.
fn bad_labels(line: &str) -> Vec<String> {
    let mut bad = Vec::new();
    let mut rest = line;
    while let Some(start) = rest.find('[') {
        let after = &rest[start + 1..];
        if after.starts_with('"') {
            let close = after[1..].find('"').map_or(after.len(), |i| i + 2);
            rest = &after[close..];
            continue;
        }
        if after.starts_with('(') {
            let close = after.find(")]").map_or(after.len(), |i| i + 2);
            rest = &after[close..];
            continue;
        }
        let Some(end) = after.find(']') else { break };
        let label = &after[..end];
        if label.contains('(') || label.contains(')') {
            bad.push(label.to_string());
        }
        rest = &after[end + 1..];
    }
    bad
}

#[test]
fn diagram_labels_with_brackets_are_quoted() {
    let docs = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/src");
    if !docs.exists() {
        return;
    }
    let mut files = Vec::new();
    pages(&docs, &mut files);
    let mut found = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        let mut inside = false;
        for (n, line) in text.lines().enumerate() {
            let t = line.trim();
            if t.starts_with("```mermaid") {
                inside = true;
            } else if inside && t.starts_with("```") {
                inside = false;
            } else if inside {
                for label in bad_labels(line) {
                    found.push(format!("{}:{}: [{}]", file.display(), n + 1, label));
                }
            }
        }
    }
    assert!(
        found.is_empty(),
        "labels with ( or ) must be quoted, [\"...\"]:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_check_tells_quoted_labels_and_cylinders_from_broken_ones() {
    assert_eq!(
        bad_labels(r#"B[start, MD (market)]"#),
        vec!["start, MD (market)"]
    );
    assert!(bad_labels(r#"B["start, MD (market)"] --> Q[(Channel)]"#).is_empty());
    assert!(bad_labels("A[plain] --> B[also plain]").is_empty());
}
