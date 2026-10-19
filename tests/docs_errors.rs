//! The texts of `brokapi::core::Error` are on the errors page of the book (docs/src/errors.md).
//!
//! The page tells a reader what each error means and what to do, and the reader finds it by the text the
//! error prints. If the text in the code changes and the page does not, this test fails.

use std::time::Duration;

use brokapi::core::{ConfigError, Error, TransportError};

/// Every fixed piece of the message (the parts around the variable fields) must be on the page.
fn assert_on_page(page: &str, error: Error, variable: &str) {
    let text = error.to_string();
    for piece in text.split(variable).filter(|p| !p.is_empty()) {
        assert!(
            page.contains(piece),
            "the errors page has no text {piece:?} (from {text:?})"
        );
    }
}

#[test]
fn error_texts_are_on_the_errors_page() {
    let page = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/docs/src/errors.md"))
        .expect("docs/src/errors.md");
    let v = "@@";
    assert_on_page(&page, Error::Config(ConfigError::Message(v.into())), v);
    assert_on_page(
        &page,
        Error::Transport(TransportError::Message(v.into())),
        v,
    );
    assert_on_page(
        &page,
        Error::Rejected {
            code: v.into(),
            message: v.into(),
        },
        v,
    );
    assert_on_page(&page, Error::Unsupported { capability: "@@" }, v);
    assert_on_page(&page, Error::Unknown, v);
    assert_on_page(&page, Error::NotReconciled, v);
    assert_on_page(&page, Error::Degraded, v);
    assert_on_page(&page, Error::ShuttingDown, v);
    // RateLimited prints a Duration, so its fixed head is checked by hand.
    let text = Error::RateLimited {
        retry_after: Duration::from_secs(1),
    }
    .to_string();
    assert!(text.starts_with("rate limited, retry after "));
    assert!(page.contains("rate limited, retry after "));
}
