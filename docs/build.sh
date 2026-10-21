#!/usr/bin/env bash
# Builds the whole documentation site into docs/book. The site serves it at https://brokapi.ru/docs/ (the folder is the
# root of /docs/, the reference is /docs/api/); all links inside are relative, book.toml has site-url = "/docs/".
#   - the book (mdBook) at the root,
#   - the API reference (rustdoc, cargo doc --no-deps) in docs/book/api/<crate>/index.html,
#   - for AI agents: llms.txt (index), llms-full.txt (the whole book as one markdown file) and the markdown of every
#     page next to its HTML (page.html -> page.md), see docs/llms.py, and api-reference.md, the reference in text
#     (docs/apidigest.py).
#
# The build goes to a temporary folder first. docs/book is replaced only when everything succeeded,
# so a broken build leaves the previous version in place.
#
#   docs/build.sh
#
# Needs mdbook (and mdbook-mermaid), python3 (standard library only, for docs/llms.py) and a Rust toolchain.
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
# The site may be built in its own target directory: CARGO_TARGET_DIR is honoured by cargo doc below
# and by the copy of the result.
TARGET="${CARGO_TARGET_DIR:-target}"

NEW=docs/book.new
OLD=docs/book.old
rm -rf "$NEW"

echo "==> book"
mdbook build docs --dest-dir "$(pwd)/$NEW"

echo "==> files for AI agents (llms.txt, llms-full.txt, page.md)"
python3 docs/llms.py docs "$NEW"

echo "==> API reference"
# Start from an empty doc folder: pages of crates built earlier (or excluded now) must not leak into the site.
rm -rf "$TARGET/doc"
# brokapi-mocks is for tests and examples: not on the site. The root crate brokapi needs its broker features.
# The reference opens in the light theme in the brokapi colors (docs/rustdoc-brokapi.css).
RUSTDOCFLAGS="${RUSTDOCFLAGS:--D warnings --default-theme=light --extend-css $(pwd)/docs/rustdoc-brokapi.css}" cargo doc --no-deps --workspace --features brokapi/bcs,brokapi/tbank,brokapi/finam \
    --exclude brokapi-mocks
rm -rf "$NEW/api"
mkdir -p "$NEW/api"
cp -r "$TARGET/doc/." "$NEW/api/"
# rustdoc preloads its own fonts (Fira Sans, Source Serif, Source Code Pro), but the reference sets its own:
# take the preload out, so the browser does not download fonts nobody uses.
python3 docs/rustdoc_nofonts.py "$NEW/api"
# Fonts for the reference under stable names (docs/rustdoc-brokapi.css points at them as fonts/NAME, relative to api/theme.css).
mkdir -p "$NEW/api/fonts"
cp docs/theme/fonts/*.woff2 docs/theme/fonts/*-LICENSE.txt "$NEW/api/fonts/"

echo "==> API reference as one markdown file for agents"
python3 docs/apidigest.py "$NEW"

echo "==> replacing docs/book"
rm -rf "$OLD"
[ -d docs/book ] && mv docs/book "$OLD"
mv "$NEW" docs/book
rm -rf "$OLD"
echo "done: docs/book (book), docs/book/api (reference)"
