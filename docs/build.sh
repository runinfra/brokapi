#!/usr/bin/env bash
# Builds the whole documentation site into docs/book. The site serves it at https://brokapi.ru/docs/ (the folder is the
# root of /docs/, the reference is /docs/api/); all links inside are relative, book.toml has site-url = "/docs/".
#   - the book (mdBook) at the root,
#   - the search index of the book (Pagefind, Russian) in docs/book/pagefind/, the window is docs/search.js,
#   - the API reference of Rust (rustdoc, cargo doc --no-deps) in docs/book/api/<crate>/index.html,
#   - the API reference of Python (pdoc, from the docstrings) in docs/book/api/python/, and python/api-reference.md,
#   - for AI agents: llms.txt (index), llms-full.txt (the whole book as one markdown file) and the markdown of every
#     page next to its HTML (page.html -> page.md), see docs/llms.py, and api-reference.md, the reference in text
#     (docs/apidigest.py).
#
# The build goes to a temporary folder first. docs/book is replaced only when everything succeeded,
# so a broken build leaves the previous version in place.
#
#   docs/build.sh
#
# Needs mdbook (and mdbook-mermaid), pagefind (or npx, which fetches it), python3 (docs/llms.py uses the standard library only, docs/pydoc.py needs pdoc, which the
# script installs into a virtualenv of its own on the first run: network on that run) and a Rust toolchain.
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
# The reference of the gRPC API (src/api-gateway.md) is made from crates/gateway/proto/: it must not be behind the files.
python3 docs/protodoc.py --check
mdbook build docs --dest-dir "$(pwd)/$NEW"
# The version in the strip of every page (theme/index.hbs) is put in here, from Cargo.toml (docs/brokapi_version.py).
python3 docs/brokapi_version.py stamp "$NEW"

echo "==> files for AI agents (llms.txt, llms-full.txt, page.md)"
python3 docs/llms.py docs "$NEW"

echo "==> search index (Pagefind, Russian)"
# The built-in search of mdBook is switched off (book.toml): its index cut every non-ASCII letter. Pagefind indexes the
# text of <main data-pagefind-body> of every chapter (theme/index.hbs), the diagrams are left out. It runs before the API
# reference is copied in: that reference has a search of its own. BROKAPI_NO_SEARCH=1 builds without the index (the window
# of docs/search.js then says that search is unavailable).
if [ "${BROKAPI_NO_SEARCH:-0}" != "1" ]; then
    if command -v pagefind >/dev/null 2>&1; then
        PAGEFIND=(pagefind)
    elif command -v npx >/dev/null 2>&1; then
        PAGEFIND=(npx --yes pagefind@1.5.2)
    else
        echo "error: pagefind is not installed (https://pagefind.app, version 1.5 or newer) and npx is not available" >&2
        echo "       install it, or build without search: BROKAPI_NO_SEARCH=1 docs/build.sh" >&2
        exit 1
    fi
    "${PAGEFIND[@]}" --site "$NEW" --force-language ru --exclude-selectors "pre.mermaid" --quiet
    # The window of docs/search.js is our own: the ready-made interfaces of Pagefind are not used, do not ship them.
    rm -f "$NEW"/pagefind/pagefind-*ui* "$NEW"/pagefind/pagefind-highlight.js
fi

echo "==> API reference"
# Start from an empty doc folder: pages of crates built earlier (or excluded now) must not leak into the site.
rm -rf "$TARGET/doc"
# brokapi-mocks is for tests and examples: not on the site. The root crate brokapi needs its broker features.
# The Bybit crates are not in the reference yet: the connector is not announced until it has passed a live run.
# The reference opens in the light theme in the brokapi colors (docs/rustdoc-brokapi.css), takes the theme of the book from
# localStorage (docs/reference/head.html) and has the thin bar of the book on top (docs/reference/bar.html).
RUSTDOCFLAGS="${RUSTDOCFLAGS:--D warnings --default-theme=light --extend-css $(pwd)/docs/rustdoc-brokapi.css --html-in-header $(pwd)/docs/reference/head.html --html-before-content $(pwd)/docs/reference/bar.html}" cargo doc --no-deps --workspace --features brokapi/bcs,brokapi/tbank,brokapi/finam,brokapi/alor \
    --exclude brokapi-mocks --exclude brokapi-bybit --exclude brokapi-bybit-protocol
rm -rf "$NEW/api"
mkdir -p "$NEW/api"
cp -r "$TARGET/doc/." "$NEW/api/"
# rustdoc preloads its own fonts (Fira Sans, Source Serif, Source Code Pro), but the reference sets its own:
# take the preload out, so the browser does not download fonts nobody uses.
python3 docs/rustdoc_nofonts.py "$NEW/api"
# Fonts for the reference under stable names (docs/rustdoc-brokapi.css points at them as fonts/NAME, relative to api/theme.css).
mkdir -p "$NEW/api/fonts"
cp docs/theme/fonts/*.woff2 docs/theme/fonts/*-LICENSE.txt "$NEW/api/fonts/"

echo "==> Python API reference (pdoc) in api/python/"
# pdoc reads the docstrings of the Python layer: the native extension is not needed (docs/pydoc.py puts a stand-in in its
# place), nothing is compiled. pdoc lives in a small virtualenv of its own, made on the first run and kept between builds.
PYDOC_VENV="${BROKAPI_PYDOC_VENV:-${XDG_CACHE_HOME:-$HOME/.cache}/brokapi-pydoc-venv}"
if ! "$PYDOC_VENV/bin/python" -c 'import pdoc' 2>/dev/null; then
    python3 -m venv "$PYDOC_VENV"
    "$PYDOC_VENV/bin/pip" install --quiet 'pdoc==16.0.0'
fi
"$PYDOC_VENV/bin/python" docs/pydoc.py "$NEW/api/python" "$NEW/python/api-reference.md"
# The fonts of the reference (docs/pdoc/custom.css points at /docs/api/python/fonts/).
mkdir -p "$NEW/api/python/fonts"
cp docs/theme/fonts/*.woff2 docs/theme/fonts/*-LICENSE.txt "$NEW/api/python/fonts/"

echo "==> API reference (Rust) as one markdown file for agents"
python3 docs/apidigest.py "$NEW"

echo "==> replacing docs/book"
rm -rf "$OLD"
[ -d docs/book ] && mv docs/book "$OLD"
mv "$NEW" docs/book
rm -rf "$OLD"
echo "done: docs/book (book), docs/book/api (reference)"
