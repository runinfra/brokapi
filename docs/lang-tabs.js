// One choice of language, "Rust" or "Python", for the whole site. Three places follow it:
//   1. the switch in the menu bar (theme/index.hbs, .lang-switch),
//   2. the tabs "Rust | Python" of every code group on a page (the page wraps the group of fences in <div class="lang-tabs">,
//      see docs/README.md),
//   3. the pages that have a twin in the other language (rust/quickstart.html and python/quickstart.html): the switch
//      takes the reader there.
// The choice is kept in the browser (localStorage, key brokapi-lang). Without this script the fences of a group stand one
// under another with a caption of the language. No requests, no libraries.
(function () {
  'use strict';
  var KEY = 'brokapi-lang';
  var NAMES = { rust: 'Rust', python: 'Python', proto: 'gRPC', protobuf: 'gRPC', bash: 'Shell', sh: 'Shell', toml: 'TOML' };
  var CHOICES = ['rust', 'python'];

  // Pages of the book that exist in both languages: the same task, the same place in the book.
  var TWINS = {
    'rust/index.html': 'python/index.html',
    'rust/quickstart.html': 'python/quickstart.html',
    'rust/howto-trading-bot.html': 'python/howto-trading-bot.html',
    'rust/howto-infrastructure.html': 'python/howto-infrastructure.html',
    'rust/mocks.html': 'python/testing.html',
    'api.html': 'api-python.html'
  };
  var HOME = { rust: 'rust/index.html', python: 'python/index.html' };

  function langOf(pre) {
    var code = pre.querySelector('code');
    if (!code) return null;
    var m = /(?:^|\s)language-([\w-]+)/.exec(code.className);
    return m ? m[1] : null;
  }

  function stored() {
    try {
      var v = window.localStorage.getItem(KEY);
      return CHOICES.indexOf(v) >= 0 ? v : null;
    } catch (e) { return null; }
  }

  function store(lang) {
    try { window.localStorage.setItem(KEY, lang); } catch (e) { /* private window: the choice lasts for the page only */ }
  }

  // The path of the page inside the book: "rust/quickstart.html", "index.html".
  function pagePath() {
    var root = typeof path_to_root === 'string' ? new URL(path_to_root, window.location.href).pathname : '/';
    var rel = window.location.pathname.slice(root.length).replace(/(^|\/)$/, '$1index.html');
    return rel || 'index.html';
  }

  // The language the page itself is written in, or null for a common page.
  function pageLang(rel) {
    if (/^rust\//.test(rel) || rel === 'api.html') return 'rust';
    if (/^python\//.test(rel) || rel === 'api-python.html') return 'python';
    return null;
  }

  function twinOf(rel, to) {
    var from = pageLang(rel);
    if (!from || from === to) return null;
    var found = null;
    Object.keys(TWINS).forEach(function (r) {
      if (from === 'rust' && r === rel) found = TWINS[r];
      if (from === 'python' && TWINS[r] === rel) found = r;
    });
    return found || HOME[to];
  }

  function init() {
    var rel = pagePath();
    var own = pageLang(rel);
    var current = stored() || 'rust';
    var uid = 0;
    var groups = [];
    var solos = [];

    document.querySelectorAll('.lang-tabs').forEach(function (box) {
      var blocks = [];
      box.querySelectorAll('pre').forEach(function (pre) {
        var lang = langOf(pre);
        if (lang) blocks.push({ lang: lang, pre: pre });
      });
      if (blocks.length < 2) return;
      uid += 1;
      var bar = document.createElement('div');
      bar.className = 'lang-tabs-bar';
      bar.setAttribute('role', 'tablist');
      bar.setAttribute('aria-label', 'Язык примера');
      var seen = [];
      blocks.forEach(function (b) {
        if (seen.indexOf(b.lang) >= 0) return;
        seen.push(b.lang);
        var button = document.createElement('button');
        button.type = 'button';
        button.setAttribute('role', 'tab');
        button.id = 'lt' + uid + '-' + b.lang + '-tab';
        button.textContent = NAMES[b.lang] || b.lang;
        button.dataset.lang = b.lang;
        button.setAttribute('aria-controls', 'lt' + uid + '-' + b.lang);
        bar.appendChild(button);
      });
      blocks.forEach(function (b) {
        b.pre.id = 'lt' + uid + '-' + b.lang;
        b.pre.setAttribute('role', 'tabpanel');
        b.pre.setAttribute('aria-labelledby', b.pre.id + '-tab');
      });
      box.insertBefore(bar, box.firstChild);
      box.classList.add('is-tabbed');
      groups.push({ box: box, bar: bar, blocks: blocks, langs: seen });
    });

    // A fence outside the groups gets its language written above it (on the common pages the reader moves between the
    // languages, and a Rust example must not pass for the Python one). Pages of one language need no caption.
    if (!own) {
      document.querySelectorAll('main pre').forEach(function (pre) {
        if (pre.closest('.lang-tabs') || pre.classList.contains('mermaid')) return;
        var lang = langOf(pre);
        if (lang !== 'rust' && lang !== 'python') return;
        pre.classList.add('lang-solo');
        pre.dataset.langSolo = lang;
        solos.push(pre);
      });
    }

    function show(lang) {
      current = lang;
      groups.forEach(function (g) {
        // A group without this language keeps its first one.
        var use = g.langs.indexOf(lang) >= 0 ? lang : g.langs[0];
        g.blocks.forEach(function (b) { b.pre.hidden = b.lang !== use; });
        g.bar.querySelectorAll('button').forEach(function (button) {
          var on = button.dataset.lang === use;
          button.classList.toggle('is-on', on);
          button.setAttribute('aria-selected', on ? 'true' : 'false');
          button.tabIndex = on ? 0 : -1;
        });
      });
      solos.forEach(function (pre) {
        var mine = pre.dataset.langSolo;
        pre.dataset.langLabel = mine === 'rust' && lang === 'python' ? 'Rust (примера на Python для этого места нет)'
          : mine === 'python' && lang === 'rust' ? 'Python (примера на Rust для этого места нет)'
          : NAMES[mine];
      });
      document.querySelectorAll('.lang-switch button').forEach(function (button) {
        var on = button.dataset.lang === (own || lang);
        button.classList.toggle('is-on', on);
        button.setAttribute('aria-pressed', on ? 'true' : 'false');
      });
      document.dispatchEvent(new CustomEvent('brokapi:lang', { detail: lang }));
    }

    groups.forEach(function (g) {
      g.bar.addEventListener('click', function (event) {
        var lang = event.target && event.target.dataset && event.target.dataset.lang;
        if (!lang) return;
        store(lang);
        show(lang);
      });
    });

    // The switch in the menu bar.
    document.querySelectorAll('.lang-switch button').forEach(function (button) {
      button.addEventListener('click', function () {
        var lang = button.dataset.lang;
        store(lang);
        var go = twinOf(rel, lang);
        if (go && typeof path_to_root === 'string') {
          window.location.href = path_to_root + go;
          return;
        }
        show(lang);
      });
    });

    // Arrow keys inside a tab bar move between the tabs. mdBook turns the chapter with the same keys, so the event is
    // stopped before it reaches the document (a listener of the window in the capture phase runs first).
    window.addEventListener('keydown', function (event) {
      var tab = event.target && event.target.closest && event.target.closest('.lang-tabs-bar [role="tab"]');
      if (!tab) return;
      var keys = { ArrowLeft: -1, ArrowRight: 1, Home: 'first', End: 'last' };
      if (!(event.key in keys)) return;
      event.stopPropagation();
      event.preventDefault();
      var tabs = Array.prototype.slice.call(tab.parentElement.querySelectorAll('[role="tab"]'));
      var at = tabs.indexOf(tab);
      var to = keys[event.key] === 'first' ? 0 : keys[event.key] === 'last' ? tabs.length - 1
        : (at + keys[event.key] + tabs.length) % tabs.length;
      tabs[to].click();
      tabs[to].focus();
    }, true);

    show(current);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
