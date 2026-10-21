// Tabs "Rust | Python" for code in several languages. The page marks a group of fences with <div class="lang-tabs">
// (see docs/README.md). Without this script the blocks stand one under another with a caption of the language.
// The chosen language is shared by all the groups of the page and is remembered in the browser. No requests, no libraries.
(function () {
  'use strict';
  var KEY = 'brokapi-lang';
  var NAMES = { rust: 'Rust', python: 'Python', proto: 'gRPC', protobuf: 'gRPC', bash: 'Shell', sh: 'Shell' };

  function langOf(pre) {
    var code = pre.querySelector('code');
    if (!code) return null;
    var m = /(?:^|\s)language-([\w-]+)/.exec(code.className);
    return m ? m[1] : null;
  }

  function stored() {
    try { return window.localStorage.getItem(KEY); } catch (e) { return null; }
  }

  function store(lang) {
    try { window.localStorage.setItem(KEY, lang); } catch (e) { /* private window: the choice lasts for the page only */ }
  }

  function init() {
    var groups = [];
    document.querySelectorAll('.lang-tabs').forEach(function (box) {
      var blocks = [];
      box.querySelectorAll('pre').forEach(function (pre) {
        var lang = langOf(pre);
        if (lang) blocks.push({ lang: lang, pre: pre });
      });
      if (blocks.length < 2) return;
      var bar = document.createElement('div');
      bar.className = 'lang-tabs-bar';
      bar.setAttribute('role', 'tablist');
      var seen = [];
      blocks.forEach(function (b) {
        if (seen.indexOf(b.lang) >= 0) return;
        seen.push(b.lang);
        var button = document.createElement('button');
        button.type = 'button';
        button.setAttribute('role', 'tab');
        button.textContent = NAMES[b.lang] || b.lang;
        button.dataset.lang = b.lang;
        bar.appendChild(button);
      });
      box.insertBefore(bar, box.firstChild);
      box.classList.add('is-tabbed');
      groups.push({ box: box, bar: bar, blocks: blocks, langs: seen });
    });
    if (!groups.length) return;

    function show(lang) {
      groups.forEach(function (g) {
        // A group without this language keeps its first one.
        var use = g.langs.indexOf(lang) >= 0 ? lang : g.langs[0];
        g.blocks.forEach(function (b) { b.pre.hidden = b.lang !== use; });
        g.bar.querySelectorAll('button').forEach(function (button) {
          var on = button.dataset.lang === use;
          button.classList.toggle('is-on', on);
          button.setAttribute('aria-selected', on ? 'true' : 'false');
        });
      });
    }

    groups.forEach(function (g) {
      g.bar.addEventListener('click', function (event) {
        var lang = event.target && event.target.dataset && event.target.dataset.lang;
        if (!lang) return;
        store(lang);
        show(lang);
      });
    });
    show(stored() || groups[0].langs[0]);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
