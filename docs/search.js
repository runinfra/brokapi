// Search over the book, in Russian: Pagefind (https://pagefind.app). The index is made after the build by docs/build.sh
// (`pagefind --site docs/book`) and lies in pagefind/ next to the pages. It is not loaded until the first search: the
// page itself weighs nothing extra. The window is built here, the markup of the button is in theme/index.hbs.
//   keys: "/" or "s" open the window, Esc closes it, the arrows move over the results, Enter opens one.
// mdBook's own search (elasticlunr) is switched off in book.toml: its index cut every non-ASCII letter, so Russian
// queries found nothing.
(function () {
  'use strict';
  var toggle = document.getElementById('brokapi-search-toggle');
  if (!toggle || typeof path_to_root === 'undefined') return;

  var root = null, input = null, list = null, status = null, pagefind = null, loading = null;
  var isOpen = false, lastFocus = null, timer = 0, serial = 0;

  // book.js asks window.search.hasFocus() before it turns the pages with the arrow keys.
  window.search = { hasFocus: function () { return isOpen; } };

  function base() { return new URL(path_to_root, window.location.href); }

  function load() {
    if (loading) return loading;
    var url = new URL('pagefind/pagefind.js', base()).href;
    loading = import(url).then(function (mod) {
      return Promise.resolve(mod.options({ baseUrl: base().pathname, excerptLength: 18 })).then(function () { return mod; });
    }).then(function (mod) { pagefind = mod; return mod; });
    loading.catch(function () { loading = null; });
    return loading;
  }

  function build() {
    root = document.createElement('div');
    root.id = 'brokapi-search';
    root.className = 'search-modal';
    root.hidden = true;
    root.innerHTML =
      '<div class="search-backdrop" data-close></div>' +
      '<div class="search-box" role="dialog" aria-modal="true" aria-label="Поиск по книге">' +
      '<form class="search-form" role="search" action="#" autocomplete="off">' +
      '<input class="search-input" type="search" placeholder="Что найти: заявка, лимиты, сверка, Unknown" ' +
      'aria-label="Запрос" spellcheck="false" autocomplete="off" autocapitalize="off" enterkeyhint="search">' +
      '<button class="search-close" type="button" data-close aria-label="Закрыть поиск">Esc</button>' +
      '</form>' +
      '<p class="search-status" role="status" aria-live="polite"></p>' +
      '<ul class="search-results"></ul>' +
      '<p class="search-hint">Стрелки: выбор, Enter: открыть, Esc: закрыть</p>' +
      '</div>';
    document.body.appendChild(root);
    input = root.querySelector('.search-input');
    list = root.querySelector('.search-results');
    status = root.querySelector('.search-status');
    root.addEventListener('click', function (e) {
      if (e.target && e.target.hasAttribute && e.target.hasAttribute('data-close')) close();
    });
    root.querySelector('form').addEventListener('submit', function (e) {
      e.preventDefault();
      var first = list.querySelector('a');
      if (first) first.click();
    });
    input.addEventListener('input', function () {
      window.clearTimeout(timer);
      timer = window.setTimeout(run, 120);
    });
    root.addEventListener('keydown', onKey);
  }

  function links() { return Array.prototype.slice.call(list.querySelectorAll('a')); }

  function onKey(e) {
    if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); close(); return; }
    if (e.key === 'Tab') {
      // Focus stays inside the window.
      var items = [input].concat(links(), [root.querySelector('.search-close')]);
      var at = items.indexOf(document.activeElement);
      var next = e.shiftKey ? at - 1 : at + 1;
      if (next < 0) next = items.length - 1;
      if (next >= items.length) next = 0;
      e.preventDefault();
      items[next].focus();
      return;
    }
    if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
    var all = links();
    if (!all.length) return;
    e.preventDefault();
    var i = all.indexOf(document.activeElement);
    if (e.key === 'ArrowDown') (all[i + 1] || all[0]).focus();
    else if (i <= 0) input.focus();
    else all[i - 1].focus();
  }

  function esc(text) {
    return String(text).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; });
  }

  function say(text) { status.textContent = text; }

  function plural(n, one, few, many) {
    var m10 = n % 10, m100 = n % 100;
    if (m10 === 1 && m100 !== 11) return one;
    if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return few;
    return many;
  }

  function card(data) {
    var title = (data.meta && data.meta.title) || data.url;
    var subs = (data.sub_results || []).filter(function (s) { return s.url !== data.url; }).slice(0, 2);
    var main = subs.length ? subs[0] : null;
    var html = '<li><a class="search-hit" href="' + esc(main ? main.url : data.url) + '">' +
      '<span class="search-hit-title">' + esc(title) + '</span>';
    if (main && main.title && main.title !== title) html += '<span class="search-hit-section">' + esc(main.title) + '</span>';
    html += '<span class="search-hit-text">' + (main ? main.excerpt : data.excerpt) + '</span></a>';
    if (subs.length > 1) {
      html += '<a class="search-hit-more" href="' + esc(subs[1].url) + '">Ещё в разделе: ' + esc(subs[1].title || title) + '</a>';
    }
    return html + '</li>';
  }

  function run() {
    var q = input.value.trim();
    var mine = ++serial;
    if (q.length < 2) { list.innerHTML = ''; say(''); return; }
    say('Ищу...');
    load().then(function (pf) {
      return pf.search(q);
    }).then(function (found) {
      if (mine !== serial) return null;
      var top = found.results.slice(0, 10);
      return Promise.all(top.map(function (r) { return r.data(); })).then(function (rows) {
        if (mine !== serial) return;
        if (!rows.length) {
          list.innerHTML = '';
          say('Ничего не найдено по запросу «' + q + '». Попробуйте другое слово или часть слова.');
          return;
        }
        list.innerHTML = rows.map(card).join('');
        var n = found.results.length;
        say('Найдено ' + n + ' ' + plural(n, 'страница', 'страницы', 'страниц') + (n > rows.length ? ', показаны первые ' + rows.length : ''));
      });
    }).catch(function () {
      if (mine !== serial) return;
      list.innerHTML = '';
      say('Поиск здесь недоступен: индекс строится при выкладке книги и не открывается из файла. Откройте книгу на сайте.');
    });
  }

  function open() {
    if (!root) build();
    if (isOpen) { input.focus(); return; }
    lastFocus = document.activeElement;
    isOpen = true;
    root.hidden = false;
    document.documentElement.classList.add('search-open');
    toggle.setAttribute('aria-expanded', 'true');
    input.focus();
    input.select();
    load().catch(function () {});
  }

  function close() {
    if (!isOpen) return;
    isOpen = false;
    root.hidden = true;
    document.documentElement.classList.remove('search-open');
    toggle.setAttribute('aria-expanded', 'false');
    if (lastFocus && lastFocus.focus) lastFocus.focus();
  }

  toggle.addEventListener('click', function () { if (isOpen) close(); else open(); });

  document.addEventListener('keydown', function (e) {
    if (isOpen || e.ctrlKey || e.metaKey || e.altKey) return;
    if (e.key !== '/' && e.key !== 's' && e.key !== 'S' && e.key !== 'ы' && e.key !== 'Ы') return;
    var t = e.target;
    if (t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName))) return;
    e.preventDefault();
    open();
  });
})();
