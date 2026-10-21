// Small improvements of the book page that mdBook does not make. No libraries, no requests. Every part works on its own:
// when something here fails, the page still reads.
//   - tables: the cell knows its column (data-label), so on a phone a row becomes a card (CSS, theme/brokapi.css) and on a
//     wide screen the table that does not fit gets a sticky first column and a hint that it scrolls,
//   - everything that scrolls sideways (tables, code, wide diagrams) can be reached from the keyboard and has a name,
//   - a long code block is folded into <details> with a limited height,
//   - the copy button speaks Russian,
//   - the menu button of the contents works from the keyboard,
//   - the link "Сообщить об ошибке" carries the page it was clicked on.
(function () {
  'use strict';

  var FOLD_AT = 45;      // lines: from here a code block gets a limited height
  var COLLAPSE_AT = 120; // lines: from here it is also closed until the reader opens it

  function each(selector, fn, root) {
    Array.prototype.forEach.call((root || document).querySelectorAll(selector), fn);
  }

  // ---- tables ------------------------------------------------------------------------------------------------------
  function tables() {
    each('main .table-wrapper', function (box) {
      var table = box.querySelector('table');
      if (!table) return;
      var head = table.querySelectorAll('thead th');
      var labels = Array.prototype.map.call(head, function (th) { return th.textContent.trim(); });
      each('thead th', function (th) {
        if (th.textContent.trim()) return;
        var hidden = document.createElement('span');
        hidden.className = 'sr-only';
        hidden.textContent = 'Параметр';
        th.appendChild(hidden);
      });
      table.setAttribute('role', 'table');
      each('thead, tbody', function (g) { g.setAttribute('role', 'rowgroup'); }, table);
      each('tr', function (tr) { tr.setAttribute('role', 'row'); }, table);
      each('th', function (th) { th.setAttribute('role', 'columnheader'); }, table);
      each('tbody tr', function (tr) {
        Array.prototype.forEach.call(tr.children, function (td, i) {
          td.setAttribute('role', 'cell');
          if (labels[i]) td.setAttribute('data-label', labels[i]);
        });
      });
      // Two columns read fine as a table on a phone: only the wider ones turn into cards.
      if (labels.length >= 3) box.classList.add('table-cards');
      var hint = document.createElement('div');
      hint.className = 'table-hint';
      hint.textContent = 'Таблица шире экрана: листайте вбок';
      box.insertAdjacentElement('afterend', hint);
    });
  }

  // ---- what scrolls sideways ---------------------------------------------------------------------------------------
  // An element that overflows gets a place in the tab order (without it a keyboard cannot scroll it) and, if it has
  // no role of its own, the role "region" with a name. A hint under a table says that it scrolls.
  var counter = { map: 0, table: 0, code: 0 };
  function labelOf(el) {
    // The name is unique on the page: several regions of one name confuse a screen reader.
    if (el.matches('pre.mermaid, .mermaid')) return 'Схема ' + (++counter.map) + ', листается вбок';
    if (el.matches('.table-wrapper')) return 'Таблица ' + (++counter.table) + ', листается вбок';
    return 'Код ' + (++counter.code) + ', листается вбок';
  }

  function checkScroll(el) {
    var over = el.scrollWidth > el.clientWidth + 1 && el.clientWidth > 0;
    var had = el.hasAttribute('data-scrolls');
    if (over === had) return;
    if (over) {
      el.setAttribute('data-scrolls', '');
      el.tabIndex = 0;
      if (!el.getAttribute('role')) { el.setAttribute('role', 'region'); el.setAttribute('aria-label', labelOf(el)); el.setAttribute('data-aria-set', ''); }
    } else {
      el.removeAttribute('data-scrolls');
      el.removeAttribute('tabindex');
      if (el.hasAttribute('data-aria-set')) { el.removeAttribute('role'); el.removeAttribute('aria-label'); el.removeAttribute('data-aria-set'); }
    }
    if (el.classList.contains('table-wrapper')) el.classList.toggle('is-scrollable', over);
  }

  function watchScroll() {
    var SELECTOR = 'main .table-wrapper, main pre, main pre > code, main .mermaid';
    var seen = [];
    var check = function () { seen.forEach(checkScroll); };
    var ro = window.ResizeObserver ? new ResizeObserver(function () { window.requestAnimationFrame(check); }) : null;
    var collect = function () {
      each(SELECTOR, function (el) {
        if (seen.indexOf(el) >= 0) return;
        seen.push(el);
        if (ro) ro.observe(el);
      });
      check();
    };
    collect();
    window.addEventListener('resize', check);
    document.addEventListener('brokapi:lang', function () { window.requestAnimationFrame(check); });
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(check);
    // A diagram is drawn after the page has loaded (and widens its box): look again a moment later.
    window.addEventListener('load', function () { window.setTimeout(collect, 600); });
  }

  // ---- long code ---------------------------------------------------------------------------------------------------
  function lineCount(pre) {
    var text = (pre.querySelector('code') || pre).textContent.replace(/\n+$/, '');
    return text ? text.split('\n').length : 0;
  }

  function fold() {
    each('main pre', function (pre) {
      if (pre.classList.contains('mermaid') || pre.closest('details.code-fold')) return;
      var n = lineCount(pre);
      if (n < FOLD_AT) return;
      var details = document.createElement('details');
      details.className = 'code-fold';
      if (n < COLLAPSE_AT) details.open = true;
      var summary = document.createElement('summary');
      var kind = (/language-([\w-]+)/.exec((pre.querySelector('code') || pre).className) || [])[1];
      summary.textContent = (kind ? 'Код' : 'Текст') + ', ' + n + ' ' + plural(n, 'строка', 'строки', 'строк') +
        (n < COLLAPSE_AT ? '' : ': показать');
      pre.parentNode.insertBefore(details, pre);
      details.appendChild(summary);
      details.appendChild(pre);
      pre.classList.add('is-long');
    });
  }

  function plural(n, one, few, many) {
    var m10 = n % 10, m100 = n % 100;
    if (m10 === 1 && m100 !== 11) return one;
    if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return few;
    return many;
  }

  // ---- copy button -------------------------------------------------------------------------------------------------
  function copyButtons() {
    each('pre > .buttons .clip-button', function (button) {
      button.title = 'Копировать код';
      button.setAttribute('aria-label', 'Копировать код');
      var tip = button.querySelector('.tooltiptext');
      if (!tip || !window.MutationObserver) return;
      var words = { 'Copied!': 'Скопировано', 'Clipboard error!': 'Не удалось скопировать' };
      new MutationObserver(function () {
        var now = tip.textContent;
        if (words[now]) tip.textContent = words[now];
      }).observe(tip, { childList: true, characterData: true, subtree: true });
    });
  }

  // ---- contents button -----------------------------------------------------------------------------------------------
  // The button is a real <button> (a <label> may not carry aria-expanded, which book.js sets): it flips the checkbox that
  // the CSS and book.js watch.
  function menuButton() {
    var button = document.getElementById('mdbook-sidebar-toggle');
    var box = document.getElementById('mdbook-sidebar-toggle-anchor');
    if (!button || !box) return;
    button.addEventListener('click', function () { box.click(); });
  }

  // ---- report an error ---------------------------------------------------------------------------------------------
  function report() {
    each('.report-error', function (a) {
      var subject = 'brokapi docs: ошибка на странице «' + document.title + '»';
      var body = 'Страница: ' + window.location.href + '\n\nЧто не так:\n';
      a.href = 'mailto:info@runinfra.ru?subject=' + encodeURIComponent(subject) + '&body=' + encodeURIComponent(body);
    });
  }

  // ---- the first Tab ---------------------------------------------------------------------------------------------------
  // toc.js scrolls the menu to the current chapter, and the browser takes that link as the point where Tab starts: the first
  // Tab then skips the link "К содержимому" and the buttons of the menu bar. Put the starting point back to the top.
  function tabStart() {
    if (window.location.hash) return;
    window.addEventListener('load', function () {
      window.setTimeout(function () {
        if (document.activeElement && document.activeElement !== document.body) return;
        document.body.tabIndex = -1;
        document.body.focus({ preventScroll: true });
        document.body.removeAttribute('tabindex');
      }, 400);
    });
  }

  function init() {
    var steps = [tables, fold, copyButtons, menuButton, report, watchScroll, tabStart];
    steps.forEach(function (step) {
      try { step(); } catch (e) { /* one part failing must not stop the others */ }
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
