// Small improvements of the book page that mdBook does not make. No libraries, no requests. Every part works on its own:
// when something here fails, the page still reads.
//   - tables: the cell knows its column (data-label), so on a phone a row becomes a card (CSS, theme/brokapi.css) and on a
//     wide screen the table that does not fit gets a sticky first column and a hint that it scrolls,
//   - everything that scrolls sideways (tables, code, wide diagrams) can be reached from the keyboard and has a name,
//   - a long code block is folded into <details> with a limited height,
//   - the copy button speaks Russian,
//   - the menu button of the contents works from the keyboard,
//   - the button "Сообщить об ошибке" opens a form that sends the report together with the page it was opened on,
//   - the column "На этой странице" at the right of a wide screen (from the headings of the chapter, with the current one marked),
//   - the cards "Назад" and "Дальше" at the foot of a chapter say when the page they lead to is in another part of the book.
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
  // A form in a <dialog>: the reader writes what is wrong and sends it, the address it goes to is not shown (decision of
  // 21.10.2026, context/brand.md). The page, its title and the text the reader selected before opening go along. The
  // receiver is POST /api/report on the same host (server side): JSON in, 2xx when taken, 429 when too often.
  var REPORT_URL = '/api/report';

  function report() {
    var buttons = document.querySelectorAll('.report-error');
    if (!buttons.length || typeof HTMLDialogElement !== 'function') return;
    var quote = '';
    var dialog = null;

    function build() {
      dialog = document.createElement('dialog');
      dialog.className = 'report-dialog';
      dialog.setAttribute('aria-labelledby', 'report-title');
      dialog.innerHTML =
        '<form class="report-form" novalidate>' +
        '<h2 id="report-title" class="report-title">Сообщить об ошибке</h2>' +
        '<p class="report-page"></p>' +
        '<blockquote class="report-quote" hidden></blockquote>' +
        '<label class="report-label" for="report-text">Что не так</label>' +
        '<textarea id="report-text" name="text" rows="5" maxlength="4000" required ' +
        'placeholder="Пример не собирается, ссылка ведет не туда, непонятно, что делать дальше"></textarea>' +
        '<label class="report-label" for="report-contact">Как вам ответить <span>необязательно</span></label>' +
        '<input id="report-contact" name="contact" type="text" maxlength="200" autocomplete="email" placeholder="Почта или Telegram">' +
        // A field for robots: people do not see it, a form filler fills it, and the receiver drops such a report.
        '<input class="report-trap" name="website" type="text" tabindex="-1" autocomplete="off" aria-hidden="true">' +
        '<p class="report-status" role="status" aria-live="polite"></p>' +
        '<div class="report-actions">' +
        '<button class="report-send" type="submit">Отправить</button>' +
        '<button class="report-cancel" type="button">Закрыть</button>' +
        '</div>' +
        '</form>';
      document.body.appendChild(dialog);
      var form = dialog.querySelector('form');
      dialog.querySelector('.report-cancel').addEventListener('click', function () { dialog.close(); });
      dialog.addEventListener('click', function (e) { if (e.target === dialog) dialog.close(); });
      form.addEventListener('submit', function (e) { e.preventDefault(); send(form); });
    }

    function open() {
      if (!dialog) build();
      var form = dialog.querySelector('form');
      var submit = form.querySelector('.report-send');
      form.reset();
      submit.disabled = false;
      submit.hidden = false;
      form.querySelector('.report-cancel').textContent = 'Закрыть';
      form.querySelector('.report-status').textContent = '';
      form.querySelector('.report-page').textContent = 'Страница: ' + document.title;
      var q = form.querySelector('.report-quote');
      q.textContent = quote;
      q.hidden = !quote;
      dialog.showModal();
      form.querySelector('textarea').focus();
    }

    function send(form) {
      var text = form.elements.text.value.trim();
      var status = form.querySelector('.report-status');
      if (!text) {
        status.textContent = 'Напишите пару слов о том, что не так.';
        form.elements.text.focus();
        return;
      }
      var button = form.querySelector('.report-send');
      button.disabled = true;
      status.textContent = 'Отправляем...';
      fetch(REPORT_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          page: window.location.href,
          title: document.title,
          quote: quote,
          text: text,
          contact: form.elements.contact.value.trim(),
          website: form.elements.website.value
        })
      }).then(function (r) {
        if (!r.ok) throw new Error(r.status === 429 ? 'often' : 'server');
        status.textContent = 'Спасибо, получили. Поправим.';
        button.hidden = true;
        var close = form.querySelector('.report-cancel');
        close.textContent = 'Готово';
        close.focus();
      }).catch(function (e) {
        button.disabled = false;
        status.textContent = e && e.message === 'often'
          ? 'Слишком много сообщений подряд. Попробуйте через несколько минут.'
          : 'Не отправилось. Текст остался в форме, попробуйте еще раз чуть позже.';
      });
    }

    Array.prototype.forEach.call(buttons, function (b) {
      // A click takes the selection away, so the selected text is read when the pointer goes down.
      b.addEventListener('pointerdown', function () { quote = selected(); });
      b.addEventListener('click', function (e) {
        if (e.detail === 0) quote = selected(); // from the keyboard
        open();
      });
      b.hidden = false;
    });
  }

  function selected() {
    var s = window.getSelection ? String(window.getSelection()).trim() : '';
    return s.length > 1000 ? s.slice(0, 1000) + '...' : s;
  }

  // ---- on this page ------------------------------------------------------------------------------------------------
  // The list of h2 and h3 of the chapter in the column at the right (theme/index.hbs has the empty <aside class="page-toc">).
  // The CSS shows it only on a wide screen and then hides the same list inside the menu at the left. The heading that has
  // passed the upper part of the window is the current one.
  function pageToc() {
    var aside = document.querySelector('.page-toc');
    var main = document.getElementById('main-content');
    if (!aside || !main) return;
    var heads = Array.prototype.filter.call(main.querySelectorAll('h2, h3'), function (h) { return h.id; });
    if (heads.length < 2) return;
    var title = document.createElement('b');
    title.textContent = 'На этой странице';
    aside.appendChild(title);
    var links = heads.map(function (h) {
      var a = document.createElement('a');
      a.href = '#' + h.id;
      a.textContent = h.textContent.trim();
      if (h.tagName === 'H3') a.className = 'h3';
      aside.appendChild(a);
      return a;
    });
    aside.hidden = false;
    document.documentElement.classList.add('has-page-toc');
    var ticking = false;
    function mark() {
      ticking = false;
      var at = 0;
      heads.forEach(function (h, i) { if (h.getBoundingClientRect().top <= 120) at = i; });
      links.forEach(function (a, i) {
        var on = i === at;
        a.classList.toggle('on', on);
        if (on) a.setAttribute('aria-current', 'location'); else a.removeAttribute('aria-current');
      });
    }
    window.addEventListener('scroll', function () {
      if (!ticking) { ticking = true; window.requestAnimationFrame(mark); }
    }, { passive: true });
    mark();
  }

  // ---- cards back and forward ----------------------------------------------------------------------------------------
  // mdBook knows only the title of the next page. When the next (or the previous) page belongs to another part of the book
  // (the parts are the bold words of the menu), the label of the card says so: "Следующий раздел: Rust".
  function pagerParts() {
    var cards = document.querySelectorAll('.pager a[rel]');
    if (!cards.length) return;
    function partOf(href) {
      var link = Array.prototype.filter.call(document.querySelectorAll('#mdbook-sidebar .chapter a'), function (a) { return a.href === href; })[0];
      if (!link) return null;
      var li = link;
      while (li && !(li.tagName === 'LI' && li.parentElement && li.parentElement.classList.contains('chapter'))) li = li.parentElement;
      // The title of a part is an <li class="part-title"> in the same list, before the chapters of the part.
      while (li) {
        if (li.classList.contains('part-title')) return li.textContent.trim();
        li = li.previousElementSibling;
      }
      return '';
    }
    var here = partOf(window.location.href.split('#')[0]);
    if (here === null) return;
    Array.prototype.forEach.call(cards, function (card) {
      var there = partOf(card.href);
      if (there === null || there === here || !there) return;
      var label = card.querySelector('.pager-label');
      if (label) label.textContent = (card.rel.indexOf('prev') >= 0 ? 'Предыдущий раздел: ' : 'Следующий раздел: ') + there;
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
    var steps = [tables, fold, copyButtons, menuButton, report, watchScroll, pageToc, pagerParts, tabStart];
    steps.forEach(function (step) {
      try { step(); } catch (e) { /* one part failing must not stop the others */ }
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
