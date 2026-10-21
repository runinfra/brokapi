// Draws the ```mermaid blocks in the colors of the book theme (brokapi, design "Specification": white page by default,
// dark "coal" as a variant). The library itself, mermaid.min.js (2.7 MB), lies next to this file: no request to
// other sites. It is fetched only on a page that has a diagram (the address is in theme/index.hbs, #mermaid-lib): the
// other pages of the book do not pay for it.
// `mdbook-mermaid install docs` rewrites this file with its stock version: after running it, restore this one
// from git (git checkout docs/mermaid-init.js).
(() => {
    if (!document.querySelector('pre.mermaid')) return;
    const lib = document.getElementById('mermaid-lib');
    if (!lib || !lib.dataset.src) return;
    const isDark = !document.documentElement.classList.contains('light');
    // Palette of the design "Specification": white page, ink-blue lines of the boxes, blue wires, the signal only for the
    // points that mean something (notes, activations). Dark: the same tones on the cold dark page.
    const light = {
        background: '#ffffff', primaryColor: '#ffffff', primaryTextColor: '#0b1220', primaryBorderColor: '#0b1220',
        secondaryColor: '#f2f4f7', tertiaryColor: '#ffffff', lineColor: '#1f3fe0', textColor: '#0b1220',
        mainBkg: '#ffffff', nodeBorder: '#0b1220', clusterBkg: '#f2f4f7', clusterBorder: '#cdd3db',
        edgeLabelBackground: '#ffffff', titleColor: '#0b1220', noteBkgColor: '#f2f4f7', noteTextColor: '#0b1220',
        nodeTextColor: '#0b1220', stateLabelColor: '#0b1220', noteBorderColor: '#566170', actorBkg: '#ffffff', actorBorder: '#0b1220', actorTextColor: '#0b1220',
        actorLineColor: '#93a0b4', signalColor: '#1f3fe0', signalTextColor: '#0b1220', labelBoxBkgColor: '#f2f4f7',
        labelBoxBorderColor: '#566170', labelTextColor: '#0b1220', loopTextColor: '#0b1220',
        activationBkgColor: '#dfe5ff', activationBorderColor: '#1f3fe0', sequenceNumberColor: '#ffffff',
        stateBkg: '#ffffff', altBackground: '#f2f4f7', compositeBackground: '#f2f4f7', labelColor: '#0b1220',
        transitionColor: '#1f3fe0', transitionLabelColor: '#0b1220', specialStateColor: '#ff5b1f',
    };
    const dark = {
        background: '#0e1523', primaryColor: '#111826', primaryTextColor: '#f3f6fb', primaryBorderColor: '#93a0b4',
        secondaryColor: '#111826', tertiaryColor: '#0e1523', lineColor: '#8fa8ff', textColor: '#e7ecf3',
        mainBkg: '#111826', nodeBorder: '#93a0b4', clusterBkg: '#0a0f1a', clusterBorder: '#243049',
        edgeLabelBackground: '#0e1523', titleColor: '#f3f6fb', noteBkgColor: '#141c2e', noteTextColor: '#f3f6fb',
        nodeTextColor: '#f3f6fb', stateLabelColor: '#f3f6fb', noteBorderColor: '#93a0b4', actorBkg: '#111826', actorBorder: '#93a0b4', actorTextColor: '#f3f6fb',
        actorLineColor: '#4a5875', signalColor: '#8fa8ff', signalTextColor: '#e7ecf3', labelBoxBkgColor: '#141c2e',
        labelBoxBorderColor: '#4a5875', labelTextColor: '#f3f6fb', loopTextColor: '#e7ecf3',
        activationBkgColor: '#26346b', activationBorderColor: '#8fa8ff', sequenceNumberColor: '#0a0f1a',
        stateBkg: '#111826', altBackground: '#0a0f1a', compositeBackground: '#0a0f1a', labelColor: '#f3f6fb',
        transitionColor: '#8fa8ff', transitionLabelColor: '#e7ecf3', specialStateColor: '#ff6a2b',
    };
    const setup = () => mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        themeVariables: Object.assign({ fontFamily: "'IBM Plex Sans', system-ui, sans-serif", fontSize: '14px' }, isDark ? dark : light),
    });
    // The library is fetched while the fonts load: the two waits go side by side.
    const library = () => new Promise((resolve, reject) => {
        const tag = document.createElement('script');
        tag.src = lib.dataset.src;
        tag.onload = resolve;
        tag.onerror = () => reject(new Error('mermaid.min.js did not load'));
        document.head.appendChild(tag);
    });

    // Wide diagrams: squeezed to the page they become unreadable on a phone (text around 7 px). If the natural width
    // is more than 1.4 of the column, draw at 80% of the natural width and let the block scroll sideways.
    const widen = () => {
        for (const svg of document.querySelectorAll('pre.mermaid svg')) {
            const box = svg.parentElement;
            const natural = parseFloat(svg.style.maxWidth) || (svg.viewBox && svg.viewBox.baseVal.width) || 0;
            if (!natural || natural <= box.clientWidth * 1.4 || box.dataset.wide) continue;
            box.dataset.wide = '1';
            svg.style.maxWidth = 'none';
            svg.style.width = Math.round(natural * 0.8) + 'px';
            box.style.overflowX = 'auto';
            const hint = document.createElement('div');
            hint.className = 'mermaid-scroll-hint';
            hint.textContent = 'Схема шире экрана: листайте вбок';
            box.insertAdjacentElement('afterend', hint);
            // Open at the left edge: a diagram is read from the left, and the hint under it says there is more to the right.
            // (Opened at the middle it was cut off on both sides and nothing said so.)
            box.scrollLeft = 0;
            box.classList.add('is-wide');
        }
    };
    // Mermaid sizes every box by measuring its label at drawing time. Drawn before IBM Plex Sans has loaded, the
    // boxes fit the fallback font, then the wider web font clips the last letters. So wait for the fonts first
    // (with a time limit, so a font that never loads does not keep the diagrams from drawing).
    const fontsReady = () => {
        if (!document.fonts) return Promise.resolve();
        // The font is split by alphabet (unicode-range): ask for Cyrillic and Latin text, or only one file loads.
        const loads = ["14px 'IBM Plex Sans'", "600 14px 'IBM Plex Sans'"]
            .map((f) => document.fonts.load(f, 'Аа Яя Aa Zz').catch(() => {}));
        const limit = new Promise((resolve) => setTimeout(resolve, 6000));
        return Promise.race([Promise.all(loads).then(() => document.fonts.ready), limit]);
    };
    const draw = () => Promise.all([library(), fontsReady()])
        .then(() => { setup(); return mermaid.run({ querySelector: 'pre.mermaid' }); })
        .then(widen)
        .catch(() => { /* the diagrams stay as text of the source: the page reads without them */ });
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', draw); else draw();

    // A change of the theme draws the diagrams again: the simplest way is to reload the page.
    for (const id of ['coal', 'light']) {
        const button = document.getElementById('mdbook-theme-' + id);
        if (!button) continue;
        button.addEventListener('click', () => {
            if ((id === 'light') === isDark) window.location.reload();
        });
    }
})();
