// Draws the ```mermaid blocks in the colors of the book theme (brokapi: light paper by default, dark "coal" as a
// variant, the same orange accent). The library itself, mermaid.min.js, lies next to this file: no request to other sites.
// `mdbook-mermaid install docs` rewrites this file with its stock version: after running it, restore this one
// from git (git checkout docs/mermaid-init.js).
(() => {
    const isDark = !document.documentElement.classList.contains('light');
    const light = {
        background: '#fbfaf7', primaryColor: '#ffffff', primaryTextColor: '#121212', primaryBorderColor: '#121212',
        secondaryColor: '#efede6', tertiaryColor: '#fbfaf7', lineColor: '#5c5a55', textColor: '#121212',
        mainBkg: '#ffffff', nodeBorder: '#121212', clusterBkg: '#f0eee8', clusterBorder: '#c8c5ba',
        edgeLabelBackground: '#fbfaf7', titleColor: '#121212', noteBkgColor: '#fff1ea', noteTextColor: '#121212',
        nodeTextColor: '#121212', stateLabelColor: '#121212', noteBorderColor: '#ff5b1f', actorBkg: '#ffffff', actorBorder: '#121212', actorTextColor: '#121212',
        actorLineColor: '#a19e94', signalColor: '#121212', signalTextColor: '#121212', labelBoxBkgColor: '#ffffff',
        labelBoxBorderColor: '#121212', labelTextColor: '#121212', loopTextColor: '#121212',
        activationBkgColor: '#ffd2bf', activationBorderColor: '#ff5b1f', sequenceNumberColor: '#ffffff',
        stateBkg: '#ffffff', altBackground: '#f0eee8', compositeBackground: '#f0eee8', labelColor: '#121212',
        transitionColor: '#5c5a55', transitionLabelColor: '#121212', specialStateColor: '#ff5b1f',
    };
    const dark = {
        background: '#1a1a17', primaryColor: '#26251f', primaryTextColor: '#f5f4ef', primaryBorderColor: '#8f8d86',
        secondaryColor: '#22211d', tertiaryColor: '#1a1a17', lineColor: '#9a978e', textColor: '#e7e5de',
        mainBkg: '#26251f', nodeBorder: '#8f8d86', clusterBkg: '#151513', clusterBorder: '#3a3935',
        edgeLabelBackground: '#1a1a17', titleColor: '#f5f4ef', noteBkgColor: '#33211a', noteTextColor: '#f5f4ef',
        nodeTextColor: '#f5f4ef', stateLabelColor: '#f5f4ef', noteBorderColor: '#ff5b1f', actorBkg: '#26251f', actorBorder: '#8f8d86', actorTextColor: '#f5f4ef',
        actorLineColor: '#5c5a55', signalColor: '#e7e5de', signalTextColor: '#e7e5de', labelBoxBkgColor: '#26251f',
        labelBoxBorderColor: '#8f8d86', labelTextColor: '#f5f4ef', loopTextColor: '#e7e5de',
        activationBkgColor: '#6b2a10', activationBorderColor: '#ff5b1f', sequenceNumberColor: '#121212',
        stateBkg: '#26251f', altBackground: '#151513', compositeBackground: '#151513', labelColor: '#f5f4ef',
        transitionColor: '#9a978e', transitionLabelColor: '#e7e5de', specialStateColor: '#ff5b1f',
    };
    mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        themeVariables: Object.assign({ fontFamily: "'Golos Text', system-ui, sans-serif", fontSize: '14px' }, isDark ? dark : light),
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
            hint.textContent = 'схема листается вбок';
            box.insertAdjacentElement('afterend', hint);
            // Open at the middle: the trunk of a top-down diagram is in the center, not at the left edge.
            box.scrollLeft = (box.scrollWidth - box.clientWidth) / 2;
        }
    };
    // Mermaid sizes every box by measuring its label at drawing time. Drawn before Golos Text has loaded, the
    // boxes fit the fallback font, then the wider web font clips the last letters. So wait for the fonts first
    // (with a time limit, so a font that never loads does not keep the diagrams from drawing).
    const fontsReady = () => {
        if (!document.fonts) return Promise.resolve();
        // The font is split by alphabet (unicode-range): ask for Cyrillic and Latin text, or only one file loads.
        const loads = ["14px 'Golos Text'", "600 14px 'Golos Text'"]
            .map((f) => document.fonts.load(f, 'Аа Яя Aa Zz').catch(() => {}));
        const limit = new Promise((resolve) => setTimeout(resolve, 6000));
        return Promise.race([Promise.all(loads).then(() => document.fonts.ready), limit]);
    };
    const draw = () => fontsReady().then(() => mermaid.run({ querySelector: 'pre.mermaid' })).then(widen);
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
