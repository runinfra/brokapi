// Draws the ```mermaid blocks in the colors of the book theme (RunInfra: dark "navy" by default, "light" with a
// darker accent). The library itself, mermaid.min.js, lies next to this file: no request to other sites.
// `mdbook-mermaid install docs` rewrites this file with its stock version: after running it, restore this one
// from git (git checkout docs/mermaid-init.js).
(() => {
    const isDark = !document.documentElement.classList.contains('light');
    const dark = {
        background: '#0e131c', primaryColor: '#12202c', primaryTextColor: '#ffffff', primaryBorderColor: '#32d2ff',
        secondaryColor: '#101a27', tertiaryColor: '#0e131c', lineColor: '#7d9ab0', textColor: '#dfe4ee',
        mainBkg: '#12202c', nodeBorder: '#32d2ff', clusterBkg: '#0b121b', clusterBorder: '#2b3a4b',
        edgeLabelBackground: '#0e131c', titleColor: '#ffffff', noteBkgColor: '#16222f', noteTextColor: '#dfe4ee',
        nodeTextColor: '#ffffff', stateLabelColor: '#ffffff', noteBorderColor: '#32d2ff', actorBkg: '#12202c', actorBorder: '#32d2ff', actorTextColor: '#ffffff',
        actorLineColor: '#4a5d70', signalColor: '#dfe4ee', signalTextColor: '#dfe4ee', labelBoxBkgColor: '#12202c',
        labelBoxBorderColor: '#32d2ff', labelTextColor: '#ffffff', loopTextColor: '#dfe4ee',
        activationBkgColor: '#1d3a4c', activationBorderColor: '#32d2ff', sequenceNumberColor: '#080a0f',
        stateBkg: '#12202c', altBackground: '#0b121b', compositeBackground: '#0b121b', labelColor: '#ffffff',
        transitionColor: '#7d9ab0', transitionLabelColor: '#dfe4ee', specialStateColor: '#32d2ff',
    };
    const light = {
        background: '#f3f6fa', primaryColor: '#e3f4fa', primaryTextColor: '#080a0f', primaryBorderColor: '#0076a3',
        secondaryColor: '#eef3f8', tertiaryColor: '#f3f6fa', lineColor: '#4d5a6e', textColor: '#1c2433',
        mainBkg: '#e3f4fa', nodeBorder: '#0076a3', clusterBkg: '#f7f9fc', clusterBorder: '#b5bdca',
        edgeLabelBackground: '#f3f6fa', titleColor: '#080a0f', noteBkgColor: '#e3f4fa', noteTextColor: '#1c2433',
        nodeTextColor: '#080a0f', stateLabelColor: '#080a0f', noteBorderColor: '#0076a3', actorBkg: '#e3f4fa', actorBorder: '#0076a3', actorTextColor: '#080a0f',
        actorLineColor: '#98a2b3', signalColor: '#1c2433', signalTextColor: '#1c2433', labelBoxBkgColor: '#e3f4fa',
        labelBoxBorderColor: '#0076a3', labelTextColor: '#080a0f', loopTextColor: '#1c2433',
        activationBkgColor: '#bfe9f7', activationBorderColor: '#0076a3', sequenceNumberColor: '#ffffff',
        stateBkg: '#e3f4fa', altBackground: '#f7f9fc', compositeBackground: '#f7f9fc', labelColor: '#080a0f',
        transitionColor: '#4d5a6e', transitionLabelColor: '#1c2433', specialStateColor: '#0076a3',
    };
    mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        themeVariables: Object.assign({ fontFamily: "'Geist', system-ui, sans-serif", fontSize: '14px' }, isDark ? dark : light),
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
    const draw = () => mermaid.run({ querySelector: 'pre.mermaid' }).then(widen);
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', draw); else draw();

    // A change of the theme draws the diagrams again: the simplest way is to reload the page.
    for (const id of ['navy', 'light']) {
        const button = document.getElementById('mdbook-theme-' + id);
        if (!button) continue;
        button.addEventListener('click', () => {
            if ((id === 'light') === isDark) window.location.reload();
        });
    }
})();
