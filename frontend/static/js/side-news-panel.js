/**
 * Side News Panel - Reusable collapsible news panel for screener pages
 */
(function() {
    'use strict';

    let _panel = null;
    let _currentTicker = null;

    function rafResize() {
        requestAnimationFrame(() => window.dispatchEvent(new Event('resize')));
    }

    function setPanelAria(stackEl, headEl) {
        if (!stackEl || !headEl) return;
        headEl.setAttribute('aria-expanded', String(!stackEl.classList.contains('collapsed')));
    }

    function bindCollapsibleHead(stackEl, headEl) {
        if (!stackEl || !headEl) return;
        const toggle = () => {
            stackEl.classList.toggle('collapsed');
            setPanelAria(stackEl, headEl);
            rafResize();
        };
        headEl.addEventListener('click', toggle);
        headEl.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                toggle();
            }
        });
    }

    function getTicker() {
        return _currentTicker;
    }

    function setupSideNewsPanel() {
        _panel = window.StockNewsShared.createNewsPanel({
            contentId: 'sideNewsContent',
            loadBtnId: 'sideLoadNewsBtn',
            benzingaBtnId: 'sideBenzingaNewsBtn',
            getTicker,
            showSnippet: false,
        });
        _panel.setup();

        const newsStack = document.getElementById('sideNewsStack');
        const newsCollapseBtn = document.getElementById('sideNewsCollapseBtn');
        if (newsStack && newsCollapseBtn) {
            const toggleNews = () => {
                newsStack.classList.toggle('collapsed');
                setPanelAria(newsStack, newsCollapseBtn);
                rafResize();
            };
            newsCollapseBtn.addEventListener('click', toggleNews);
            newsCollapseBtn.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    toggleNews();
                }
            });
        }

        const metricsStack = document.getElementById('sideMetricsStack');
        const metricsHead = document.getElementById('sideMetricsHead');
        bindCollapsibleHead(metricsStack, metricsHead);
    }

    function setNewsTicker(ticker) {
        _currentTicker = ticker;
        if (_panel) {
            _panel.reset();
            _panel.onTickerChange();
        }
    }

    function resetNewsPanel() {
        if (_panel) _panel.reset();
    }

    window.SideNewsPanel = {
        setup: setupSideNewsPanel,
        setTicker: setNewsTicker,
        reset: resetNewsPanel,
    };
})();
