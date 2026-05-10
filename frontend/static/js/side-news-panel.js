/**
 * Side News Panel - Reusable collapsible news panel for screener pages
 * Provides load/render functionality that mirrors stock detail page
 */
(function() {
    'use strict';

    let _allNewsArticles = [];
    let _activeNewsSource = 'All';
    let _currentTicker = null;

    function escAttr(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

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

    function resetNewsPanel() {
        _allNewsArticles = [];
        _activeNewsSource = 'All';
        const c = document.getElementById('sideNewsContent');
        if (c) c.innerHTML = '<div class="news-empty">Click Load News to fetch headlines.</div>';
        const btn = document.getElementById('sideLoadNewsBtn');
        if (btn) {
            btn.disabled = false;
            btn.classList.remove('loading');
            btn.textContent = 'Load News';
        }
    }

    function computeSourceCounts(articles) {
        return {
            'FMP': articles.filter(a => a.source === 'FMP').length,
            'Yahoo Finance': articles.filter(a => a.source === 'Yahoo Finance').length,
            'Seeking Alpha': articles.filter(a => a.source === 'Seeking Alpha').length,
        };
    }

    function sourceBadgeClass(source) {
        if (source === 'Seeking Alpha') return 'source-sa';
        if (source === 'Yahoo Finance') return 'source-yahoo';
        if (source === 'FMP') return 'source-fmp';
        return '';
    }

    function renderNews(articles, sourceCounts) {
        const container = document.getElementById('sideNewsContent');
        if (!container) return;

        const sources = ['All', 'Seeking Alpha', 'Yahoo Finance', 'FMP'];
        const total = articles.length;

        const chipsHtml = '<div class="news-source-filters">' + sources.map(s => {
            const count = s === 'All' ? total : (sourceCounts[s] || 0);
            const active = s === _activeNewsSource ? ' active' : '';
            const disabled = s !== 'All' && count === 0;
            if (disabled) return '';
            return `<button type="button" class="news-filter-chip${active}" data-news-source="${escAttr(s)}">${escAttr(s)} (${count})</button>`;
        }).join('') + '</div>';

        const filtered = _activeNewsSource === 'All'
            ? articles
            : articles.filter(a => a.source === _activeNewsSource);

        if (filtered.length === 0) {
            container.innerHTML = chipsHtml + '<div class="news-empty">No articles from ' + escAttr(_activeNewsSource) + '.</div>';
            return;
        }

        const listHtml = '<div class="news-list">' + filtered.map(a => {
            const date = a.published_date ? new Date(a.published_date).toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric'}) : '';
            const thumb = a.image ? `<img class="news-article-thumb" src="${escAttr(a.image)}" alt="" onerror="this.style.display='none'">` : '';
            const snippet = a.text ? a.text.substring(0, 200) : '';
            const badge = a.source ? `<span class="news-source-badge ${sourceBadgeClass(a.source)}">${escAttr(a.source)}</span>` : '';
            const title = escAttr(a.title || 'Untitled');
            const url = a.url ? escAttr(a.url) : '#';
            const siteSpan = a.site && a.source !== a.site ? `<span>${escAttr(a.site)}</span>` : '';

            return `
            <div class="news-article">
                ${thumb}
                <div class="news-article-body">
                    <div class="news-article-title"><a href="${url}" target="_blank" rel="noopener">${title}</a></div>
                    <div class="news-article-meta">
                        ${badge}
                        ${siteSpan}
                        ${date ? `<span>${escAttr(date)}</span>` : ''}
                    </div>
                    ${snippet ? `<div class="news-article-snippet">${escAttr(snippet)}</div>` : ''}
                </div>
            </div>
        `;
        }).join('') + '</div>';

        container.innerHTML = chipsHtml + listHtml;
    }

    async function loadStockNews() {
        if (!_currentTicker) return;
        const btn = document.getElementById('sideLoadNewsBtn');
        const container = document.getElementById('sideNewsContent');
        if (!btn || !container) return;

        btn.disabled = true;
        btn.classList.add('loading');
        btn.textContent = 'Loading';
        container.innerHTML = '<div class="detail-loading">Fetching news from FMP, Yahoo, Seeking Alpha...</div>';

        try {
            const response = await fetch(`/api/frontend/stock-news/${_currentTicker}?limit=40`);
            const data = await response.json();

            if (data.error) {
                container.innerHTML = '<div class="news-empty">Failed to load news.</div>';
                return;
            }

            if (!data.articles || data.articles.length === 0) {
                container.innerHTML = '<div class="news-empty">No news found for this stock.</div>';
                return;
            }

            _allNewsArticles = data.articles;
            _activeNewsSource = 'All';
            renderNews(_allNewsArticles, data.source_counts || computeSourceCounts(_allNewsArticles));
            btn.textContent = 'Refresh News';
        } catch (error) {
            console.error('Error loading news:', error);
            container.innerHTML = '<div class="news-empty">Error fetching news.</div>';
        } finally {
            btn.disabled = false;
            btn.classList.remove('loading');
        }
    }

    function setupSideNewsPanel() {
        const newsStack = document.getElementById('sideNewsStack');
        const newsCollapseBtn = document.getElementById('sideNewsCollapseBtn');
        const loadBtn = document.getElementById('sideLoadNewsBtn');
        
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
        
        if (loadBtn) {
            loadBtn.addEventListener('click', () => loadStockNews());
        }

        const metricsStack = document.getElementById('sideMetricsStack');
        const metricsHead = document.getElementById('sideMetricsHead');
        bindCollapsibleHead(metricsStack, metricsHead);

        const newsInner = document.getElementById('sideNewsInner');
        if (newsInner) {
            newsInner.addEventListener('click', (e) => {
                const chip = e.target.closest('.news-filter-chip[data-news-source]');
                if (!chip) return;
                e.preventDefault();
                _activeNewsSource = chip.getAttribute('data-news-source') || 'All';
                renderNews(_allNewsArticles, computeSourceCounts(_allNewsArticles));
            });
        }

        resetNewsPanel();
    }

    function setNewsTicker(ticker) {
        _currentTicker = ticker;
        resetNewsPanel();
    }

    // Expose to global scope
    window.SideNewsPanel = {
        setup: setupSideNewsPanel,
        setTicker: setNewsTicker,
        reset: resetNewsPanel
    };
})();
