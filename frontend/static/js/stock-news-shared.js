/**
 * Shared stock news UI: FMP + Yahoo + Seeking Alpha headlines with source filter chips.
 */
(function(global) {
    'use strict';

    const NEWS_SOURCES = ['All', 'Seeking Alpha', 'Yahoo Finance', 'FMP'];
    const OTHER_SOURCES = ['Seeking Alpha', 'Yahoo Finance', 'FMP'];
    /** US equities app — display datetimes in Eastern (matches backend / market_brief). */
    const APP_TIMEZONE = 'America/New_York';

    function escAttr(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function sourceBadgeClass(source) {
        if (source === 'Seeking Alpha') return 'source-sa';
        if (source === 'Yahoo Finance') return 'source-yahoo';
        if (source === 'FMP') return 'source-fmp';
        return '';
    }

    /** Datetime in US Eastern (EDT/EST), regardless of browser timezone. */
    function formatAppDateTime(dateStr, opts) {
        if (!dateStr) return '';
        const options = Object.assign({
            month: 'short',
            day: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            timeZone: APP_TIMEZONE,
            timeZoneName: 'short',
        }, opts || {});
        if (opts && opts.year) {
            options.year = 'numeric';
        }
        return new Date(dateStr).toLocaleString('en-US', options);
    }

    function computeSourceCounts(articles) {
        const counts = {};
        for (const s of OTHER_SOURCES) {
            counts[s] = articles.filter(a => a.source === s).length;
        }
        return counts;
    }

    function createNewsPanel(config) {
        const {
            contentId,
            loadBtnId,
            filterBarId,
            getTicker,
            buttonLabels = {},
            showSnippet = true,
        } = config;
        const labels = {
            load: buttonLabels.load || 'Load News',
            loadRefresh: buttonLabels.loadRefresh || 'Refresh News',
        };

        let allArticles = [];
        let activeSource = 'All';

        function getContentEl() {
            return document.getElementById(contentId);
        }

        function getLoadBtn() {
            return loadBtnId ? document.getElementById(loadBtnId) : null;
        }

        function getFilterBarEl() {
            return filterBarId ? document.getElementById(filterBarId) : null;
        }

        function buildFilterChipsHtml(articles, sourceCounts) {
            const total = articles.length;
            return NEWS_SOURCES.map(s => {
                const count = s === 'All' ? total : (sourceCounts[s] || 0);
                const active = s === activeSource ? ' active' : '';
                const disabled = s !== 'All' && count === 0;
                if (disabled) return '';
                return `<button type="button" class="news-filter-chip${active}" data-news-source="${escAttr(s)}">${escAttr(s)} (${count})</button>`;
            }).join('');
        }

        function renderFilterChips(articles, sourceCounts) {
            const chipsHtml = buildFilterChipsHtml(articles, sourceCounts);
            const filterBar = getFilterBarEl();
            if (filterBar) {
                filterBar.innerHTML = chipsHtml;
                filterBar.hidden = !articles.length;
                return '';
            }
            if (!chipsHtml) return '';
            return '<div class="news-source-filters">' + chipsHtml + '</div>';
        }

        function renderNews(articles, sourceCounts) {
            const container = getContentEl();
            if (!container) return;

            const chipsPrefix = renderFilterChips(articles, sourceCounts);

            const filtered = activeSource === 'All'
                ? articles
                : articles.filter(a => a.source === activeSource);

            if (filtered.length === 0) {
                container.innerHTML = chipsPrefix + '<div class="news-empty">No articles from ' + escAttr(activeSource) + '.</div>';
                return;
            }

            const listHtml = '<div class="news-list">' + filtered.map(a => {
                const date = a.published_date
                    ? formatAppDateTime(a.published_date, { year: 'numeric' })
                    : '';
                const snippet = showSnippet && a.text ? escAttr(a.text.substring(0, 200)) : '';
                const badge = a.source
                    ? `<span class="news-source-badge ${sourceBadgeClass(a.source)}">${escAttr(a.source)}</span>`
                    : '';
                const title = escAttr(a.title || 'Untitled');
                const siteSpan = a.site && a.source !== a.site
                    ? `<span>${escAttr(a.site)}</span>`
                    : '';

                return `
                <div class="news-article">
                    <div class="news-article-body">
                        <div class="news-article-title"><a href="${escAttr(a.url || '#')}" target="_blank" rel="noopener">${title}</a></div>
                        <div class="news-article-meta">
                            ${badge}
                            ${siteSpan}
                            ${date ? `<span>${escAttr(date)}</span>` : ''}
                        </div>
                        ${snippet ? `<div class="news-article-snippet">${snippet}</div>` : ''}
                    </div>
                </div>
            `;
            }).join('') + '</div>';

            container.innerHTML = chipsPrefix + listHtml;
        }

        async function loadStockNews() {
            const ticker = getTicker();
            const btn = getLoadBtn();
            const container = getContentEl();
            if (!ticker || !btn || !container) return;

            btn.disabled = true;
            btn.classList.add('loading');
            btn.textContent = 'Loading';
            container.innerHTML = '<div class="detail-loading">Fetching news from FMP, Yahoo, Seeking Alpha…</div>';

            try {
                const newsRes = await fetch(`/api/frontend/stock-news/${ticker}?limit=40`);
                const data = await newsRes.json();

                if (data.error) {
                    container.innerHTML = '<div class="news-empty">Failed to load news.</div>';
                    return;
                }

                allArticles = data.articles || [];
                activeSource = 'All';
                renderNews(allArticles, computeSourceCounts(allArticles));
                btn.textContent = labels.loadRefresh;
            } catch (e) {
                console.error('Error loading news:', e);
                container.innerHTML = '<div class="news-empty">Error fetching news.</div>';
            } finally {
                btn.disabled = false;
                btn.classList.remove('loading');
            }
        }

        function reset() {
            allArticles = [];
            activeSource = 'All';
            const c = getContentEl();
            if (c) {
                c.innerHTML = '<div class="news-empty">Tap Load to fetch headlines.</div>';
            }
            const btn = getLoadBtn();
            if (btn) {
                btn.disabled = false;
                btn.classList.remove('loading');
                btn.textContent = labels.load;
            }
            const filterBar = getFilterBarEl();
            if (filterBar) {
                filterBar.innerHTML = '';
                filterBar.hidden = true;
            }
        }

        function handleNewsClick(e) {
            const chip = e.target.closest('.news-filter-chip[data-news-source]');
            if (chip) {
                e.preventDefault();
                activeSource = chip.getAttribute('data-news-source') || 'All';
                renderNews(allArticles, computeSourceCounts(allArticles));
            }
        }

        function bindContentClicks() {
            const container = getContentEl();
            if (container) container.addEventListener('click', handleNewsClick);
            const filterBar = getFilterBarEl();
            if (filterBar) filterBar.addEventListener('click', handleNewsClick);
        }

        function setup() {
            const loadBtn = getLoadBtn();
            if (loadBtn) {
                loadBtn.addEventListener('click', () => loadStockNews());
            }
            bindContentClicks();
            reset();
        }

        return {
            setup,
            reset,
            onTickerChange: reset,
            loadStockNews,
            renderNews,
            getArticles: () => allArticles,
            setActiveSource: (s) => { activeSource = s; },
        };
    }

    global.StockNewsShared = {
        createNewsPanel,
        formatAppDateTime,
        APP_TIMEZONE,
        sourceBadgeClass,
        computeSourceCounts,
        NEWS_SOURCES,
    };
})(window);
