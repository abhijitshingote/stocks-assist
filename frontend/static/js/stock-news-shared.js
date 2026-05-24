/**
 * Shared stock news UI: source tabs, Benzinga DB/API, article modal.
 */
(function(global) {
    'use strict';

    const NEWS_SOURCES = ['All', 'Seeking Alpha', 'Yahoo Finance', 'FMP', 'Benzinga'];
    const OTHER_SOURCES = ['Seeking Alpha', 'Yahoo Finance', 'FMP'];

    let _modalReady = false;

    function escAttr(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function escHtml(s) {
        return escAttr(s).replace(/\n/g, '<br>');
    }

    function sourceBadgeClass(source) {
        if (source === 'Seeking Alpha') return 'source-sa';
        if (source === 'Yahoo Finance') return 'source-yahoo';
        if (source === 'FMP') return 'source-fmp';
        if (source === 'Benzinga') return 'source-benzinga';
        return '';
    }

    function computeSourceCounts(articles) {
        const counts = {};
        for (const s of OTHER_SOURCES.concat(['Benzinga'])) {
            counts[s] = articles.filter(a => a.source === s).length;
        }
        return counts;
    }

    function ensureModal() {
        if (_modalReady) return;
        const overlay = document.createElement('div');
        overlay.id = 'benzingaArticleModal';
        overlay.className = 'benzinga-modal-overlay';
        overlay.innerHTML = `
            <div class="benzinga-modal" role="dialog" aria-modal="true" aria-labelledby="benzingaModalTitle">
                <div class="benzinga-modal-header">
                    <h2 class="benzinga-modal-title" id="benzingaModalTitle"></h2>
                    <button type="button" class="benzinga-modal-close" aria-label="Close">&times;</button>
                </div>
                <div class="benzinga-modal-meta" id="benzingaModalMeta"></div>
                <div class="benzinga-modal-body" id="benzingaModalBody"></div>
                <div class="benzinga-modal-footer" id="benzingaModalFooter"></div>
            </div>
        `;
        document.body.appendChild(overlay);

        const close = () => overlay.classList.remove('open');
        overlay.querySelector('.benzinga-modal-close').addEventListener('click', close);
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && overlay.classList.contains('open')) close();
        });
        _modalReady = true;
    }

    function linkifyTickers(htmlEscaped, tickers) {
        if (!tickers || !tickers.length) return htmlEscaped;
        const unique = [...new Set(tickers.map(t => String(t).toUpperCase()))]
            .filter(t => t.length >= 1 && t.length <= 5)
            .sort((a, b) => b.length - a.length);
        let out = htmlEscaped;
        for (const t of unique) {
            const re = new RegExp('\\b(' + t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\b', 'g');
            out = out.replace(re, '<a href="/stock/$1" class="benzinga-ticker-tag">$1</a>');
        }
        return out;
    }

    function formatBenzingaReaderHtml(article) {
        const tickers = article.tickers || [];
        const teaser = (article.teaser || '').trim();
        const body = (article.body_text || article.text || '').trim();

        let html = '<div class="benzinga-reader-panel">';
        if (teaser) {
            html += '<div class="benzinga-reader-item benzinga-reader-teaser">'
                + linkifyTickers(escHtml(teaser), tickers) + '</div>';
        }
        const paragraphs = body.split(/\n\n+/).map(p => p.trim()).filter(Boolean);
        if (paragraphs.length === 0 && body && !teaser) {
            paragraphs.push(body);
        }
        for (const p of paragraphs) {
            html += '<div class="benzinga-reader-item">'
                + linkifyTickers(escHtml(p), tickers) + '</div>';
        }
        html += '</div>';
        return html;
    }

    function showBenzingaArticle(article) {
        ensureModal();
        const overlay = document.getElementById('benzingaArticleModal');
        const titleEl = document.getElementById('benzingaModalTitle');
        const metaEl = document.getElementById('benzingaModalMeta');
        const bodyEl = document.getElementById('benzingaModalBody');
        const footerEl = document.getElementById('benzingaModalFooter');

        const date = article.published_date
            ? new Date(article.published_date).toLocaleString('en-US', {
                month: 'short', day: 'numeric', year: 'numeric',
                hour: 'numeric', minute: '2-digit',
            })
            : '';
        const metaParts = [];
        if (article.author) metaParts.push('By ' + article.author);
        if (date) metaParts.push(date);
        if (article.channels && article.channels.length) {
            metaParts.push(article.channels.slice(0, 5).join(', '));
        }

        titleEl.textContent = article.title || 'Untitled';
        metaEl.textContent = metaParts.join(' · ');
        bodyEl.innerHTML = formatBenzingaReaderHtml(article);

        if (article.url) {
            footerEl.innerHTML = '<a href="' + escAttr(article.url) + '" target="_blank" rel="noopener">Open on Benzinga ↗</a>';
        } else {
            footerEl.innerHTML = '';
        }

        overlay.classList.add('open');
    }

    function mergeBenzingaArticles(allArticles, benzingaArticles) {
        const withoutBz = allArticles.filter(a => a.source !== 'Benzinga');
        return withoutBz.concat(benzingaArticles || []);
    }

    function createNewsPanel(config) {
        const {
            contentId,
            loadBtnId,
            benzingaBtnId,
            getTicker,
        } = config;

        let allArticles = [];
        let activeSource = 'All';

        function getContentEl() {
            return document.getElementById(contentId);
        }

        function getLoadBtn() {
            return loadBtnId ? document.getElementById(loadBtnId) : null;
        }

        function getBenzingaBtn() {
            return benzingaBtnId ? document.getElementById(benzingaBtnId) : null;
        }

        function renderNews(articles, sourceCounts) {
            const container = getContentEl();
            if (!container) return;

            const total = articles.length;
            const chipsHtml = '<div class="news-source-filters">' + NEWS_SOURCES.map(s => {
                const count = s === 'All' ? total : (sourceCounts[s] || 0);
                const active = s === activeSource ? ' active' : '';
                const disabled = s !== 'All' && count === 0;
                if (disabled) return '';
                return `<button type="button" class="news-filter-chip${active}" data-news-source="${escAttr(s)}">${escAttr(s)} (${count})</button>`;
            }).join('') + '</div>';

            const filtered = activeSource === 'All'
                ? articles
                : articles.filter(a => a.source === activeSource);

            if (filtered.length === 0) {
                const hint = activeSource === 'Benzinga'
                    ? 'Click Get Benzinga News to fetch articles.'
                    : 'No articles from ' + escAttr(activeSource) + '.';
                container.innerHTML = chipsHtml + '<div class="news-empty">' + hint + '</div>';
                return;
            }

            const listHtml = '<div class="news-list">' + filtered.map(a => {
                const date = a.published_date
                    ? new Date(a.published_date).toLocaleDateString('en-US', {
                        month: 'short', day: 'numeric', year: 'numeric',
                    })
                    : '';
                const thumb = a.image
                    ? `<img class="news-article-thumb" src="${escAttr(a.image)}" alt="" onerror="this.style.display='none'">`
                    : '';
                const snippet = a.text ? escAttr(a.text.substring(0, 200)) : '';
                const badge = a.source
                    ? `<span class="news-source-badge ${sourceBadgeClass(a.source)}">${escAttr(a.source)}</span>`
                    : '';
                const title = escAttr(a.title || 'Untitled');
                const isBenzinga = a.source === 'Benzinga';
                const titleHtml = isBenzinga
                    ? `<span class="news-read-inline" data-benzinga-id="${escAttr(a.benzinga_id)}">${title}</span>`
                    : `<a href="${escAttr(a.url || '#')}" target="_blank" rel="noopener">${title}</a>`;
                const siteSpan = a.site && a.source !== a.site
                    ? `<span>${escAttr(a.site)}</span>`
                    : '';

                return `
                <div class="news-article" ${isBenzinga ? `data-benzinga-id="${escAttr(a.benzinga_id)}"` : ''}>
                    ${thumb}
                    <div class="news-article-body">
                        <div class="news-article-title">${titleHtml}</div>
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

            container.innerHTML = chipsHtml + listHtml;
        }

        async function loadCachedBenzinga() {
            const ticker = getTicker();
            if (!ticker) return [];
            try {
                const response = await fetch(`/api/frontend/benzinga-news/${ticker}?limit=40`);
                const data = await response.json();
                if (data.error || !data.articles) return [];
                return data.articles;
            } catch (e) {
                console.error('Benzinga cache load failed:', e);
                return [];
            }
        }

        async function refreshBenzinga() {
            const ticker = getTicker();
            const btn = getBenzingaBtn();
            const container = getContentEl();
            if (!ticker || !btn || !container) return;

            btn.disabled = true;
            btn.classList.add('loading');
            const prevText = btn.textContent;
            btn.textContent = 'Loading…';

            try {
                const response = await fetch(`/api/frontend/benzinga-news/${ticker}?limit=40`, {
                    method: 'POST',
                });
                const data = await response.json();
                if (data.error) {
                    if (activeSource === 'Benzinga') {
                        container.innerHTML = '<div class="news-empty">Failed to fetch Benzinga news.</div>';
                    }
                    return;
                }
                allArticles = mergeBenzingaArticles(allArticles, data.articles || []);
                activeSource = 'Benzinga';
                renderNews(allArticles, computeSourceCounts(allArticles));
                btn.textContent = 'Refresh Benzinga';
            } catch (e) {
                console.error('Benzinga refresh failed:', e);
                if (activeSource === 'Benzinga') {
                    container.innerHTML = '<div class="news-empty">Error fetching Benzinga news.</div>';
                }
            } finally {
                btn.disabled = false;
                btn.classList.remove('loading');
                if (!btn.textContent.includes('Refresh')) {
                    btn.textContent = prevText;
                }
            }
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
                const [newsRes, benzingaCached] = await Promise.all([
                    fetch(`/api/frontend/stock-news/${ticker}?limit=40`),
                    loadCachedBenzinga(),
                ]);
                const data = await newsRes.json();

                if (data.error) {
                    container.innerHTML = '<div class="news-empty">Failed to load news.</div>';
                    return;
                }

                const base = data.articles || [];
                allArticles = mergeBenzingaArticles(base, benzingaCached);
                activeSource = 'All';
                renderNews(allArticles, computeSourceCounts(allArticles));
                btn.textContent = 'Refresh News';
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
                c.innerHTML = '<div class="news-empty">Click Load News to fetch headlines.</div>';
            }
            const btn = getLoadBtn();
            if (btn) {
                btn.disabled = false;
                btn.classList.remove('loading');
                btn.textContent = 'Load News';
            }
            const bzBtn = getBenzingaBtn();
            if (bzBtn) {
                bzBtn.disabled = false;
                bzBtn.classList.remove('loading');
                bzBtn.textContent = 'Get Benzinga News';
            }
        }

        async function onTickerChange() {
            reset();
            const cached = await loadCachedBenzinga();
            if (cached.length) {
                allArticles = mergeBenzingaArticles([], cached);
                renderNews(allArticles, computeSourceCounts(allArticles));
            }
        }

        function bindContentClicks() {
            const container = getContentEl();
            if (!container) return;

            container.addEventListener('click', (e) => {
                const chip = e.target.closest('.news-filter-chip[data-news-source]');
                if (chip) {
                    e.preventDefault();
                    activeSource = chip.getAttribute('data-news-source') || 'All';
                    renderNews(allArticles, computeSourceCounts(allArticles));
                    if (activeSource === 'Benzinga' && !allArticles.some(a => a.source === 'Benzinga')) {
                        loadCachedBenzinga().then((cached) => {
                            if (cached.length) {
                                allArticles = mergeBenzingaArticles(allArticles, cached);
                                renderNews(allArticles, computeSourceCounts(allArticles));
                            }
                        });
                    }
                    return;
                }

                const readLink = e.target.closest('.news-read-inline[data-benzinga-id]');
                if (readLink) {
                    e.preventDefault();
                    const id = readLink.getAttribute('data-benzinga-id');
                    const article = allArticles.find(
                        a => a.source === 'Benzinga' && String(a.benzinga_id) === String(id)
                    );
                    if (article) showBenzingaArticle(article);
                }
            });
        }

        function setup() {
            const loadBtn = getLoadBtn();
            if (loadBtn) {
                loadBtn.addEventListener('click', () => loadStockNews());
            }
            const bzBtn = getBenzingaBtn();
            if (bzBtn) {
                bzBtn.addEventListener('click', () => refreshBenzinga());
            }
            bindContentClicks();
            reset();
        }

        return {
            setup,
            reset,
            onTickerChange,
            loadStockNews,
            refreshBenzinga,
            showBenzingaArticle,
            renderNews,
            getArticles: () => allArticles,
            setActiveSource: (s) => { activeSource = s; },
        };
    }

    global.StockNewsShared = {
        createNewsPanel,
        showBenzingaArticle,
        sourceBadgeClass,
        computeSourceCounts,
        NEWS_SOURCES,
    };
})(window);
