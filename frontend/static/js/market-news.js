/**
 * Market News Page - Card-based layout matching stock detail page
 */
(function() {
    'use strict';

    // State
    let activeSource = 'benzinga';
    let benzingaArticles = [];
    let fmpArticles = [];
    let seekingAlphaArticles = [];
    let benzingaChannels = [];
    let benzingaActiveChannel = '';

    // Elements
    const tabs = document.querySelectorAll('.news-tab');
    const sections = document.querySelectorAll('.news-source-section');
    
    // Benzinga
    const benzingaSection = document.getElementById('benzingaSection');
    const benzingaContent = document.getElementById('benzingaContent');
    const benzingaChannelsEl = document.getElementById('benzingaChannels');
    const benzingaWindow = document.getElementById('benzingaWindow');
    const benzingaRefresh = document.getElementById('benzingaRefresh');
    const benzingaCountEl = document.getElementById('benzingaCount');
    
    // FMP
    const fmpSection = document.getElementById('fmpSection');
    const fmpContent = document.getElementById('fmpContent');
    const fmpRefresh = document.getElementById('fmpRefresh');
    const fmpCountEl = document.getElementById('fmpCount');
    
    // Seeking Alpha
    const seekingAlphaSection = document.getElementById('seekingAlphaSection');
    const seekingAlphaContent = document.getElementById('seekingAlphaContent');
    const seekingAlphaRefresh = document.getElementById('seekingAlphaRefresh');
    const seekingAlphaCountEl = document.getElementById('seekingAlphaCount');

    // Utilities
    function escAttr(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function escHtml(s) {
        return escAttr(s);
    }

    function formatChannelLabel(slug) {
        return String(slug).replace(/_/g, ' ');
    }

    function formatDate(dateStr) {
        if (window.StockNewsShared && window.StockNewsShared.formatAppDateTime) {
            return window.StockNewsShared.formatAppDateTime(dateStr);
        }
        if (!dateStr) return '';
        return new Date(dateStr).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            timeZone: 'America/New_York',
            timeZoneName: 'short',
        });
    }

    function formatBenzingaDate(dateStr) {
        if (window.StockNewsShared && window.StockNewsShared.formatBenzingaDate) {
            return window.StockNewsShared.formatBenzingaDate(dateStr);
        }
        return formatDate(dateStr);
    }

    function getSourceBadgeClass(source) {
        if (source === 'Seeking Alpha') return 'source-sa';
        if (source === 'Yahoo Finance') return 'source-yahoo';
        if (source === 'FMP') return 'source-fmp';
        if (source === 'Benzinga') return 'source-benzinga';
        return '';
    }

    // Tab Switching
    function switchToTab(source) {
        activeSource = source;
        
        tabs.forEach(tab => {
            if (tab.getAttribute('data-source') === source) {
                tab.classList.add('active');
            } else {
                tab.classList.remove('active');
            }
        });
        
        sections.forEach(section => {
            section.classList.remove('active');
        });
        
        if (source === 'benzinga') {
            benzingaSection.classList.add('active');
        } else if (source === 'fmp') {
            fmpSection.classList.add('active');
        } else if (source === 'seeking-alpha') {
            seekingAlphaSection.classList.add('active');
        }
    }

    // Benzinga Functions
    function renderBenzingaChannelChips() {
        if (!benzingaChannelsEl) return;
        const chips = ['<button type="button" class="news-filter-chip' +
            (benzingaActiveChannel === '' ? ' active' : '') +
            '" data-channel="">All</button>'];
        for (const ch of benzingaChannels) {
            const active = ch === benzingaActiveChannel ? ' active' : '';
            chips.push(
                '<button type="button" class="news-filter-chip' + active +
                '" data-channel="' + escAttr(ch) + '">' +
                escHtml(formatChannelLabel(ch)) + '</button>'
            );
        }
        benzingaChannelsEl.innerHTML = chips.join('');
    }

    function renderBenzingaArticles() {
        if (!benzingaContent) return;

        if (benzingaArticles.length === 0) {
            const hint = benzingaActiveChannel
                ? 'No articles for channel "' + escHtml(formatChannelLabel(benzingaActiveChannel)) + '"'
                : 'No Benzinga articles available';
            benzingaContent.innerHTML = '<div class="news-empty">' + hint + '</div>';
            if (benzingaCountEl) benzingaCountEl.textContent = '0';
            return;
        }

        const listHtml = '<div class="news-list">' + benzingaArticles.map(a => {
            const date = formatBenzingaDate(a.published_date);
            const snippet = a.teaser || a.text || '';
            const channels = (a.channels || []).slice(0, 2).map(c =>
                '<span class="news-channel-tag">' + escHtml(c) + '</span>'
            ).join(' ');
            const tickers = (a.tickers || []).slice(0, 5).map(t => {
                const sym = String(t).replace(/^X:/i, '').toUpperCase();
                return '<a href="/stock/' + escAttr(sym) + '" class="news-ticker-tag" ' +
                    'onclick="event.stopPropagation()">' + escHtml(sym) + '</a>';
            }).join(' ');

            return (
                '<div class="news-article" data-benzinga-id="' + escAttr(a.benzinga_id) + '" ' +
                'role="button" tabindex="0">' +
                '<div class="news-article-body">' +
                '<div class="news-article-title">' + escHtml(a.title || 'Untitled') + '</div>' +
                '<div class="news-article-meta">' +
                '<span class="news-source-badge source-benzinga">BENZINGA</span>' +
                (date ? '<span>' + escHtml(date) + '</span>' : '') +
                channels +
                tickers +
                '</div>' +
                (snippet ? '<div class="news-article-snippet">' + escHtml(snippet) + '</div>' : '') +
                '</div>' +
                '</div>'
            );
        }).join('') + '</div>';

        benzingaContent.innerHTML = listHtml;
        if (benzingaCountEl) {
            benzingaCountEl.textContent = benzingaArticles.length;
        }
    }

    async function loadBenzingaNews(opts) {
        const fromApi = !!(opts && opts.fromApi);
        const originalLabel = benzingaRefresh ? benzingaRefresh.textContent : '';
        if (benzingaRefresh) {
            benzingaRefresh.disabled = true;
            benzingaRefresh.classList.add('loading');
            benzingaRefresh.textContent = fromApi ? 'Fetching from API…' : 'Loading…';
        }
        if (benzingaContent && benzingaArticles.length === 0) {
            benzingaContent.innerHTML = '<div class="news-empty">' +
                (fromApi ? 'Fetching fresh articles from Benzinga API…' : 'Loading…') +
                '</div>';
        }

        try {
            const qs = new URLSearchParams({ limit: '200' });
            if (benzingaActiveChannel) qs.set('channel', benzingaActiveChannel);
            const url = '/api/frontend/benzinga-news/market?' + qs.toString();
            const response = await fetch(url, { method: fromApi ? 'POST' : 'GET' });
            const data = await response.json();
            if (data.error) {
                if (benzingaContent) {
                    benzingaContent.innerHTML = '<div class="news-empty">Failed to load: ' +
                        escHtml(data.error) + '</div>';
                }
                return;
            }
            benzingaArticles = data.articles || [];
            if (data.channels_available && data.channels_available.length) {
                benzingaChannels = data.channels_available;
            }
            if (benzingaWindow && data.window) {
                const label = data.window.label || '';
                benzingaWindow.textContent = label;
            }
            renderBenzingaChannelChips();
            renderBenzingaArticles();
        } catch (e) {
            console.error('Benzinga news load failed:', e);
            if (benzingaContent) {
                benzingaContent.innerHTML = '<div class="news-empty">Error loading Benzinga news.</div>';
            }
        } finally {
            if (benzingaRefresh) {
                benzingaRefresh.disabled = false;
                benzingaRefresh.classList.remove('loading');
                benzingaRefresh.textContent = originalLabel || 'Refresh from API';
            }
        }
    }

    // FMP Functions
    function renderFmpArticles() {
        if (!fmpContent) return;

        if (fmpArticles.length === 0) {
            fmpContent.innerHTML = '<div class="news-empty">No FMP articles available</div>';
            if (fmpCountEl) fmpCountEl.textContent = '0';
            return;
        }

        const listHtml = '<div class="news-list">' + fmpArticles.map(a => {
            const date = formatDate(a.published_date);
            const snippet = a.text || '';

            return (
                '<div class="news-article" data-article-url="' + escAttr(a.url) + '" ' +
                'role="button" tabindex="0">' +
                '<div class="news-article-body">' +
                '<div class="news-article-title">' + escHtml(a.title || 'Untitled') + '</div>' +
                '<div class="news-article-meta">' +
                '<span class="news-source-badge source-fmp">FMP</span>' +
                (date ? '<span>' + escHtml(date) + '</span>' : '') +
                (a.site ? '<span class="news-channel-tag">' + escHtml(a.site) + '</span>' : '') +
                '</div>' +
                (snippet ? '<div class="news-article-snippet">' + escHtml(snippet) + '</div>' : '') +
                '</div>' +
                '</div>'
            );
        }).join('') + '</div>';

        fmpContent.innerHTML = listHtml;
        if (fmpCountEl) {
            fmpCountEl.textContent = fmpArticles.length;
        }
    }

    async function loadFmpNews() {
        if (fmpRefresh) {
            fmpRefresh.disabled = true;
            fmpRefresh.classList.add('loading');
            fmpRefresh.textContent = 'Loading…';
        }
        if (fmpContent) {
            fmpContent.innerHTML = '<div class="news-empty">Loading…</div>';
        }

        try {
            const response = await fetch('/api/frontend/market-news/fmp?limit=100');
            const data = await response.json();
            if (data.error) {
                if (fmpContent) {
                    fmpContent.innerHTML = '<div class="news-empty">Failed to load: ' +
                        escHtml(data.error) + '</div>';
                }
                return;
            }
            fmpArticles = data.articles || [];
            renderFmpArticles();
        } catch (e) {
            console.error('FMP news load failed:', e);
            if (fmpContent) {
                fmpContent.innerHTML = '<div class="news-empty">Error loading FMP news</div>';
            }
        } finally {
            if (fmpRefresh) {
                fmpRefresh.disabled = false;
                fmpRefresh.classList.remove('loading');
                fmpRefresh.textContent = 'Refresh';
            }
        }
    }

    // Seeking Alpha Functions
    function renderSeekingAlphaArticles() {
        if (!seekingAlphaContent) return;

        if (seekingAlphaArticles.length === 0) {
            seekingAlphaContent.innerHTML = '<div class="news-empty">No Seeking Alpha articles available</div>';
            if (seekingAlphaCountEl) seekingAlphaCountEl.textContent = '0';
            return;
        }

        const listHtml = '<div class="news-list">' + seekingAlphaArticles.map(a => {
            const date = formatDate(a.published_date);
            const snippet = a.text || '';

            return (
                '<div class="news-article" data-article-url="' + escAttr(a.url) + '" ' +
                'role="button" tabindex="0">' +
                '<div class="news-article-body">' +
                '<div class="news-article-title">' + escHtml(a.title || 'Untitled') + '</div>' +
                '<div class="news-article-meta">' +
                '<span class="news-source-badge source-sa">SEEKING ALPHA</span>' +
                (date ? '<span>' + escHtml(date) + '</span>' : '') +
                '</div>' +
                (snippet ? '<div class="news-article-snippet">' + escHtml(snippet) + '</div>' : '') +
                '</div>' +
                '</div>'
            );
        }).join('') + '</div>';

        seekingAlphaContent.innerHTML = listHtml;
        if (seekingAlphaCountEl) {
            seekingAlphaCountEl.textContent = seekingAlphaArticles.length;
        }
    }

    async function loadSeekingAlphaNews() {
        if (seekingAlphaRefresh) {
            seekingAlphaRefresh.disabled = true;
            seekingAlphaRefresh.classList.add('loading');
            seekingAlphaRefresh.textContent = 'Loading…';
        }
        if (seekingAlphaContent) {
            seekingAlphaContent.innerHTML = '<div class="news-empty">Loading…</div>';
        }

        try {
            const response = await fetch('/api/frontend/market-news/seeking-alpha?limit=100');
            const data = await response.json();
            if (data.error) {
                if (seekingAlphaContent) {
                    seekingAlphaContent.innerHTML = '<div class="news-empty">Failed to load: ' +
                        escHtml(data.error) + '</div>';
                }
                return;
            }
            seekingAlphaArticles = data.articles || [];
            renderSeekingAlphaArticles();
        } catch (e) {
            console.error('Seeking Alpha news load failed:', e);
            if (seekingAlphaContent) {
                seekingAlphaContent.innerHTML = '<div class="news-empty">Error loading Seeking Alpha news</div>';
            }
        } finally {
            if (seekingAlphaRefresh) {
                seekingAlphaRefresh.disabled = false;
                seekingAlphaRefresh.classList.remove('loading');
                seekingAlphaRefresh.textContent = 'Refresh';
            }
        }
    }

    // Event Handlers
    function onContentClick(e) {
        // Channel filter chip
        const chip = e.target.closest('.news-filter-chip[data-channel]');
        if (chip) {
            e.preventDefault();
            benzingaActiveChannel = chip.getAttribute('data-channel') || '';
            loadBenzingaNews();
            return;
        }

        // Benzinga article
        const benzingaRow = e.target.closest('.news-article[data-benzinga-id]');
        if (benzingaRow && window.StockNewsShared) {
            e.preventDefault();
            const id = benzingaRow.getAttribute('data-benzinga-id');
            const article = benzingaArticles.find(a => String(a.benzinga_id) === String(id));
            if (article) window.StockNewsShared.showBenzingaArticle(article);
            return;
        }

        // FMP or Seeking Alpha article (open URL in new tab)
        const urlRow = e.target.closest('.news-article[data-article-url]');
        if (urlRow) {
            e.preventDefault();
            const url = urlRow.getAttribute('data-article-url');
            if (url) window.open(url, '_blank');
        }
    }

    function onContentKeydown(e) {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        const row = e.target.closest('.news-article');
        if (!row) return;
        e.preventDefault();
        row.click();
    }

    // Initialize
    function init() {
        // Tab switching
        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                const source = tab.getAttribute('data-source');
                switchToTab(source);
            });
        });

        // Benzinga
        if (benzingaChannelsEl) {
            benzingaChannelsEl.addEventListener('click', onContentClick);
        }
        if (benzingaContent) {
            benzingaContent.addEventListener('click', onContentClick);
            benzingaContent.addEventListener('keydown', onContentKeydown);
        }
        if (benzingaRefresh) {
            benzingaRefresh.addEventListener('click', () => loadBenzingaNews({ fromApi: true }));
        }

        // FMP
        if (fmpContent) {
            fmpContent.addEventListener('click', onContentClick);
            fmpContent.addEventListener('keydown', onContentKeydown);
        }
        if (fmpRefresh) {
            fmpRefresh.addEventListener('click', () => loadFmpNews());
        }

        // Seeking Alpha
        if (seekingAlphaContent) {
            seekingAlphaContent.addEventListener('click', onContentClick);
            seekingAlphaContent.addEventListener('keydown', onContentKeydown);
        }
        if (seekingAlphaRefresh) {
            seekingAlphaRefresh.addEventListener('click', () => loadSeekingAlphaNews());
        }

        // Load initial data (Benzinga only)
        loadBenzingaNews();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
