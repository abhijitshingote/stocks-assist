/**
 * Market News Page - Card-based layout matching stock detail page
 */
(function() {
    'use strict';

    // State
    let activeSource = 'seeking-alpha';
    let seekingAlphaArticles = [];

    // Elements
    const tabs = document.querySelectorAll('.news-tab');
    const sections = document.querySelectorAll('.news-source-section');
    
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

    function getSourceBadgeClass(source) {
        if (source === 'Seeking Alpha') return 'source-sa';
        if (source === 'Yahoo Finance') return 'source-yahoo';
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
        
        if (source === 'seeking-alpha') {
            seekingAlphaSection.classList.add('active');
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
        // Seeking Alpha article (open URL in new tab)
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

        // Seeking Alpha
        if (seekingAlphaContent) {
            seekingAlphaContent.addEventListener('click', onContentClick);
            seekingAlphaContent.addEventListener('keydown', onContentKeydown);
        }
        if (seekingAlphaRefresh) {
            seekingAlphaRefresh.addEventListener('click', () => loadSeekingAlphaNews());
        }

        loadSeekingAlphaNews();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
