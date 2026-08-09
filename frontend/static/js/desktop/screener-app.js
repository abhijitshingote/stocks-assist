/* Desktop Screener Engine — shared by all desktop stock-list screener pages.
   Usage: call DesktopScreener.init(config) from a thin page wrapper.

   Config object:
   {
     endpoint:       string   API path segment, e.g. 'top-losers'
                              → fetches /api/frontend/{endpoint}/{cap}
     endpointFn:     null | function(cap) → URL string
                              Override for non-standard endpoint patterns.
                              If provided, endpoint is ignored.
     capFilter:      'server' (default) | 'client'
                              'server': reload data from API on cap change.
                              'client': fetch once; cap tabs filter allStocks in-memory.
     capField:       string   Field used for client-side cap filtering (default 'market_cap_bucket').
     defaultSort:    { col: string, dir: 'asc'|'desc' }
     label:          string   Human label shown in list header and stats bar
     accentCss:      string   CSS value for --screener-accent, e.g. 'var(--accent-red)'
     wideStorageKey: string   localStorage key for wide-layout override
     listValueFn:    function (stock) → { text, cls }
                              What to show in the list right-hand column.
                              cls: '' | 'positive' | 'negative' | 'muted'
                              Defaults to 1D return (dr_1) when omitted.
     listExtraFn:    null | function (stock) → html string
                              Additive HTML in the second row (after mcap). Prefer
                              '.list-extra' / '.event-mag' for the primary secondary
                              metric; '.mini-badge' for small type tags.
     listPrefixFn:   null | function (stock) → html string
                              HTML injected in the main row between serial # and ticker
                              (e.g. RS blue-dot).
     groupByFn:      null | function (stock) → string key
                              When set, the standard list renderer groups visible rows
                              under collapsible headers (same chrome as Vol Spike).
     groupLabelFn:   null | function (key) → display string
                              Header label for a group key (defaults to the key).
     groupCollapseStorageKey: null | string
                              localStorage key for collapsed group state. If omitted,
                              collapse state is in-memory for the session only.
     onListRendered: null | function ({ visible, stocks, helpers })
                              After every list paint (sector filter, resort, etc.).
     capFilterFn:    null | function(stock, cap) → boolean
                              Override client-side cap filtering (when capFilter='client').
                              Receives the stock object and the selected cap slug.
     extraControlsHtml: null | string
                              HTML injected into #screenerExtraControls in the top bar.
     onReady:        null | function({ reloadFn, resortFn, rerenderFn, selectStock, getState })
                              Called after setup; provides helpers for extra controls
                              that need to trigger data re-sort, reload, or re-render.
     onStockSelected: null | function (stock, api)
                              Optional hook called after a stock is selected.
                              api = { ticker, notes, inWatchlist, stockChart:()=>…, helpers }.

     ── Advanced hooks (all optional; enable page-specific screeners) ──
     transformData:  null | function (json) → stocks[]
                              Map a non-array API payload to the stocks array.
                              Use with onDataLoaded to capture side data (context, dates).
     onDataLoaded:   null | function (json, { filteredStocks, currentCap })
                              Side-effects after each load (context chips, date labels, counts).
     sortFn:         null | function (stocks, helpers) → stocks[]
                              Full sort override (used instead of defaultSort).
     filterFn:       null | function (stock) → boolean
                              Extra per-stock visibility filter, AND-ed with sector/industry
                              (e.g. recency filter). Call rerenderFn() after changing its inputs.
     renderListFn:   null | function ({ listEl, stocks, isVisible, selectedTicker, helpers })
                              Escape hatch — full stock-list render override. Prefer
                              groupByFn / listExtraFn / listPrefixFn instead.
     updateMetricsFn: null | function (stock, container, helpers)
                              Metrics-panel HTML override.
     prependMetricsFn: null | function (stock, helpers) → html string
                              Extra section prepended before the standard metrics
                              (e.g. the technical screener's Intraday block).
     updateTagsFn:   null | function (stock, strip, helpers)
                              Tags-strip override.
     onChartLoaded:  null | function (ticker, stockChart)
                              After the main price chart finishes loading (build subcharts here).
     onTimeframeChange: null | function (days, stockChart)
                              Timeframe button handler override (default: stockChart.setTimeframe).
     resortOnStarChange: false | true
                              Re-sort the list after a star rating change (watchlist page).
     seedWatchlistFromData: false | true
                              Seed watchlistStatus from each stock's watchlist_stars field
                              before the batch-check round-trip (watchlist page).
     removeOnUnwatch: false | true
                              When a ticker is removed from the watchlist, drop it from the
                              in-memory list (watchlist page).
   }
*/

window.DesktopScreener = (function () {

    function init(config) {
        // Apply screener accent CSS variable
        if (config.accentCss) {
            document.documentElement.style.setProperty('--screener-accent', config.accentCss);
        }

        // Inject extra controls HTML if provided
        const extraCtrl = document.getElementById('screenerExtraControls');
        if (extraCtrl && config.extraControlsHtml) {
            extraCtrl.innerHTML = config.extraControlsHtml;
        }

        // ── State ──────────────────────────────────────────────────
        let allStocks = [];        // full dataset (server response)
        let filteredStocks = [];   // after client-side cap filter
        let currentCap = 'all';
        let selectedTicker = null;
        let currentTimeframeDays = 365;
        let sortColumn = config.defaultSort ? config.defaultSort.col : 'dr_1';
        let sortDirection = config.defaultSort ? config.defaultSort.dir : 'desc';
        let stockChart = null;
        let selectedSector = null;
        let selectedIndustry = null;
        let watchlistStatus = {};
        let abiTickerNotesStatus = {};
        let newsPanel = null;
        let lastResponse = null;

        const WIDE_STORAGE = config.wideStorageKey || 'wideLayoutOverride';
        const WIDE_MQ = window.matchMedia('(min-width: 1600px)');

        // ── Wide layout ────────────────────────────────────────────
        function wideEffective() {
            const o = localStorage.getItem(WIDE_STORAGE);
            if (o === '1') return true;
            if (o === '0') return false;
            return WIDE_MQ.matches;
        }

        function applyWideLayout() {
            document.body.classList.toggle('wide-layout', wideEffective());
            const btn = document.getElementById('wideToggle');
            if (btn) {
                const wide = document.body.classList.contains('wide-layout');
                btn.classList.toggle('active', wide);
                btn.textContent = wide ? 'Standard layout' : 'Wide layout';
            }
            requestAnimationFrame(() => window.dispatchEvent(new Event('resize')));
        }

        function toggleWideLayout() {
            localStorage.setItem(WIDE_STORAGE, wideEffective() ? '0' : '1');
            applyWideLayout();
        }

        applyWideLayout();
        WIDE_MQ.addEventListener('change', applyWideLayout);
        document.getElementById('wideToggle')?.addEventListener('click', toggleWideLayout);

        // ── Helpers ────────────────────────────────────────────────
        function escAttr(s) {
            return String(s == null ? '' : s)
                .replace(/&/g, '&amp;')
                .replace(/"/g, '&quot;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;');
        }

        function fmtRet(v) {
            if (v == null) return '—';
            return (v >= 0 ? '+' : '') + v.toFixed(0) + '%';
        }

        function retCls(v) {
            if (v == null) return '';
            return v >= 0 ? 'ms-positive' : 'ms-negative';
        }

        function fmtVal(v, d) {
            if (v == null || v <= 0) return '—';
            return v.toFixed(d == null ? 1 : d);
        }

        function fmtVol(v) {
            if (!v) return '—';
            if (v >= 1e9) return (v / 1e9).toFixed(1) + 'B';
            if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
            if (v >= 1e3) return (v / 1e3).toFixed(0) + 'K';
            return v.toLocaleString();
        }

        function fmtMktCap(v) {
            if (!v) return '—';
            if (v >= 1e12) return '$' + (v / 1e12).toFixed(1) + 'T';
            if (v >= 1e9)  return '$' + (v / 1e9).toFixed(0) + 'B';
            if (v >= 1e6)  return '$' + (v / 1e6).toFixed(0) + 'M';
            return '$' + v.toLocaleString();
        }

        function msItem(label, val, cls, sub) {
            return `<span class="ms-item">` +
                `<span class="ms-label">${label}</span>` +
                `<span class="ms-val ${cls || ''}">${val}</span>` +
                (sub ? `<span class="ms-sub">${sub}</span>` : '') +
                `</span>`;
        }

        // ── URL builder ────────────────────────────────────────────
        function buildUrl(cap) {
            if (config.endpointFn) return config.endpointFn(cap);
            return `/api/frontend/${config.endpoint}/${cap}`;
        }

        // ── Client-side cap filter ──────────────────────────────────
        const CAP_BUCKETS = {
            all:   null,
            mega:  'Mega Cap',
            large: 'Large Cap',
            mid:   'Mid Cap',
            small: 'Small Cap',
            micro: 'Micro Cap',
        };

        function applyCapFilter(stocks, cap) {
            if (config.capFilter !== 'client') return stocks;
            if (cap === 'all') return stocks;
            if (config.capFilterFn) return stocks.filter(s => config.capFilterFn(s, cap));
            // Default: filter by market_cap_bucket field
            const bucket = CAP_BUCKETS[cap] || null;
            if (!bucket) return stocks;
            const field = config.capField || 'market_cap_bucket';
            return stocks.filter(s => s[field] === bucket);
        }

        // ── Data loading ───────────────────────────────────────────
        function sortData(data) {
            if (config.sortFn) return config.sortFn([...data], helpers);
            return [...data].sort((a, b) => {
                let aVal = a[sortColumn], bVal = b[sortColumn];
                if (aVal == null) return 1;
                if (bVal == null) return -1;
                if (typeof aVal === 'string') {
                    return sortDirection === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }
                return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
            });
        }

        async function loadData(cap) {
            showLoading();
            const isClientCap = config.capFilter === 'client';
            // For client-side cap: only fetch once (on first call or cap='all').
            // For server-side cap: always fetch.
            const shouldFetch = !isClientCap || allStocks.length === 0 || cap === 'all';
            if (shouldFetch) {
                try {
                    const url = isClientCap ? buildUrl('all') : buildUrl(cap);
                    const resp = await fetch(url);
                    const json = await resp.json();
                    lastResponse = json;
                    allStocks = config.transformData ? config.transformData(json) : json;
                    if (!Array.isArray(allStocks) || allStocks.error) allStocks = [];
                    if (config.seedWatchlistFromData) {
                        const seed = {};
                        allStocks.forEach(s => {
                            seed[s.ticker] = {
                                stars: Number.isFinite(s.watchlist_stars) ? s.watchlist_stars : 0,
                            };
                        });
                        watchlistStatus = seed;
                    }
                } catch (e) {
                    console.error('Failed to load screener data', e);
                    allStocks = [];
                    lastResponse = null;
                }
            }
            filteredStocks = sortData(applyCapFilter(allStocks, cap));
            hideLoading();
            document.getElementById('mainLayout').style.display = 'flex';
            const totalEl = document.getElementById('totalStocks');
            if (totalEl) totalEl.textContent = filteredStocks.length;

            // Reset sector/industry if they no longer exist in the current view
            if (selectedSector && !filteredStocks.some(s => (s.sector || 'Unknown') === selectedSector)) {
                selectedSector = null;
                selectedIndustry = null;
            } else if (selectedIndustry && !filteredStocks.some(s =>
                (s.sector || 'Unknown') === selectedSector &&
                (s.industry || 'Unknown') === selectedIndustry
            )) {
                selectedIndustry = null;
            }

            renderSectorTabs();
            renderIndustryTabs();
            renderList();

            if (config.onDataLoaded) config.onDataLoaded(lastResponse, { filteredStocks, currentCap: cap });

            const tickers = filteredStocks.map(s => s.ticker);
            if (tickers.length > 0) loadWatchlistForStocks(tickers);

            if (filteredStocks.length > 0 && !selectedTicker) {
                selectStock(filteredStocks[0].ticker);
            } else if (selectedTicker) {
                const stillExists = filteredStocks.find(s => s.ticker === selectedTicker);
                if (stillExists) selectStock(selectedTicker);
                else if (filteredStocks.length > 0) selectStock(filteredStocks[0].ticker);
            }
        }

        // ── Watchlist + Abi ticker notes ───────────────────────────
        function getStars(ticker) {
            const wl = watchlistStatus[ticker];
            return (wl && Number.isFinite(wl.stars)) ? wl.stars : 0;
        }

        function starsHtml(ticker) {
            if (!watchlistStatus[ticker]) return '';
            const stars = getStars(ticker);
            let html = `<span class="star-rating" data-stars="${stars}">`;
            for (let i = 1; i <= 3; i++) {
                html += `<span class="star${i <= stars ? ' filled' : ''}" data-level="${i}" title="${i} star${i > 1 ? 's' : ''}">★</span>`;
            }
            return html + '</span>';
        }

        async function setStarsForTicker(ticker, stars) {
            if (!watchlistStatus[ticker]) return;
            const prev = watchlistStatus[ticker].stars || 0;
            watchlistStatus[ticker].stars = stars;
            if (config.resortOnStarChange) filteredStocks = sortData(filteredStocks);
            renderList();
            try {
                const resp = await fetch('/api/frontend/abi-watchlist/' + ticker, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ stars }),
                });
                if (!resp.ok) throw new Error('PUT failed: ' + resp.status);
            } catch (e) {
                console.error('Failed to save stars for ' + ticker, e);
                watchlistStatus[ticker].stars = prev;
                renderList();
            }
        }

        async function loadWatchlistForStocks(tickers) {
            if (!tickers || tickers.length === 0) return;
            try {
                const [wlResp, cmtResp] = await Promise.all([
                    fetch('/api/frontend/abi-watchlist/batch-check', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ tickers }),
                    }),
                    fetch('/api/frontend/abi-ticker-notes/batch-check', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ tickers }),
                    }),
                ]);
                if (wlResp.ok) watchlistStatus = await wlResp.json();
                if (cmtResp.ok) abiTickerNotesStatus = await cmtResp.json();
                renderList();
                if (selectedTicker) {
                    updateWatchlistBtn(selectedTicker);
                    updateWatchlistNotes(selectedTicker);
                }
            } catch (e) { console.error('Error loading watchlist/notes:', e); }
        }

        function updateWatchlistBtn(ticker) {
            const btn = document.getElementById('detailWlBtn');
            if (!btn) return;
            const inWl = !!watchlistStatus[ticker];
            btn.classList.toggle('in-watchlist', inWl);
            btn.textContent = inWl ? '★ Watchlist' : '+ Watchlist';
        }

        function updateWatchlistNotes(ticker) {
            const section = document.getElementById('wlNotesSection');
            const textEl = document.getElementById('wlNotesText');
            if (!ticker) { section.style.display = 'none'; return; }
            section.style.display = 'block';
            const cmt = abiTickerNotesStatus[ticker];
            if (cmt && cmt.notes) {
                textEl.innerHTML = (typeof marked !== 'undefined') ? marked.parse(cmt.notes) : cmt.notes;
                textEl.classList.remove('empty');
            } else {
                textEl.innerHTML = '<em>No Abi ticker notes</em>';
                textEl.classList.add('empty');
            }
        }

        window._screener_toggleWatchlist = function () {
            if (!selectedTicker) return;
            const inWl = !!watchlistStatus[selectedTicker];
            window._wlToggle(selectedTicker, inWl, function (nowIn, ticker) {
                if (nowIn) {
                    watchlistStatus[ticker] = watchlistStatus[ticker] || { stars: 0 };
                } else {
                    delete watchlistStatus[ticker];
                    if (config.removeOnUnwatch) {
                        allStocks = allStocks.filter(s => s.ticker !== ticker);
                        filteredStocks = filteredStocks.filter(s => s.ticker !== ticker);
                        const totalEl = document.getElementById('totalStocks');
                        if (totalEl) totalEl.textContent = filteredStocks.length;
                        if (selectedTicker === ticker) {
                            if (filteredStocks.length > 0) {
                                selectStock(filteredStocks[0].ticker);
                            } else {
                                selectedTicker = null;
                                document.getElementById('rightPlaceholder').style.display = 'flex';
                                document.getElementById('detailHeader').style.display = 'none';
                                document.getElementById('chartsContainer').style.display = 'none';
                                document.getElementById('tpRightColumn')?.classList.remove('visible');
                            }
                        }
                    }
                }
                updateWatchlistBtn(ticker);
                renderList();
            });
        };

        window._screener_openNotes = function () {
            if (!selectedTicker) return;
            const cmt = abiTickerNotesStatus[selectedTicker];
            const currentNotes = (cmt && cmt.notes) || '';
            window._notesOpen(selectedTicker, currentNotes, !!currentNotes, function (action, ticker, newNotes) {
                if (action === 'saved') abiTickerNotesStatus[ticker] = { notes: newNotes || '' };
                else if (action === 'removed') delete abiTickerNotesStatus[ticker];
                updateWatchlistNotes(ticker);
            });
        };

        // ── Sector / Industry filter ───────────────────────────────
        function passesSectorIndustry(s) {
            if (selectedSector && (s.sector || 'Unknown') !== selectedSector) return false;
            if (selectedIndustry && (s.industry || 'Unknown') !== selectedIndustry) return false;
            if (config.filterFn && !config.filterFn(s)) return false;
            return true;
        }

        // True when the stock passes the page's extra filter (e.g. recency).
        // Used for sector/industry counts so they reflect the active filter.
        function passesExtraFilter(s) {
            return !config.filterFn || config.filterFn(s);
        }

        function renderSectorTabs() {
            const bar = document.getElementById('sectorBar');
            if (!bar) return;
            const base = filteredStocks.filter(passesExtraFilter);
            const counts = {};
            base.forEach(s => {
                const sec = s.sector || 'Unknown';
                counts[sec] = (counts[sec] || 0) + 1;
            });
            const sectors = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);
            const allActive = selectedSector === null ? ' active' : '';
            let html = `<span class="si-bar-label">Sector</span>` +
                `<button class="si-tab sector-tab${allActive}" data-sector="">All <span class="count">${base.length}</span></button>`;
            sectors.forEach(sec => {
                const isActive = sec === selectedSector ? ' active' : '';
                html += `<button class="si-tab sector-tab${isActive}" data-sector="${escAttr(sec)}">${escAttr(sec)} <span class="count">${counts[sec]}</span></button>`;
            });
            bar.innerHTML = html;
            bar.querySelectorAll('.sector-tab').forEach(btn => {
                btn.addEventListener('click', () => {
                    const sec = btn.dataset.sector || null;
                    if (sec === selectedSector) { selectedSector = null; selectedIndustry = null; }
                    else { selectedSector = sec; selectedIndustry = null; }
                    renderSectorTabs();
                    renderIndustryTabs();
                    renderList();
                    ensureSelectedVisible();
                });
            });
        }

        function renderIndustryTabs() {
            const bar = document.getElementById('industryBar');
            if (!bar) return;
            if (!selectedSector) { bar.classList.remove('visible'); bar.innerHTML = ''; return; }
            const counts = {};
            filteredStocks.forEach(s => {
                if ((s.sector || 'Unknown') !== selectedSector) return;
                if (!passesExtraFilter(s)) return;
                const ind = s.industry || 'Unknown';
                counts[ind] = (counts[ind] || 0) + 1;
            });
            const industries = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);
            const totalInSector = industries.reduce((acc, k) => acc + counts[k], 0);
            const allActive = selectedIndustry === null ? ' active' : '';
            let html = `<span class="si-bar-label">Industry</span>` +
                `<button class="si-tab industry-tab${allActive}" data-industry="">All <span class="count">${totalInSector}</span></button>`;
            industries.forEach(ind => {
                const isActive = ind === selectedIndustry ? ' active' : '';
                html += `<button class="si-tab industry-tab${isActive}" data-industry="${escAttr(ind)}">${escAttr(ind)} <span class="count">${counts[ind]}</span></button>`;
            });
            bar.innerHTML = html;
            bar.classList.add('visible');
            bar.querySelectorAll('.industry-tab').forEach(btn => {
                btn.addEventListener('click', () => {
                    const ind = btn.dataset.industry || null;
                    selectedIndustry = (ind === selectedIndustry) ? null : ind;
                    renderIndustryTabs();
                    renderList();
                    ensureSelectedVisible();
                });
            });
        }

        function ensureSelectedVisible() {
            const visible = filteredStocks.filter(passesSectorIndustry);
            if (!visible.length) return;
            if (!visible.find(s => s.ticker === selectedTicker)) selectStock(visible[0].ticker);
        }

        // ── Stock list ─────────────────────────────────────────────
        // Wire click + star handlers on the current list DOM. Reused by the
        // ── Collapsible group state (used when config.groupByFn is set) ──
        let sessionCollapsedGroups = {};

        function getCollapsedGroups() {
            const key = config.groupCollapseStorageKey;
            if (!key) return sessionCollapsedGroups;
            try { return JSON.parse(localStorage.getItem(key)) || {}; }
            catch { return {}; }
        }

        function setCollapsedGroups(collapsed) {
            const key = config.groupCollapseStorageKey;
            if (!key) { sessionCollapsedGroups = collapsed; return; }
            localStorage.setItem(key, JSON.stringify(collapsed));
        }

        function updateCollapseAllBtn() {
            const btn = document.getElementById('collapseAllBtn');
            if (!btn) return;
            const headers = document.querySelectorAll('.day-group-header');
            if (!headers.length) { btn.textContent = '▼ All'; return; }
            const anyExpanded = Array.from(headers).some(h => !h.classList.contains('collapsed'));
            btn.textContent = anyExpanded ? '▼ All' : '▶ All';
        }

        function bindGroupHeaders(listEl) {
            listEl.querySelectorAll('.day-group-header').forEach(headerEl => {
                headerEl.addEventListener('click', () => {
                    const gkey = headerEl.dataset.date;
                    const collapsed = getCollapsedGroups();
                    collapsed[gkey] = !collapsed[gkey];
                    setCollapsedGroups(collapsed);
                    headerEl.classList.toggle('collapsed', !!collapsed[gkey]);
                    updateCollapseAllBtn();
                });
            });
            updateCollapseAllBtn();
        }

        // default renderer and available to renderListFn overrides via helpers.
        function bindListRows(listEl) {
            listEl.querySelectorAll('.stock-item').forEach(el => {
                el.addEventListener('click', (e) => {
                    if (e.target.closest('.star-rating')) return;
                    selectStock(el.dataset.ticker);
                });
            });
            listEl.querySelectorAll('.star-rating .star').forEach(starEl => {
                starEl.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const itemEl = starEl.closest('.stock-item');
                    if (!itemEl) return;
                    const ticker = itemEl.dataset.ticker;
                    const level = parseInt(starEl.dataset.level, 10);
                    const current = getStars(ticker);
                    setStarsForTicker(ticker, level === current ? level - 1 : level);
                });
            });
        }

        function badge52w(s) {
            return s.at_52w_high ? '<span class="tag-near-52w" title="At/near 52-week high">52W</span>' : '';
        }

        function defaultListValue(s) {
            const v = s.dr_1;
            return {
                text: v != null ? (v >= 0 ? '+' : '') + v.toFixed(1) + '%' : '—',
                cls: v != null ? (v >= 0 ? 'positive' : 'negative') : 'muted',
            };
        }

        // Canonical two-row stock item — same markup every screener uses.
        function stockRowHtml(s, idx, hiddenCls) {
            const isActive = s.ticker === selectedTicker ? ' active' : '';
            const listVal = config.listValueFn ? config.listValueFn(s) : defaultListValue(s);
            const mcapStr = s.market_cap ? ' (' + fmtMktCap(s.market_cap) + ')' : '';
            const stars = getStars(s.ticker);
            const hasStarsCls = stars > 0 ? ' has-stars' : '';
            const extra = config.listExtraFn ? config.listExtraFn(s) : '';
            const prefix = config.listPrefixFn ? config.listPrefixFn(s) : '';
            return `<div class="stock-item two-row${isActive}${hasStarsCls}${hiddenCls || ''}" data-ticker="${s.ticker}">` +
                `<div class="stock-main-row">` +
                    `<span class="si-left">` +
                    starsHtml(s.ticker) +
                    `<span class="sn">${idx}.</span>` +
                    prefix +
                    `<span class="ticker">${s.ticker}</span>` +
                    badge52w(s) +
                    `</span>` +
                    `<span class="ret ${listVal.cls}">${listVal.text}</span>` +
                `</div>` +
                `<div class="stock-extra-row">` +
                    (mcapStr ? `<span class="ticker-mcap">${mcapStr}</span>` : '') +
                    extra +
                `</div>` +
                `</div>`;
        }

        function renderGroupedList(listEl, visible) {
            // Preserve first-seen order from the already-sorted visible array.
            const groups = new Map();
            visible.forEach(s => {
                const key = config.groupByFn(s) || 'Unknown';
                if (!groups.has(key)) groups.set(key, []);
                groups.get(key).push(s);
            });

            const collapsed = getCollapsedGroups();
            let html = '';
            let runningIdx = 0;
            groups.forEach((rows, key) => {
                const isCollapsed = collapsed[key] ? ' collapsed' : '';
                const label = config.groupLabelFn ? config.groupLabelFn(key) : key;
                html += `<div class="day-group-header${isCollapsed}" data-date="${escAttr(key)}">` +
                    `<span class="day-group-chevron">▼</span>` +
                    `<span class="day-group-date">${label}</span>` +
                    `<span class="day-group-count">${rows.length}</span>` +
                    `</div><div class="day-group-items">`;
                rows.forEach(s => {
                    runningIdx++;
                    html += stockRowHtml(s, runningIdx, '');
                });
                html += `</div>`;
            });
            listEl.innerHTML = html;
            bindGroupHeaders(listEl);
        }

        function renderList() {
            const listEl = document.getElementById('stockList');

            if (config.renderListFn) {
                const visibleCount = config.renderListFn({
                    listEl,
                    stocks: filteredStocks,
                    isVisible: passesSectorIndustry,
                    selectedTicker,
                    helpers,
                });
                const countElC = document.getElementById('stockCount');
                if (countElC && typeof visibleCount === 'number') countElC.textContent = visibleCount;
                bindListRows(listEl);
                if (config.onListRendered) {
                    config.onListRendered({
                        visible: filteredStocks.filter(passesSectorIndustry),
                        stocks: filteredStocks,
                        helpers,
                    });
                }
                return;
            }

            let visibleCount = 0;
            if (config.groupByFn) {
                const visible = filteredStocks.filter(passesSectorIndustry);
                visibleCount = visible.length;
                renderGroupedList(listEl, visible);
            } else {
                listEl.innerHTML = filteredStocks.map((s, i) => {
                    const hidden = passesSectorIndustry(s) ? '' : ' hidden-by-filter';
                    if (!hidden) visibleCount++;
                    return stockRowHtml(s, i + 1, hidden);
                }).join('');
            }

            const countEl = document.getElementById('stockCount');
            if (countEl) countEl.textContent = visibleCount;

            bindListRows(listEl);

            if (config.onListRendered) {
                config.onListRendered({
                    visible: filteredStocks.filter(passesSectorIndustry),
                    stocks: filteredStocks,
                    helpers,
                });
            }
        }

        // ── Select stock ───────────────────────────────────────────
        async function selectStock(ticker) {
            if (!ticker) return;
            selectedTicker = ticker;

            document.querySelectorAll('.stock-item').forEach(el => {
                el.classList.toggle('active', el.dataset.ticker === ticker);
            });

            const stock = filteredStocks.find(s => s.ticker === ticker);
            if (!stock) return;

            document.getElementById('rightPlaceholder').style.display = 'none';
            document.getElementById('detailHeader').style.display = 'block';
            document.getElementById('chartsContainer').style.display = 'flex';
            document.getElementById('tpRightColumn').classList.add('visible');

            if (newsPanel) { newsPanel.reset(); newsPanel.onTickerChange(); }

            document.getElementById('detailTicker').textContent = ticker;
            document.getElementById('detailCompany').textContent = stock.company_name || '';
            document.getElementById('detailMktCap').textContent = stock.market_cap ? fmtMktCap(stock.market_cap) : '';
            const sectorParts = [stock.sector, stock.industry].filter(Boolean);
            document.getElementById('detailSectorIndustry').textContent = sectorParts.length ? sectorParts.join(' · ') : '';
            document.getElementById('detailLink').href = '/stock/' + ticker;
            document.getElementById('metricsTicker').textContent = ticker;
            document.getElementById('metricsCompany').textContent = stock.company_name || '';

            updateTagsStrip(stock);
            updateMetrics(stock);
            updateWatchlistBtn(ticker);
            updateWatchlistNotes(ticker);

            if (config.onStockSelected) {
                config.onStockSelected(stock, {
                    ticker,
                    notes: (abiTickerNotesStatus[ticker] && abiTickerNotesStatus[ticker].notes) || '',
                    inWatchlist: !!watchlistStatus[ticker],
                    helpers,
                });
            }

            await loadChart(ticker);
        }

        // ── Tags strip ─────────────────────────────────────────────
        function updateTagsStrip(stock) {
            const strip = document.getElementById('tagsStrip');
            if (!strip) return;
            if (config.updateTagsFn) { config.updateTagsFn(stock, strip, helpers); return; }
            let pills = '';
            const tags = (stock.tags || '').split(', ').filter(t => t.trim());
            if (tags.includes('high_sales_growth')) {
                pills += '<span class="tag-pill high-growth">high_sales_growth</span>';
            }
            if (stock.last_event_type && stock.last_event_date) {
                const isSpike = stock.last_event_type === 'volume_spike';
                const label = isSpike ? 'spike' : 'gap';
                const cls = isSpike ? 'spike' : 'gapper';
                let mag = '';
                if (stock.last_event_magnitude != null) {
                    mag = isSpike
                        ? stock.last_event_magnitude.toFixed(1) + 'x'
                        : (stock.last_event_magnitude * 100).toFixed(1) + '%';
                }
                pills += `<span class="tag-pill ${cls}">last ${label}: ${mag} (${stock.last_event_date})</span>`;
            }
            if (!pills) { strip.classList.remove('visible'); strip.innerHTML = ''; return; }
            strip.innerHTML = pills;
            strip.classList.add('visible');
        }

        // ── Metrics panel ──────────────────────────────────────────
        function updateMetrics(s) {
            const container = document.getElementById('metricsContent');
            if (config.updateMetricsFn) { config.updateMetricsFn(s, container, helpers); return; }
            function section(title, items) {
                return `<div class="ms-section"><span class="ms-section-title">${title}</span><div class="ms-section-row">${items}</div></div>`;
            }
            let html = '';
            // Page-specific leading section(s), e.g. the technical screener's Intraday block.
            if (config.prependMetricsFn) html += config.prependMetricsFn(s, helpers);
            let items = '';

            items += msItem('Price', s.current_price ? '$' + s.current_price.toFixed(2) : '—');
            items += msItem('MCap', fmtMktCap(s.market_cap));
            items += msItem('Vol', fmtVol(s.volume));
            items += msItem('$Vol', s.dollar_volume ? fmtMktCap(s.dollar_volume) : '—');
            html += section('Price & Market', items);

            items = '';
            [['1D', 'dr_1'], ['5D', 'dr_5'], ['20D', 'dr_20']].forEach(([l, k]) => {
                items += msItem(l, fmtRet(s[k]), retCls(s[k]) + ' ms-val-lg');
            });
            [['60D', 'dr_60'], ['120D', 'dr_120']].forEach(([l, k]) => {
                items += msItem(l, fmtRet(s[k]), retCls(s[k]));
            });
            html += section('Returns', items);

            items = '';
            [['T-1', 'rev_growth_t_minus_1'], ['T', 'rev_growth_t'], ['T+1', 'rev_growth_t_plus_1'], ['T+2', 'rev_growth_t_plus_2']].forEach(([l, k]) => {
                items += msItem(l, fmtRet(s[k]), retCls(s[k]));
            });
            html += section('Revenue Growth', items);

            items = '';
            [['T-1', 'eps_growth_t_minus_1'], ['T', 'eps_growth_t'], ['T+1', 'eps_growth_t_plus_1'], ['T+2', 'eps_growth_t_plus_2']].forEach(([l, k]) => {
                items += msItem(l, fmtRet(s[k]), retCls(s[k]));
            });
            html += section('EPS Growth', items);

            items = '';
            [['T-1', 'ps_t_minus_1'], ['T', 'ps_t'], ['T+1', 'ps_t_plus_1'], ['T+2', 'ps_t_plus_2']].forEach(([l, k]) => {
                items += msItem(l, fmtVal(s[k], 1));
            });
            html += section('P/S Ratio', items);

            items = '';
            [['T-1', 'pe_t_minus_1'], ['T', 'pe_t'], ['T+1', 'pe_t_plus_1'], ['T+2', 'pe_t_plus_2']].forEach(([l, k]) => {
                items += msItem(l, fmtVal(s[k], 0));
            });
            html += section('P/E Ratio', items);

            items = '';
            items += msItem('RSI', s.rsi_mktcap || '—', s.rsi_mktcap >= 70 ? 'ms-positive' : s.rsi_mktcap <= 30 ? 'ms-negative' : '');
            items += msItem('ATR%', s.atr20 ? s.atr20.toFixed(1) + '%' : '—');
            items += msItem('V/Avg', s.vol_vs_10d_avg ? s.vol_vs_10d_avg.toFixed(1) + 'x' : '—');
            html += section('Technical', items);

            items = '';
            items += msItem('Float', fmtVol(s.float_shares));
            items += msItem('Free%', s.free_float ? s.free_float.toFixed(1) + '%' : '—');
            items += msItem('Short%', s.short_float ? s.short_float.toFixed(1) + '%' : '—');
            items += msItem('S.Ratio', s.short_ratio ? s.short_ratio.toFixed(1) : '—');
            html += section('Float & Short', items);

            container.innerHTML = html;
        }

        // ── Price Chart ────────────────────────────────────────────
        function resizeStockChart() {
            if (!stockChart || !stockChart.chart) return;
            const panel = document.getElementById('stockChartPanel');
            if (!panel) return;
            const header = panel.querySelector('.chart-panel-header');
            const usedH = (header ? header.offsetHeight : 20) + 10;
            const tickerH = Math.max(panel.clientHeight - usedH, 200);
            const container = document.getElementById('stockChartContainer');
            container.style.height = tickerH + 'px';
            const legendH = stockChart.legendContainer ? stockChart.legendContainer.offsetHeight : 0;
            const chartAreaH = tickerH - legendH;
            const priceH = Math.floor(chartAreaH * CHART_CONFIG.candlestickRatio);
            const volumeH = chartAreaH - priceH;
            if (stockChart.priceContainer) stockChart.priceContainer.style.height = priceH + 'px';
            if (stockChart.volumeContainer) stockChart.volumeContainer.style.height = volumeH + 'px';
            stockChart.chart.applyOptions({ height: priceH, width: container.clientWidth });
            if (stockChart.volumeChart) {
                stockChart.volumeChart.applyOptions({ height: volumeH, width: container.clientWidth });
            }
        }

        let resizeHandler = null;

        function setupResize() {
            if (resizeHandler) window.removeEventListener('resize', resizeHandler);
            resizeHandler = () => resizeStockChart();
            window.addEventListener('resize', resizeHandler);
        }

        async function loadChart(ticker) {
            if (stockChart) { stockChart.destroy(); stockChart = null; }
            document.getElementById('stockChartContainer').innerHTML = '';
            const titleEl = document.getElementById('stockChartTitle');
            if (titleEl) { titleEl.textContent = ticker + ' Chart'; titleEl.style.display = ''; }

            const panel = document.getElementById('stockChartPanel');
            const header = panel.querySelector('.chart-panel-header');
            const usedH = (header ? header.offsetHeight : 20) + 10;
            const tickerH = Math.max(panel.clientHeight - usedH, 200);
            document.getElementById('stockChartContainer').style.height = tickerH + 'px';

            try {
                stockChart = new StockChart('stockChartContainer', { height: tickerH, showRSI: false });
                await stockChart.load(ticker);
                setupResize();
                stockChart.setTimeframe(currentTimeframeDays);
                if (config.onChartLoaded) config.onChartLoaded(ticker, stockChart);
                requestAnimationFrame(() => resizeStockChart());
            } catch (e) {
                console.error('Chart load error for ' + ticker, e);
            }
        }

        // ── UI helpers ─────────────────────────────────────────────
        function showLoading() {
            document.getElementById('loadingState').style.display = 'flex';
            document.getElementById('mainLayout').style.display = 'none';
        }

        function hideLoading() {
            document.getElementById('loadingState').style.display = 'none';
        }

        function getVisibleTickers() {
            return filteredStocks.filter(passesSectorIndustry).map(s => s.ticker);
        }

        async function copyVisibleTickers() {
            const btn = document.getElementById('copyTickersBtn');
            const tickers = getVisibleTickers();
            const original = '📋 Copy';
            const flash = (msg, ok) => {
                if (!btn) return;
                btn.classList.toggle('copied', !!ok);
                btn.textContent = msg;
                setTimeout(() => { btn.classList.remove('copied'); btn.textContent = original; }, 1500);
            };
            if (!tickers.length) { flash('No tickers', false); return; }
            const text = tickers.join(', ');
            try {
                if (navigator.clipboard && window.isSecureContext) {
                    await navigator.clipboard.writeText(text);
                } else {
                    const ta = document.createElement('textarea');
                    ta.value = text;
                    ta.style.position = 'fixed';
                    ta.style.opacity = '0';
                    document.body.appendChild(ta);
                    ta.select();
                    document.execCommand('copy');
                    document.body.removeChild(ta);
                }
                flash(`✓ Copied ${tickers.length}`, true);
            } catch (e) {
                console.error('Copy failed', e);
                flash('Copy failed', false);
            }
        }

        function scrollToStock(ticker) {
            const el = document.querySelector(`.stock-item[data-ticker="${ticker}"]`);
            if (el) el.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        }

        // ── News panel setup ───────────────────────────────────────
        function setupNewsAndPanels() {
            newsPanel = window.StockNewsShared.createNewsPanel({
                contentId: 'tpNewsContent',
                loadBtnId: 'tpLoadNewsBtn',
                benzingaBtnId: 'tpBenzingaNewsBtn',
                getTicker: () => selectedTicker,
                showSnippet: false,
            });
            newsPanel.setup();

            const newsStack = document.getElementById('tpNewsStack');
            const newsCollapseBtn = document.getElementById('tpNewsCollapseBtn');
            if (newsStack && newsCollapseBtn) {
                const toggle = () => {
                    newsStack.classList.toggle('collapsed');
                    newsCollapseBtn.setAttribute('aria-expanded', String(!newsStack.classList.contains('collapsed')));
                    requestAnimationFrame(() => window.dispatchEvent(new Event('resize')));
                };
                newsCollapseBtn.addEventListener('click', toggle);
                newsCollapseBtn.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); }
                });
            }

            const metricsStack = document.getElementById('tpMetricsStack');
            const metricsHead = document.getElementById('tpMetricsHead');
            if (metricsStack && metricsHead) {
                const toggle = () => {
                    metricsStack.classList.toggle('collapsed');
                    metricsHead.setAttribute('aria-expanded', String(!metricsStack.classList.contains('collapsed')));
                    requestAnimationFrame(() => window.dispatchEvent(new Event('resize')));
                };
                metricsHead.addEventListener('click', toggle);
                metricsHead.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); }
                });
            }

            setupColumnToggle();
        }

        // Collapse/expand the entire right column so the chart can use the freed
        // width. State persists per browser via localStorage.
        function setupColumnToggle() {
            const rightCol = document.getElementById('tpRightColumn');
            const colToggle = document.getElementById('tpColToggle');
            if (!rightCol || !colToggle) return;
            const KEY = 'tpColCollapsed';
            const apply = (collapsed) => {
                rightCol.classList.toggle('col-collapsed', collapsed);
                colToggle.setAttribute('aria-expanded', String(!collapsed));
                colToggle.textContent = collapsed ? '‹' : '›';
                colToggle.title = collapsed ? 'Expand panel' : 'Collapse panel to widen chart';
                requestAnimationFrame(() => window.dispatchEvent(new Event('resize')));
            };
            apply(localStorage.getItem(KEY) === '1');
            colToggle.addEventListener('click', () => {
                const collapsed = !rightCol.classList.contains('col-collapsed');
                localStorage.setItem(KEY, collapsed ? '1' : '0');
                apply(collapsed);
            });
        }

        // ── Event listeners ────────────────────────────────────────
        document.querySelectorAll('.cap-tab').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.cap-tab').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentCap = btn.dataset.cap;
                if (config.capFilter !== 'client') selectedTicker = null;
                loadData(currentCap);
            });
        });

        document.querySelectorAll('#chartTimeframes button').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('#chartTimeframes button').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentTimeframeDays = parseInt(btn.dataset.days, 10);
                if (config.onTimeframeChange) config.onTimeframeChange(currentTimeframeDays, stockChart);
                else if (stockChart) stockChart.setTimeframe(currentTimeframeDays);
            });
        });

        document.getElementById('copyTickersBtn')?.addEventListener('click', copyVisibleTickers);

        document.addEventListener('keydown', function (e) {
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
            const visible = filteredStocks.filter(passesSectorIndustry);
            if (!visible.length) return;
            const idx = visible.findIndex(s => s.ticker === selectedTicker);
            if (e.key === 'ArrowDown' || e.key === 'j') {
                e.preventDefault();
                const next = visible[idx < visible.length - 1 ? idx + 1 : 0];
                selectStock(next.ticker);
                scrollToStock(next.ticker);
            } else if (e.key === 'ArrowUp' || e.key === 'k') {
                e.preventDefault();
                const prev = visible[idx > 0 ? idx - 1 : visible.length - 1];
                selectStock(prev.ticker);
                scrollToStock(prev.ticker);
            }
        });

        // ── Hook helper surface ────────────────────────────────────
        // Shared utilities passed to page-specific hooks (sortFn, renderListFn,
        // updateMetricsFn, updateTagsFn, onStockSelected).
        const helpers = {
            escAttr, fmtRet, retCls, fmtVal, fmtVol, fmtMktCap, msItem,
            getStars, starsHtml, setStarsForTicker, badge52w,
            passesSectorIndustry,
            selectStock: (t) => selectStock(t),
            getSelectedTicker: () => selectedTicker,
        };

        // ── Collapse-all for grouped lists ─────────────────────────
        if (config.groupByFn) {
            document.getElementById('collapseAllBtn')?.addEventListener('click', () => {
                const headers = document.querySelectorAll('.day-group-header');
                if (!headers.length) return;
                const anyExpanded = Array.from(headers).some(h => !h.classList.contains('collapsed'));
                const collapsed = getCollapsedGroups();
                headers.forEach(h => {
                    collapsed[h.dataset.date] = anyExpanded;
                    h.classList.toggle('collapsed', anyExpanded);
                });
                setCollapsedGroups(collapsed);
                updateCollapseAllBtn();
            });
        }

        // ── Init ───────────────────────────────────────────────────
        setupNewsAndPanels();
        loadData('all');

        // Expose helpers for pages that need to trigger resort/reload from custom controls
        if (config.onReady) {
            config.onReady({
                reloadFn: () => loadData(currentCap),
                resortFn: (col, dir) => {
                    sortColumn = col;
                    sortDirection = dir;
                    filteredStocks = sortData(filteredStocks);
                    renderList();
                    ensureSelectedVisible();
                },
                rerenderFn: () => { renderSectorTabs(); renderIndustryTabs(); renderList(); ensureSelectedVisible(); },
                resortWithFn: () => { filteredStocks = sortData(filteredStocks); renderList(); ensureSelectedVisible(); },
                selectStock: (t) => selectStock(t),
                getState: () => ({ allStocks, filteredStocks, currentCap, selectedTicker }),
            });
        }
    }

    return { init };

})();
