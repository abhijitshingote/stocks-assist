/* Vol Spike & Gapper — 90d.
   Universe: stock_volspike_gapper with last_event_date in the last 90 calendar days.
   Sort toggle: mcap-adjusted event-day return (Daily Review -0.134 scale) vs
   monthly setup/readiness (setupParts from vsg-grouped.js, as-is) vs
   last_event_date descending.
   Flat list — no date grouping; event date is a chip on the row.
*/
(function () {
    const SORT_KEY = 'vsg90dSort';
    const MONTH_ABBREV = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const MA_LABELS = { ema_10: 'E10', ema_20: 'E20', dma_50: 'D50', dma_200: 'D200' };
    const SETUP_W = { prox: 0.35, tight: 0.25, hold: 0.20, trend: 0.20 };
    const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

    // Same as vsg-grouped.js setupParts (monthly readiness).
    function setupParts(m) {
        if (!m) return { prox: 0, tight: 0, hold: 0, trend: 0, total: 0 };
        const distAtr = m.ma_dist_atr != null ? m.ma_dist_atr : Math.abs(m.ma_dist_pct) / 2;
        const prox = clamp(1 - distAtr / 1.5, 0, 1);
        const tight = clamp((3.5 - m.range10_atr) / 2, 0, 1);
        const pos = m.pos_in_range10 == null ? 0.5 : m.pos_in_range10;
        const hold = clamp((pos - 0.4) / 0.5, 0, 1);
        const trend = m.above_all_ma ? 1 : m.above_ema20 ? 0.6 : 0;
        return {
            prox, tight, hold, trend,
            total: SETUP_W.prox * prox + SETUP_W.tight * tight +
                SETUP_W.hold * hold + SETUP_W.trend * trend,
        };
    }

    function compactDate(dateStr) {
        if (!dateStr) return '';
        const parts = String(dateStr).split('-');
        if (parts.length !== 3) return '';
        const month = MONTH_ABBREV[parseInt(parts[1], 10) - 1];
        if (!month) return '';
        return month + ' ' + parseInt(parts[2], 10);
    }

    let sortMode = 'adj';
    try {
        const saved = localStorage.getItem(SORT_KEY);
        if (saved === 'ready' || saved === 'adj' || saved === 'date') sortMode = saved;
    } catch (e) {}

    let screenerApi = null;
    let setupMap = null;
    let setupResortPending = false;

    fetch('/api/frontend/volspike-gapper-setup')
        .then(r => r.json())
        .then(m => {
            if (!m || m.error) return;
            setupMap = m;
            if (screenerApi) screenerApi.resortWithFn();
            else setupResortPending = true;
        })
        .catch(() => {});

    function readiness(s) {
        return setupParts(setupMap ? setupMap[s.ticker] : null);
    }

    function sortKey(s) {
        if (sortMode === 'ready') return readiness(s).total;
        if (sortMode === 'date') return s.last_event_date ? Date.parse(s.last_event_date) : -Infinity;
        const v = s.adjusted_event_return;
        return v == null ? -Infinity : v;
    }

    function setupChipHtml(s) {
        if (!setupMap) return '';
        const m = setupMap[s.ticker];
        if (!m || !m.nearest_ma) return '';
        const p = setupParts(m);
        const dist = (m.ma_dist_pct >= 0 ? '+' : '') + m.ma_dist_pct.toFixed(1) + '%';
        const tip = `${m.nearest_ma} ${dist} (${m.ma_dist_atr} ATR), ` +
            `10-bar range ${m.range10_atr.toFixed(1)} ATR, ` +
            `close at ${((m.pos_in_range10 == null ? 0.5 : m.pos_in_range10) * 100).toFixed(0)}% of range` +
            (m.above_all_ma ? ', above all 4 MAs' : m.above_ema20 ? ', above ema_20' : ', below ema_20');
        const cls = p.total >= 0.7 ? ' tight' : '';
        return `<span class="vsg-setup${cls}" title="${tip}">` +
            `${MA_LABELS[m.nearest_ma] || m.nearest_ma} ${dist}</span>`;
    }

    function scoreChipHtml(s) {
        if (sortMode === 'ready') {
            const p = readiness(s);
            const tip = `setup ${p.total.toFixed(2)} = ` +
                `prox ${p.prox.toFixed(2)} tight ${p.tight.toFixed(2)} ` +
                `hold ${p.hold.toFixed(2)} trend ${p.trend.toFixed(2)}`;
            return `<span class="vsg-score" title="${tip}">${p.total.toFixed(2)}</span>`;
        }
        const v = s.adjusted_event_return;
        if (v == null) return '';
        return `<span class="vsg-score" title="adjusted event return ${v.toFixed(2)} = ` +
            `event_return_pct / (clip(mcap,$200M,$100B)/$100B)^-0.134">${v.toFixed(1)}</span>`;
    }

    function eventExtraHtml(s) {
        const evtLabel = s.last_event_type === 'volume_spike' ? 'S' : s.last_event_type === 'gapper' ? 'G' : '';
        const evtClass = s.last_event_type === 'volume_spike' ? 'spike' : s.last_event_type === 'gapper' ? 'gap' : '';
        let magStr = '';
        if (s.last_event_magnitude != null) {
            if (s.last_event_type === 'volume_spike') {
                magStr = s.last_event_magnitude.toFixed(1) + 'x vol';
            } else if (s.last_event_type === 'gapper') {
                magStr = (s.last_event_magnitude >= 0 ? '+' : '') + (s.last_event_magnitude * 100).toFixed(1) + '% gap';
            }
        }
        const eventDate = compactDate(s.last_event_date);
        return (evtLabel ? `<span class="mini-badge ${evtClass}">${evtLabel}</span>` : '') +
            (magStr ? `<span class="list-extra event-mag">${magStr}</span>` : '') +
            `<span class="vsg-right">` +
                setupChipHtml(s) +
                (eventDate ? `<span class="last-date">${eventDate}</span>` : '') +
                scoreChipHtml(s) +
            `</span>`;
    }

    function listValueFn(s) {
        const v = s.last_event_return != null ? s.last_event_return * 100 : null;
        return {
            text: v != null ? (v >= 0 ? '+' : '') + v.toFixed(1) + '%' : '—',
            cls: v != null ? (v >= 0 ? 'positive' : 'negative') : 'muted',
        };
    }

    document.querySelectorAll('.recency-btn[data-sort]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.sort === sortMode);
    });

    DesktopScreener.init({
        endpoint: 'volspike-gapper-90d',
        accentCss: 'var(--accent-yellow)',
        label: 'Vol Spike & Gapper - 90d',
        weeklyDisposition: 'vsg90',

        sortFn: (stocks) => stocks.sort((a, b) => {
            const d = sortKey(b) - sortKey(a);
            if (d) return d;
            return (b.market_cap || 0) - (a.market_cap || 0) || a.ticker.localeCompare(b.ticker);
        }),

        listValueFn: listValueFn,
        listExtraFn: eventExtraHtml,

        onListRendered: ({ visible }) => {
            document.getElementById('totalStocks').textContent = visible.length;
            document.getElementById('withSpikes').textContent = visible.filter(d => d.spike_day_count > 0).length;
            document.getElementById('withGaps').textContent = visible.filter(d => d.gapper_day_count > 0).length;
        },

        onReady: (api) => {
            screenerApi = api;
            if (setupResortPending) { setupResortPending = false; api.resortWithFn(); }

            document.querySelectorAll('.recency-btn[data-sort]').forEach(btn => {
                btn.addEventListener('click', () => {
                    sortMode = btn.dataset.sort;
                    try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
                    document.querySelectorAll('.recency-btn[data-sort]').forEach(b => {
                        b.classList.toggle('active', b === btn);
                    });
                    api.resortWithFn();
                });
            });
        },
    });
})();
