/* Top 5D/20D.
   Universe: liquid, top 30 by adjusted_dr_5 ∪ top 30 by adjusted_dr_20 (All, then cap-filter).
   Sort toggle: mcap-adjusted 5d return vs raw dr_5 vs monthly setup/readiness
   (setupParts from vsg-90d / Strong Stocks, as-is).
   Flat list — no date grouping.
*/
(function () {
    const SORT_KEY = 'topReturns520Sort';
    const MA_LABELS = { ema_10: 'E10', ema_20: 'E20', dma_50: 'D50', dma_200: 'D200' };
    const SETUP_W = { prox: 0.35, tight: 0.25, hold: 0.20, trend: 0.20 };
    const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

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

    let sortMode = 'adj';
    try {
        const saved = localStorage.getItem(SORT_KEY);
        if (saved === 'flat' || saved === 'adj' || saved === 'ready') sortMode = saved;
    } catch (e) {}

    let screenerApi = null;
    let setupMap = null;
    let setupResortPending = false;

    fetch('/api/frontend/strong-stocks-setup')
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
        const v = sortMode === 'adj' ? s.adjusted_dr_5 : s.dr_5;
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
        if (sortMode === 'flat') {
            const v = s.dr_5;
            if (v == null) return '';
            return `<span class="vsg-score" title="raw dr_5 ${v.toFixed(1)}%">${v.toFixed(1)}</span>`;
        }
        const v = s.adjusted_dr_5;
        if (v == null) return '';
        return `<span class="vsg-score" title="adjusted dr_5 ${v.toFixed(1)} = ` +
            `dr_5 / (clip(mcap,$500M,$100B)/$100B)^-0.192">${v.toFixed(1)}</span>`;
    }

    function extraHtml(s) {
        const badges =
            (s.in_5d ? '<span class="mini-badge spike">5</span>' : '') +
            (s.in_20d ? '<span class="mini-badge gap">20</span>' : '');
        return badges + `<span class="vsg-right">` +
            setupChipHtml(s) +
            scoreChipHtml(s) +
        `</span>`;
    }

    function listValueFn(s) {
        const v = s.dr_5;
        return {
            text: v != null ? (v >= 0 ? '+' : '') + v.toFixed(1) + '%' : '—',
            cls: v != null ? (v >= 0 ? 'positive' : 'negative') : 'muted',
        };
    }

    document.querySelectorAll('.recency-btn[data-sort]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.sort === sortMode);
    });

    DesktopScreener.init({
        endpoint: 'top-returns-5-20',
        capFilter: 'client',
        capFilterFn: (s, cap) => s.cap_bucket === cap,
        accentCss: 'var(--accent-green)',
        label: 'Top 5D/20D',
        weeklyDisposition: 'top520',

        sortFn: (stocks) => stocks.sort((a, b) => {
            const d = sortKey(b) - sortKey(a);
            if (d) return d;
            return (b.market_cap || 0) - (a.market_cap || 0) || a.ticker.localeCompare(b.ticker);
        }),

        listValueFn: listValueFn,
        listExtraFn: extraHtml,

        onListRendered: ({ visible }) => {
            document.getElementById('totalStocks').textContent = visible.length;
            const in5 = document.getElementById('in5d');
            const in20 = document.getElementById('in20d');
            if (in5) in5.textContent = visible.filter(s => s.in_5d).length;
            if (in20) in20.textContent = visible.filter(s => s.in_20d).length;
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
