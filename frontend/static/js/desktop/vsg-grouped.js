/* Vol Spike & Gapper — bucketed views (weekly / monthly).
   Same dataset/endpoint as /volspike-gapper; rows are grouped into calendar
   buckets of last_event_date and ranked inside a bucket by conviction score.

   Usage: VsgGrouped.init({ bucket: 'week' | 'month', label, collapseStorageKey })
   Requires DesktopScreener plus the topbar controls from the page template
   (.recency-btn, #withSpikes, #withGaps, #count1D/#count5D/#count20D).

   The monthly profile also pulls /api/frontend/volspike-gapper-setup and ranks
   primarily by how tightly price is consolidating on one of the 4 MAs.
*/

window.VsgGrouped = (function () {
    const MONTH_ABBREV = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const MONTH_FULL = ['January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'];

    const pad = (n) => String(n).padStart(2, '0');

    // ── Bucketers: key must sort lexicographically by chronology ────
    const BUCKETS = {
        // Monday (ISO week start) of the event date, as YYYY-MM-DD.
        week: {
            keyFn: (dateStr) => {
                const [y, m, d] = dateStr.split('-').map(Number);
                const date = new Date(y, m - 1, d);
                date.setDate(date.getDate() - ((date.getDay() + 6) % 7));
                return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
            },
            labelFn: (key) => {
                const [y, m, d] = key.split('-').map(Number);
                const start = new Date(y, m - 1, d);
                const end = new Date(y, m - 1, d + 4);
                const endStr = start.getMonth() === end.getMonth()
                    ? `${end.getDate()}`
                    : `${MONTH_ABBREV[end.getMonth()]} ${end.getDate()}`;
                return `Week of ${MONTH_ABBREV[start.getMonth()]} ${start.getDate()}–${endStr}`;
            },
            unknownLabel: 'Unknown Week',
        },
        month: {
            keyFn: (dateStr) => dateStr.slice(0, 7),
            labelFn: (key) => {
                const [y, m] = key.split('-').map(Number);
                return `${MONTH_FULL[m - 1]} ${y}`;
            },
            unknownLabel: 'Unknown Month',
        },
    };

    // ── Intra-bucket ranking score ─────────────────────────────────
    // Additive terms, all in log units (except setup) so no single one can run away.
    //   move    log2(1 + r/0.05)         5%→1.0  15%→2.0  30%→2.8  80%→4.1
    //   size    log10(mcap/1e9), capped  1B→0    10B→1    100B→2  (cap per profile)
    //   vol     log2(volRatio/3.5)       spikes only; 3.5x→0  7x→0.4  14x→0.8
    //   novelty 1 - log2(events365)/3    1 event→0.4, 8+ events→0
    //   setup   0..1 base/consolidation readiness (see setupParts); needs the
    //           /api/frontend/volspike-gapper-setup map, so weighted only where asked.
    // Weekly weights are set so a mid/large cap on an ordinary move outranks a
    // $1B name on the same move, while a genuine outlier move (>~40%) on a
    // small cap still clears the biggest names in the bucket. Monthly instead
    // leads with setup + size: the event only qualifies a name, the base decides.
    const PROFILES = {
        week:  { move: 1.0, size: 0.8, vol: 0.4, novel: 0.4, setup: 0,   sizeCap: 3 },
        // Setup outranges size (3.5 vs 2.5 max) so a clean base on a $5B name
        // beats a mega cap that is extended, but size breaks the tie among bases.
        month: { move: 0.3, size: 1.0, vol: 0.15, novel: 0.15, setup: 3.5, sizeCap: 2.5 },
    };

    const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
    const log2 = (v) => Math.log(v) / Math.LN2;

    // Consolidation near one of the 4 MAs (ema_10 / ema_20 / dma_50 / dma_200),
    // measured on the last 10 bars. Returns 0..1; 0 when metrics are missing.
    //   prox   |close − nearest MA| in ATRs   0→1, ≥1.5 ATR→0 (ATR-relative so a
    //                                         low-vol mega cap is not flattered)
    //   tight   10-bar high-low range / ATR   ≤1.5 ATR→1, ≥3.5 ATR→0
    //   hold    close position in that range  ≥0.9→1, ≤0.4→0 (not rolling over)
    //   trend   above all 4 MAs→1, above ema_20 only→0.6, else 0
    const SETUP_W = { prox: 0.35, tight: 0.25, hold: 0.20, trend: 0.20 };

    const MA_LABELS = { ema_10: 'E10', ema_20: 'E20', dma_50: 'D50', dma_200: 'D200' };

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

    function scoreParts(s, W, setupMetrics) {
        // Returns are stored as fractions; clamp guards against bad rows
        // (the source table has at least one 9909999 return).
        const r = clamp(s.last_event_return || 0, 0, 3);
        const mcapB = (s.market_cap || 0) / 1e9;
        const spikeDates = s.volume_spike_days || [];
        const gapDates = s.gap_days || [];
        const events = new Set(spikeDates.concat(gapDates)).size;

        const move = log2(1 + r / 0.05);
        const size = mcapB > 0 ? clamp(Math.log10(mcapB), -1, W.sizeCap) : -1;
        const vol = s.last_event_magnitude
            ? clamp(log2(s.last_event_magnitude / 3.5), 0, 1.5)
            : 0;
        const novelty = Math.max(0, 1 - log2(Math.max(events, 1)) / 3);
        const setup = W.setup ? setupParts(setupMetrics) : null;

        return {
            move, size, vol, novelty, setup,
            total: W.move * move + W.size * size + W.vol * vol + W.novel * novelty +
                (setup ? W.setup * setup.total : 0),
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

    function init(options) {
        const bucket = BUCKETS[options.bucket];
        if (!bucket) throw new Error('VsgGrouped: unknown bucket ' + options.bucket);
        const W = PROFILES[options.bucket];

        let currentRecency = 'all';
        let recencyCutoff = null;
        let screenerApi = null;
        let setupMap = null;
        let setupResortPending = false;
        const scoreCache = new Map();

        // Setup metrics arrive on their own request; re-sort once they land
        // (either here, or from onReady if the engine is not up yet).
        if (W.setup) {
            fetch('/api/frontend/volspike-gapper-setup')
                .then(r => r.json())
                .then(m => {
                    if (!m || m.error) return;
                    setupMap = m;
                    scoreCache.clear();
                    if (screenerApi) screenerApi.resortWithFn();
                    else setupResortPending = true;
                })
                .catch(() => {});
        }

        function convictionScore(s) {
            let p = scoreCache.get(s.ticker);
            if (!p) {
                p = scoreParts(s, W, setupMap ? setupMap[s.ticker] : null);
                scoreCache.set(s.ticker, p);
            }
            return p;
        }

        function bucketKey(dateStr) {
            if (!dateStr) return null;
            try { return bucket.keyFn(dateStr); } catch { return null; }
        }

        function bucketLabel(key) {
            if (key === 'Unknown') return bucket.unknownLabel;
            try { return bucket.labelFn(key); } catch { return key; }
        }

        function recencyDays(filter) {
            if (filter === 'all') return null;
            const n = parseInt(filter, 10);
            return Number.isFinite(n) ? n : null;
        }

        // Cutoff date (inclusive) for the last N trading days, anchored to the
        // trading days that actually appear in the dataset (handles holidays/late data).
        function cutoffStrForTradingDays(days, stocks) {
            const uniq = Array.from(new Set(
                stocks.map(d => d.last_event_date).filter(Boolean)
            )).sort().reverse();
            if (!uniq.length) return null;
            return uniq[Math.min(days, uniq.length) - 1];
        }

        function recomputeCutoff() {
            const days = recencyDays(currentRecency);
            if (days == null || !screenerApi) { recencyCutoff = null; return; }
            recencyCutoff = cutoffStrForTradingDays(days, screenerApi.getState().filteredStocks);
        }

        // Nearest MA + signed distance, e.g. "E10 +0.8%". Only on setup-weighted
        // buckets; 'tight' class marks a base that scores in the top band.
        function setupChipHtml(s) {
            if (!W.setup || !setupMap) return '';
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

        function eventExtraHtml(s) {
            const p = convictionScore(s);
            let tip = `score ${p.total.toFixed(2)} = move ${(W.move * p.move).toFixed(2)} ` +
                `+ size ${(W.size * p.size).toFixed(2)} + vol ${(W.vol * p.vol).toFixed(2)} ` +
                `+ novelty ${(W.novel * p.novelty).toFixed(2)}`;
            if (p.setup) {
                tip += ` + setup ${(W.setup * p.setup.total).toFixed(2)} ` +
                    `[prox ${p.setup.prox.toFixed(2)} tight ${p.setup.tight.toFixed(2)} ` +
                    `hold ${p.setup.hold.toFixed(2)} trend ${p.setup.trend.toFixed(2)}]`;
            }
            // Date chip matters here: rows within a bucket are no longer date-ordered.
            const eventDate = compactDate(s.last_event_date);
            return VsgEvent.extraHtml(s) +
                `<span class="vsg-right">` +
                    setupChipHtml(s) +
                    (eventDate ? `<span class="last-date">${eventDate}</span>` : '') +
                    `<span class="vsg-score" title="${tip}">${p.total.toFixed(1)}</span>` +
                `</span>`;
        }

        DesktopScreener.init({
            endpoint: 'volspike-gapper',
            accentCss: 'var(--accent-yellow)',
            label: options.label,

            // Buckets stay newest-first (renderGroupedList keeps first-seen order);
            // within a bucket, rank by conviction score instead of event date.
            sortFn: (stocks) => stocks.sort((a, b) => {
                const aB = bucketKey(a.last_event_date) || '', bB = bucketKey(b.last_event_date) || '';
                if (aB !== bB) return bB.localeCompare(aB);
                const d = convictionScore(b).total - convictionScore(a).total;
                if (d) return d;
                return (b.market_cap || 0) - (a.market_cap || 0) || a.ticker.localeCompare(b.ticker);
            }),

            // Recency filter (AND-ed with sector/industry by the engine).
            filterFn: (s) => {
                if (recencyDays(currentRecency) == null) return true;
                if (!recencyCutoff) return false;
                return s.last_event_date && s.last_event_date >= recencyCutoff;
            },

            groupByFn: (s) => bucketKey(s.last_event_date) || 'Unknown',
            groupLabelFn: bucketLabel,
            groupCollapseStorageKey: options.collapseStorageKey,

            listExtraFn: eventExtraHtml,

            onDataLoaded: () => { recomputeCutoff(); },

            onListRendered: ({ visible, stocks }) => {
                document.getElementById('totalStocks').textContent = visible.length;
                document.getElementById('withSpikes').textContent = visible.filter(d => d.spike_day_count > 0).length;
                document.getElementById('withGaps').textContent = visible.filter(d => d.gapper_day_count > 0).length;
                const countForDays = (days) => {
                    const c = cutoffStrForTradingDays(days, stocks);
                    if (!c) return 0;
                    return stocks.filter(d => d.last_event_date && d.last_event_date >= c).length;
                };
                document.getElementById('count1D').textContent = countForDays(1);
                document.getElementById('count5D').textContent = countForDays(5);
                document.getElementById('count20D').textContent = countForDays(20);
            },

            onReady: (api) => {
                screenerApi = api;
                if (setupResortPending) { setupResortPending = false; api.resortWithFn(); }

                document.querySelectorAll('.recency-btn').forEach(btn => {
                    btn.addEventListener('click', () => {
                        if (!btn.dataset.filter) return;
                        document.querySelectorAll('.recency-btn').forEach(b => b.classList.remove('active'));
                        btn.classList.add('active');
                        currentRecency = btn.dataset.filter;
                        recomputeCutoff();
                        api.rerenderFn();
                    });
                });
            },
        });
    }

    return { init };
})();
