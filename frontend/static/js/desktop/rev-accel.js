/* Revenue Acceleration (desktop). Base screen is server-side (REV_ACCEL_CRITERIA);
   chips AND extra filters, sort buttons pick the key. Shared logic in /static/js/rev-accel.js. */
(function () {
    const RA = window.RevAccel;
    const SORT_KEY = 'revAccelSort';
    const FILTER_KEY = 'revAccelFilters';

    let sortMode = 'score';
    let active = new Set();
    try {
        const s = localStorage.getItem(SORT_KEY);
        if (RA.SORTS.some(x => x.id === s)) sortMode = s;
        const f = JSON.parse(localStorage.getItem(FILTER_KEY));
        if (Array.isArray(f)) f.filter(id => RA.FILTERS.some(x => x.id === id)).forEach(id => active.add(id));
    } catch (e) {}

    document.getElementById('raFilters').innerHTML = RA.FILTERS.map(f =>
        `<button type="button" class="recency-btn${active.has(f.id) ? ' active' : ''}" data-ra-filter="${f.id}" title="${f.title}">` +
        `${f.label} <span class="count" id="raCount-${f.id}">0</span></button>`
    ).join('');
    document.getElementById('raSorts').innerHTML = RA.SORTS.map(x =>
        `<button type="button" class="recency-btn${x.id === sortMode ? ' active' : ''}" data-ra-sort="${x.id}" title="${x.title}">${x.label}</button>`
    ).join('');

    function extraHtml(s) {
        const a1 = s.rev_accel_1q;
        const tip = `Q YoY ${RA.fmtPct(s.rev_yoy_q0)} (prev ${RA.fmtPct(s.rev_yoy_q1)}), ` +
            `TTM ${RA.fmtPct(s.rev_ttm_yoy)}, streak ${s.rev_accel_streak || 0}Q`;
        const streak = (s.rev_accel_streak || 0) >= 2
            ? `<span class="vsg-setup tight" title="YoY rising ${s.rev_accel_streak} quarters">${s.rev_accel_streak}Q↑</span>` : '';
        return `<span class="vsg-right">` + streak +
            `<span class="list-extra" title="${tip}">${RA.fmtPct(s.rev_yoy_q0)} ` +
            `<span class="${a1 != null && a1 >= 0 ? 'ra-up' : 'ra-down'}">${RA.fmtPp(a1)}</span></span>` +
            `</span>`;
    }

    DesktopScreener.init({
        endpoint: 'rev-accel',
        accentCss: 'var(--accent-green)',
        label: 'Rev Accel',
        weeklyDisposition: 'revaccel',

        sortFn: (stocks) => RA.sortBy(stocks, sortMode),
        filterFn: (s) => RA.passes(s, active),
        listExtraFn: extraHtml,
        prependMetricsFn: (s, helpers) => RA.metricsHtml(s, helpers.msItem),

        onListRendered: ({ visible, stocks }) => {
            document.getElementById('totalStocks').textContent = visible.length;
            RA.FILTERS.forEach(f => {
                const el = document.getElementById('raCount-' + f.id);
                if (el) el.textContent = stocks.filter(f.test).length;
            });
        },

        onReady: (api) => {
            document.querySelectorAll('[data-ra-sort]').forEach(btn => {
                btn.addEventListener('click', () => {
                    sortMode = btn.dataset.raSort;
                    try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
                    document.querySelectorAll('[data-ra-sort]').forEach(b => b.classList.toggle('active', b === btn));
                    api.resortWithFn();
                });
            });
            document.querySelectorAll('[data-ra-filter]').forEach(btn => {
                btn.addEventListener('click', () => {
                    const id = btn.dataset.raFilter;
                    if (active.has(id)) active.delete(id); else active.add(id);
                    try { localStorage.setItem(FILTER_KEY, JSON.stringify([...active])); } catch (e) {}
                    btn.classList.toggle('active', active.has(id));
                    api.rerenderFn();
                });
            });
        },
    });
})();
