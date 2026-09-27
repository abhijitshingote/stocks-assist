/* Revenue Acceleration — shared by desktop (rev_accel.html) and mobile (pages/rev-accel.js).
   Fields come from /api/frontend/rev-accel/<cap> (revenue_acceleration table). */
window.RevAccel = (function () {
    'use strict';

    const SORTS = [
        { id: 'score', label: 'Score', key: 'rev_accel_score', title: '100 × (0.4·ln((1+y0)/(1+y1)) + 0.3·ln((1+y0)/(1+y2)) + 0.3·ln((1+ttm)/(1+ttm_prev)))' },
        { id: 'a1', label: 'Δ1Q', key: 'rev_accel_1q', title: 'yoy_q0 − yoy_q1 (pp)' },
        { id: 'yoy', label: 'Q YoY', key: 'rev_yoy_q0', title: 'Latest quarter YoY' },
        { id: 'ttm', label: 'TTM YoY', key: 'rev_ttm_yoy', title: 'sum(q0..q3) / sum(q4..q7) − 1' },
        { id: 'dr1', label: '1D', key: 'dr_1', title: '1D return' },
    ];

    const FILTERS = [
        { id: 'streak2', label: 'Streak 2+', title: 'rev_accel_streak ≥ 2 (yoy rising 2+ quarters)', test: s => (s.rev_accel_streak || 0) >= 2 },
        { id: 'ttmup', label: 'TTM ↑', title: 'rev_ttm_accel > 0', test: s => s.rev_ttm_accel != null && s.rev_ttm_accel > 0 },
        { id: 'fwdup', label: 'Fwd ↑', title: 'next-Q consensus YoY > yoy_q0', test: s => s.rev_fwd_accel != null && s.rev_fwd_accel > 0 },
        { id: 'beat', label: 'Beat', title: 'q0 revenue > consensus', test: s => s.rev_surprise_q0 != null && s.rev_surprise_q0 > 0 },
    ];

    function sortBy(stocks, sortId) {
        const key = (SORTS.find(x => x.id === sortId) || SORTS[0]).key;
        return stocks.sort((a, b) => {
            const av = a[key], bv = b[key];
            if (av == null && bv == null) return 0;
            if (av == null) return 1;
            if (bv == null) return -1;
            return bv - av;
        });
    }

    function passes(s, activeFilters) {
        return FILTERS.every(f => !activeFilters.has(f.id) || f.test(s));
    }

    function fmtPct(v) {
        if (v == null) return '—';
        return (v >= 0 ? '+' : '') + v.toFixed(0) + '%';
    }

    function fmtPp(v) {
        if (v == null) return '—';
        return (v >= 0 ? '+' : '') + v.toFixed(0) + 'pp';
    }

    function fmtRev(v) {
        if (!v) return '—';
        if (v >= 1e9) return '$' + (v / 1e9).toFixed(1) + 'B';
        if (v >= 1e6) return '$' + (v / 1e6).toFixed(0) + 'M';
        return '$' + (v / 1e3).toFixed(0) + 'K';
    }

    function cls(v) {
        if (v == null) return '';
        return v >= 0 ? 'ms-positive' : 'ms-negative';
    }

    function shortDate(iso) {
        if (!iso) return '';
        const [y, m] = iso.split('-');
        const mon = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][+m - 1];
        return mon + y.slice(2);
    }

    function section(title, body) {
        return '<div class="ms-section"><span class="ms-section-title">' + title +
            '</span><div class="ms-section-row">' + body + '</div></div>';
    }

    function barsHtml(s) {
        const qs = s.rev_quarters || [];
        if (!qs.length) return '';
        const maxAbs = Math.max(1, ...qs.map(q => Math.abs(q.yoy || 0)));
        const cols = qs.map(q => {
            const y = q.yoy;
            const h = y == null ? 0 : Math.max(4, Math.round(Math.abs(y) / maxAbs * 100));
            const tip = q.d + ' · ' + fmtRev(q.rev) + ' · ' + (y == null ? 'no YoY' : fmtPct(y) + ' YoY');
            return '<div class="ra-col" title="' + tip + '">' +
                '<span class="ra-val ' + cls(y) + '">' + (y == null ? '—' : fmtPct(y)) + '</span>' +
                '<div class="ra-track"><div class="ra-bar ' + (y != null && y < 0 ? 'neg' : 'pos') +
                '" style="height:' + h + '%"></div></div>' +
                '<span class="ra-lbl">' + shortDate(q.d) + '</span></div>';
        }).join('');
        return '<div class="ms-section"><span class="ms-section-title">Quarterly Rev YoY</span>' +
            '<div class="ra-bars">' + cols + '</div></div>';
    }

    // msItem(label, val, cls, sub) — same signature on desktop helpers and MobileUtil.
    function metricsHtml(s, msItem) {
        let html = barsHtml(s);

        let items = '';
        items += msItem('Q YoY', fmtPct(s.rev_yoy_q0), cls(s.rev_yoy_q0) + ' ms-val-lg');
        items += msItem('Δ1Q', fmtPp(s.rev_accel_1q), cls(s.rev_accel_1q) + ' ms-val-lg');
        items += msItem('Δ2Q', fmtPp(s.rev_accel_2q), cls(s.rev_accel_2q));
        items += msItem('Streak', s.rev_accel_streak != null ? s.rev_accel_streak + 'Q' : '—',
            s.rev_accel_streak >= 2 ? 'ms-positive' : '');
        html += section('Rev Acceleration', items);

        items = '';
        items += msItem('TTM YoY', fmtPct(s.rev_ttm_yoy), cls(s.rev_ttm_yoy));
        items += msItem('TTM Δ', fmtPp(s.rev_ttm_accel), cls(s.rev_ttm_accel));
        items += msItem('TTM Rev', fmtRev(s.ttm_rev));
        items += msItem('Score', s.rev_accel_score != null ? s.rev_accel_score.toFixed(1) : '—');
        html += section('TTM', items);

        items = '';
        items += msItem('QoQ', fmtPct(s.rev_qoq_q0), cls(s.rev_qoq_q0));
        items += msItem('Beat', fmtPct(s.rev_surprise_q0), cls(s.rev_surprise_q0));
        items += msItem('Fwd YoY', fmtPct(s.rev_fwd_yoy_est), cls(s.rev_fwd_yoy_est));
        items += msItem('Fwd Δ', fmtPp(s.rev_fwd_accel), cls(s.rev_fwd_accel));
        html += section('Last Q · Next Q est' + (s.next_date ? ' (' + shortDate(s.next_date) + ')' : ''), items);

        return html;
    }

    // Compact list chip: "Q +106% +21pp"
    function listChip(s) {
        return fmtPct(s.rev_yoy_q0) + ' ' + fmtPp(s.rev_accel_1q);
    }

    return { SORTS, FILTERS, sortBy, passes, metricsHtml, listChip, fmtPct, fmtPp };
})();
