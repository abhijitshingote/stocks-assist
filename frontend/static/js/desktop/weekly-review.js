/* Weekly review queue: live union of vsg90 / strong / top520 / fastrs.
   Disposition (pass / watch / buy / short / exclude) drops the row.
   Pass is cycle-scoped (Sat 00:00 ET → Friday).
*/
(function () {
    const SRC_LABEL = { vsg90: 'VSG', strong: 'STR', top520: '5/20', fastrs: 'RS' };
    const SRC_ORDER = ['vsg90', 'strong', 'top520', 'fastrs'];
    const MONTH_ABBREV = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const SORT_KEY = 'weeklyReviewSort';
    let lastMeta = null;
    let sortMode = 'ati65';
    try {
        const saved = localStorage.getItem(SORT_KEY);
        if (saved === 'best' || saved === 'ati65' || saved === 'sources' || saved === 'dr1' || saved === 'dr5') sortMode = saved;
    } catch (e) {}

    function fmtCutoff(id, spec) {
        if (!spec) return id;
        if (!spec.enabled) return id + '=off';
        return id + ':' + spec.field + '≥' + spec.min;
    }

    function renderMeta(json) {
        lastMeta = json;
        const cycleEl = document.getElementById('wrCycle');
        const cutEl = document.getElementById('wrCutoffs');
        const funEl = document.getElementById('wrFunnel');
        if (cycleEl) {
            cycleEl.textContent = 'cycle Sat ' + (json.cycle || '—') +
                ' → Fri ' + (json.cycle_ends || '—');
        }
        if (cutEl && json.cutoffs) {
            cutEl.innerHTML = Object.keys(json.cutoffs).map(k =>
                '<code title="' + (json.cutoffs[k].note || '') + '">' +
                fmtCutoff(k, json.cutoffs[k]) + '</code>'
            ).join(' ');
        }
        if (funEl && json.funnel) {
            const f = json.funnel;
            funEl.textContent =
                'union ' + (f.union || 0) +
                ' −watch ' + (f.hidden_watch || 0) +
                ' −trade ' + (f.hidden_trade || 0) +
                ' −pass ' + (f.hidden_pass || 0) +
                ' = ' + (f.queue || 0) +
                '  (vsg ' + (f.vsg90 && f.vsg90.after_cutoff) +
                ' str ' + (f.strong && f.strong.after_cutoff) +
                ' 520 ' + (f.top520 && f.top520.after_cutoff) +
                ' rs ' + (f.fastrs && f.fastrs.after_cutoff) + ')';
        }
    }

    function compactDate(dateStr) {
        if (!dateStr) return '';
        const parts = String(dateStr).split('-');
        if (parts.length !== 3) return '';
        const month = MONTH_ABBREV[parseInt(parts[1], 10) - 1];
        if (!month) return '';
        return month + ' ' + parseInt(parts[2], 10);
    }

    function signedPct(v, digits) {
        if (v == null || !Number.isFinite(v)) return null;
        return (v >= 0 ? '+' : '') + v.toFixed(digits) + '%';
    }

    function pill(id, num, unit, when) {
        if (!num) return '';
        return '<span class="wr-pill ' + id + '" title="' + (SRC_LABEL[id] || id) + '">' +
            '<span class="wr-num">' + num + '</span>' +
            (unit ? '<span class="wr-unit">' + unit + '</span>' : '') +
            (when ? '<span class="wr-when">' + when + '</span>' : '') +
            '</span>';
    }

    function sourcePills(s, id) {
        if (id === 'vsg90') {
            const mag = VsgEvent.magStr(s, {compact: true});
            return pill('vsg90', mag, '', compactDate(s.last_event_date));
        }
        if (id === 'strong') {
            return s.adjusted_ti65 != null ? pill('strong', s.adjusted_ti65.toFixed(2), 'ati65') : '';
        }
        if (id === 'top520') {
            const out = [];
            if (s.in_5d && s.dr_5 != null) out.push(pill('top520', signedPct(s.dr_5, 1), '5d'));
            if (s.in_20d && s.dr_20 != null) out.push(pill('top520', signedPct(s.dr_20, 1), '20d'));
            if (!out.length && s.dr_5 != null) out.push(pill('top520', signedPct(s.dr_5, 1), '5d'));
            return out.join('');
        }
        if (id === 'fastrs') {
            const v = signedPct(s.rs_score, 1);
            return v ? pill('fastrs', v, 'rs') : '';
        }
        return '';
    }

    DesktopScreener.init({
        endpointFn: () => '/api/frontend/weekly-review',
        capFilter: 'client',
        capField: 'cap_bucket',
        accentCss: 'var(--accent-green)',
        weeklyDisposition: true,
        transformData: (json) => {
            renderMeta(json);
            return Array.isArray(json.stocks) ? json.stocks : [];
        },
        capFilterFn: (s, cap) => s.cap_bucket === cap,
        sortFn: (stocks) => {
            stocks.sort((a, b) => {
                if (sortMode === 'sources') {
                    const d = (b.sources || []).length - (a.sources || []).length;
                    if (d) return d;
                } else if (sortMode === 'ati65') {
                    const av = a.adjusted_ti65, bv = b.adjusted_ti65;
                    if (av == null && bv == null) return 0;
                    if (av == null) return 1;
                    if (bv == null) return -1;
                    if (bv !== av) return bv - av;
                } else if (sortMode === 'dr1') {
                    const av = a.dr_1, bv = b.dr_1;
                    if (av == null && bv == null) return 0;
                    if (av == null) return 1;
                    if (bv == null) return -1;
                    if (bv !== av) return bv - av;
                } else if (sortMode === 'dr5') {
                    const av = a.dr_5, bv = b.dr_5;
                    if (av == null && bv == null) return 0;
                    if (av == null) return 1;
                    if (bv == null) return -1;
                    if (bv !== av) return bv - av;
                } else {
                    const ar = a.best_rank, br = b.best_rank;
                    if (ar == null && br == null) return 0;
                    if (ar == null) return 1;
                    if (br == null) return -1;
                    if (ar !== br) return ar - br;
                }
                const ns = (b.sources || []).length - (a.sources || []).length;
                if (ns) return ns;
                return (a.ticker || '').localeCompare(b.ticker || '');
            });
            return stocks;
        },
        listExtraFn: (s) => {
            const have = new Set(s.sources || []);
            return SRC_ORDER.filter(id => have.has(id))
                .map(id => sourcePills(s, id))
                .filter(Boolean)
                .join('');
        },
        onReady: (api) => {
            document.querySelectorAll('[data-sort]').forEach(btn => {
                btn.classList.toggle('active', btn.dataset.sort === sortMode);
                btn.addEventListener('click', () => {
                    sortMode = btn.dataset.sort;
                    try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
                    document.querySelectorAll('[data-sort]').forEach(b =>
                        b.classList.toggle('active', b.dataset.sort === sortMode)
                    );
                    api.resortWithFn();
                });
            });
        },
    });
})();
