/* Weekly review queue: live union of vsg90 / strong / top520 / fastrs.
   Disposition (pass / watch / buy / short / exclude) drops the row.
   Pass is cycle-scoped (Sat 00:00 ET → Friday).
*/
(function () {
    const SRC_LABEL = { vsg90: 'VSG', strong: 'STR', top520: '5/20', fastrs: 'RS' };
    const SORT_KEY = 'weeklyReviewSort';
    let lastMeta = null;
    let screenerApi = null;
    let sortMode = 'best';
    try {
        const saved = localStorage.getItem(SORT_KEY);
        if (saved === 'best' || saved === 'sources' || saved === 'dr1') sortMode = saved;
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

    function sourceChips(s) {
        return (s.sources || []).map(id =>
            '<span class="wr-src ' + id + '">' + (SRC_LABEL[id] || id) + '</span>'
        ).join('');
    }

    async function dispose(kind) {
        const ticker = document.getElementById('detailTicker')?.textContent?.trim();
        if (!ticker || ticker === '—') return;
        const stock = (screenerApi && screenerApi.getState().allStocks || [])
            .find(s => s.ticker === ticker);
        const sources = (stock && stock.sources) || [];
        try {
            if (kind === 'pass') {
                await fetch('/api/frontend/abi-passes', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ticker, sources }),
                });
            } else if (kind === 'buy' || kind === 'short') {
                await fetch('/api/frontend/abi-trades', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ticker, side: kind }),
                });
            }
        } catch (e) {
            console.error('disposition failed', e);
        }
        window.dispatchEvent(new CustomEvent('abi-exclude-changed', {
            detail: { action: 'saved', ticker },
        }));
    }

    DesktopScreener.init({
        endpointFn: () => '/api/frontend/weekly-review',
        capFilter: 'client',
        capField: 'cap_bucket',
        accentCss: 'var(--accent-green)',
        removeOnWatch: true,
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
                } else if (sortMode === 'dr1') {
                    const av = a.dr_1, bv = b.dr_1;
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
        listPrefixFn: (s) => sourceChips(s),
        listExtraFn: (s) => {
            const bits = [];
            if (s.best_rank != null) {
                bits.push(
                    (SRC_LABEL[s.best_source] || s.best_source || '?') +
                    ' #' + s.best_rank + '/' + s.best_rank_n
                );
            }
            if (s.adjusted_ti65 != null) bits.push('ati65 ' + s.adjusted_ti65.toFixed(2));
            if (s.adjusted_rs_score != null) bits.push('ars ' + s.adjusted_rs_score.toFixed(2));
            return bits.length
                ? '<span class="list-extra">' + bits.join(' · ') + '</span>'
                : '';
        },
        onReady: (api) => {
            screenerApi = api;
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
            document.getElementById('wrDisp')?.addEventListener('click', (e) => {
                const btn = e.target.closest('[data-disp]');
                if (!btn) return;
                dispose(btn.dataset.disp);
            });
        },
    });
})();
