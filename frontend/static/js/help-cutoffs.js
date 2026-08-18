fetch('/api/frontend/weekly-review-config').then(r => r.json()).then(d => {
  const el = document.getElementById('helpWrCutoffs');
  if (!el || !d.cutoffs) return;
  const parts = Object.keys(d.cutoffs).map(k => {
    const s = d.cutoffs[k];
    const val = s.enabled ? (s.field + ' ≥ ' + s.min) : 'off';
    return '<code>' + k + '</code> ' + val + (s.note ? ' <span class="help-route">(' + s.note + ')</span>' : '');
  });
  el.innerHTML = 'cycle Sat ' + d.cycle + ' → Fri ' + d.cycle_ends + '. ' + parts.join(' · ');
}).catch(() => {
  const el = document.getElementById('helpWrCutoffs');
  if (el) el.textContent = 'failed to load WEEKLY_REVIEW_CUTOFFS';
});
