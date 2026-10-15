/* Theme Discovery — shared desktop + mobile renderer for /theme-discovery, /m/theme-discovery.
 * Data: /api/frontend/theme-discovery → user_data/theme_discovery/<run>/themes.json
 */
const ThemeDiscovery = (() => {
  const API = '/api/frontend/theme-discovery';
  const LIST_LABELS = {
    fastrs: 'Fast RS', strong: 'Strong Stocks', top5d: 'Top Returns 5d',
    top20d: 'Top Returns 20d', vsg90: 'Vol Spike & Gaps 90d',
  };

  function esc(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function pct(v) {
    if (v === null || v === undefined) return '<span class="td-muted">–</span>';
    const cls = v > 0 ? 'up' : v < 0 ? 'down' : '';
    return `<span class="td-pct ${cls}">${v > 0 ? '+' : ''}${Number(v).toFixed(1)}%</span>`;
  }

  function init({ rootId, mobile = false }) {
    const root = document.getElementById(rootId);
    const $ = (sel) => root.querySelector(sel);
    const href = (t) => `${mobile ? '/m' : ''}/stock/${encodeURIComponent(t)}`;
    const tick = (t) => `<a class="td-tk" href="${href(t)}">${esc(t)}</a>`;
    let tab = 'themes';

    function stockRow(s) {
      const badge = s.found_by === 'probe'
        ? '<span class="td-badge probe">probe</span>'
        : '<span class="td-badge screener">screener</span>';
      const listed = s.listed ? ` <span class="td-badge listed">${s.listed === 'trade' ? 'T' : 'W'}</span>` : '';
      const why = `${esc(s.why)}${s.industry ? `<div class="td-sub">${esc(s.industry)}</div>` : ''}`;
      const cells = `
        <td class="td-nowrap">${tick(s.ticker)}${listed}</td>
        <td>${badge}</td>
        <td class="td-nowrap td-muted">${esc(s.cap_bucket || '')}</td>
        <td class="td-num">${pct(s.dr_5)}</td>
        <td class="td-num">${pct(s.dr_20)}</td>
        <td class="td-num">${pct(s.dr_60)}</td>`;
      return mobile
        ? `<tr class="td-head-row">${cells}</tr><tr class="td-why-row"><td colspan="6">${why}</td></tr>`
        : `<tr>${cells}<td class="td-why">${why}</td></tr>`;
    }

    const STOCK_HEAD = `<tr><th>Ticker</th><th>Found by</th><th>Cap</th><th class="td-num">1w</th><th class="td-num">1m</th><th class="td-num">3m</th>${mobile ? '' : '<th>Why it belongs</th>'}</tr>`;

    function themeTags(t, run) {
      const tags = [];
      if (t.previous) tags.push(`<span class="td-badge returning">returning · first seen ${esc(t.first_seen)}</span>`);
      else if (t.first_seen === run) tags.push('<span class="td-badge new">new</span>');
      if (t.kind === 'single') tags.push(`<span class="td-badge single">1 stock · probe found ${(t.confirm_up || []).length} up</span>`);
      if (t.kind === 'previous_single') tags.push('<span class="td-badge single">1 stock · known theme</span>');
      if (t.found_in === 'pass2') tags.push(`<span class="td-badge layer">checklist${t.layer ? `: ${esc(t.layer)}` : ''}</span>`);
      return tags.join(' ');
    }

    function renderUnconfirmed(d) {
      const u = d.unconfirmed || [];
      if (!u.length) return '';
      return `<div class="td-sub">One-stock themes not confirmed by probe (stock moved to QA): ${u.map((x) => `${esc(x.name)} (${tick(x.ticker)}, ${x.confirm_up.length} up)`).join(' · ')}</div>`;
    }

    function renderThemes(d, run) {
      if (!d.themes.length) return '<div class="td-empty">No themes in this run.</div>';
      return d.themes.map((t) => `
        <details class="td-theme">
          <summary class="td-theme-head">
            <span class="td-theme-name">${esc(t.name)}</span>
            <span class="td-muted">${t.stocks.length} from screeners · ${t.probe_added.length} from probe</span>
            ${themeTags(t, run)}
          </summary>
          <div class="td-theme-desc">${esc(t.description)}</div>
          ${t.secular ? `<div class="td-theme-sec">${esc(t.secular)}</div>` : ''}
          <div class="td-table-wrap"><table class="td-table">
            <thead>${STOCK_HEAD}</thead>
            <tbody>${t.stocks.map(stockRow).join('')}${t.probe_added.map(stockRow).join('')}</tbody>
          </table></div>
          ${t.probe_error ? `<div class="td-err">Probe failed: ${esc(t.probe_error)}</div>` : ''}
          ${t.probe_rejected.length ? `<div class="td-sub">Probe names not added: ${t.probe_rejected.map((r) => `${esc(r.ticker)} (${esc(r.reject_reason)})`).join(', ')}</div>` : ''}
        </details>`).join('') + renderUnconfirmed(d);
    }

    function renderQa(d) {
      if (!d.qa.length) return '<div class="td-empty">Every candidate landed in a theme.</div>';
      const row = (q) => {
        const cells = `
          <td class="td-nowrap">${tick(q.ticker)}</td>
          <td class="td-muted">${esc(q.cap_bucket || '')}</td>
          <td class="td-num">${pct(q.dr_5)}</td><td class="td-num">${pct(q.dr_20)}</td><td class="td-num">${pct(q.dr_60)}</td>`;
        const reason = `${esc(q.reason)}${q.industry ? `<div class="td-sub">${esc(q.industry)}</div>` : ''}`;
        const whyNot = esc(q.why_not_theme);
        return mobile
          ? `<tr class="td-head-row">${cells}</tr><tr class="td-why-row"><td colspan="5">${reason}<div class="td-sub">No theme: ${whyNot}</div></td></tr>`
          : `<tr>${cells}<td class="td-why">${reason}</td><td class="td-why td-muted">${whyNot}</td></tr>`;
      };
      const head = `<tr><th>Ticker</th><th>Cap</th><th class="td-num">1w</th><th class="td-num">1m</th><th class="td-num">3m</th>${mobile ? '' : '<th>Why it went up</th><th>Why no theme</th>'}</tr>`;
      return `<div class="td-table-wrap"><table class="td-table">
        <thead>${head}</thead>
        <tbody>${d.qa.map(row).join('')}</tbody>
      </table></div>`;
    }

    function renderFunnel(d) {
      const f = d.funnel;
      const lists = Object.entries(f.lists).map(([id, l]) => `<tr>
        <td class="td-nowrap">${esc(LIST_LABELS[id] || id)}</td>
        <td class="td-muted td-nowrap">top ${l.top_n}${l.per_cap ? ' per cap' : ''} by ${esc(l.rank_field)}${l.filter ? ` · ${esc(l.filter)}` : ''}</td>
        <td class="td-num">${l.pool}</td>
        <td class="td-num">${l.picked.length}</td>
        <td>${l.picked.map(tick).join(' ')}</td>
      </tr>`).join('');
      const cands = f.candidates.map((c) => `<tr>
        <td class="td-nowrap">${tick(c.ticker)}</td>
        <td class="td-muted">${esc(c.cap_bucket || '')}</td>
        <td class="td-num">${pct(c.dr_5)}</td><td class="td-num">${pct(c.dr_20)}</td><td class="td-num">${pct(c.dr_60)}</td>
        <td>${esc(c.industry || '')}</td>
        <td class="td-muted">${(c.picked_by || []).map((p) => esc(LIST_LABELS[p] || p)).join(', ')}</td>
      </tr>`).join('');
      const dropped = f.dropped.length
        ? `<div class="td-sub">Dropped by the ${f.max_per_industry}-per-industry limit: ${f.dropped.map((x) => esc(x.ticker)).join(', ')}</div>` : '';
      return `
        <div class="td-sub">Universe: ${esc(f.universe.cap_buckets.join(' + '))} caps from the weekly screeners, excluding ${esc(f.universe.exclude_industries.join(', '))} · ${f.universe.rows ?? f.universe.large_mega_rows} stocks${f.max_per_industry ? ` · max ${f.max_per_industry} per industry` : ''}${f.universe.hidden_pass ? ` · ${f.universe.hidden_pass} passes hidden by Weekly Review` : ''}</div>
        <div class="td-table-wrap"><table class="td-table">
          <thead><tr><th>Screener</th><th>Pick rule</th><th class="td-num">Pool</th><th class="td-num">Picked</th><th>Tickers</th></tr></thead>
          <tbody>${lists}</tbody>
        </table></div>
        <div class="td-h">Candidates (${f.candidates.length})</div>
        <div class="td-table-wrap"><table class="td-table">
          <thead><tr><th>Ticker</th><th>Cap</th><th class="td-num">1w</th><th class="td-num">1m</th><th class="td-num">3m</th><th>Industry</th><th>Picked by</th></tr></thead>
          <tbody>${cands}</tbody>
        </table></div>
        ${dropped}`;
    }

    function render(payload) {
      const sel = $('#tdRun');
      sel.innerHTML = (payload.runs || []).map((r) => `<option value="${esc(r)}"${r === payload.run ? ' selected' : ''}>${esc(r)}</option>`).join('');
      const d = payload.data;
      if (!d) {
        $('#tdMeta').textContent = '';
        $('#tdBody').innerHTML = '<div class="td-empty">No runs yet. Run: docker compose exec backend python -m theme_discovery.run</div>';
        return;
      }
      const s = payload.summary || {};
      $('#tdMeta').textContent = [
        d.weekly_cycle ? `weekly cycle ${d.weekly_cycle}` : null,
        `${d.funnel.candidates.length} candidates`,
        `${d.themes.length} themes`,
        `${d.qa.length} in QA`,
        s.total_cost_usd !== undefined ? `$${Number(s.total_cost_usd).toFixed(2)}` : null,
      ].filter(Boolean).join(' · ');
      root.querySelectorAll('.td-tab').forEach((b) => {
        b.classList.toggle('active', b.dataset.tab === tab);
        const n = b.dataset.tab === 'themes' ? d.themes.length : b.dataset.tab === 'qa' ? d.qa.length : d.funnel.candidates.length;
        b.querySelector('.td-count').textContent = n;
      });
      $('#tdBody').innerHTML = tab === 'themes' ? renderThemes(d, payload.run) : tab === 'qa' ? renderQa(d) : renderFunnel(d);
    }

    let current = null;
    async function load(run) {
      $('#tdBody').innerHTML = '<div class="td-empty">Loading…</div>';
      try {
        const r = await fetch(API + (run ? `?run=${encodeURIComponent(run)}` : ''));
        current = await r.json();
        render(current);
      } catch (e) {
        $('#tdBody').innerHTML = `<div class="td-err">Failed to load: ${esc(e.message)}</div>`;
      }
    }

    $('#tdRun').addEventListener('change', (e) => load(e.target.value));
    root.querySelectorAll('.td-tab').forEach((b) => b.addEventListener('click', () => {
      tab = b.dataset.tab;
      if (current) render(current);
    }));
    load(null);
  }

  return { init };
})();
