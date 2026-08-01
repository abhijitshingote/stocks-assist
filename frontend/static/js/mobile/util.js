(function () {
  'use strict';

  const SECTOR_ABBREV = {
    'Technology': 'Tech',
    'Financial Services': 'Fin Svcs',
    'Healthcare': 'Health',
    'Industrials': 'Ind',
    'Consumer Cyclical': 'Cons Cyc',
    'Consumer Defensive': 'Cons Def',
    'Communication Services': 'Comm',
    'Basic Materials': 'Materials',
    'Real Estate': 'RE',
  };

  function escAttr(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function abbrevSector(name) {
    if (!name) return 'All';
    return SECTOR_ABBREV[name] || (name.length > 14 ? name.slice(0, 12) + '…' : name);
  }

  function fmtRet(v) {
    if (v == null) return '—';
    return (v >= 0 ? '+' : '') + v.toFixed(0) + '%';
  }

  function retCls(v) {
    if (v == null) return '';
    return v >= 0 ? 'up' : 'down';
  }

  function msRetCls(v) {
    if (v == null) return '';
    return v >= 0 ? 'ms-positive' : 'ms-negative';
  }

  function msItem(label, val, cls, sub) {
    return '<span class="ms-item"><span class="ms-label">' + label +
      '</span><span class="ms-val ' + (cls || '') + '">' + val + '</span>' +
      (sub ? '<span class="ms-sub">' + sub + '</span>' : '') + '</span>';
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
    if (v >= 1e9) return '$' + (v / 1e9).toFixed(0) + 'B';
    if (v >= 1e6) return '$' + (v / 1e6).toFixed(0) + 'M';
    return '$' + v.toLocaleString();
  }

  function isLowFloat(floatShares) {
    return floatShares != null && floatShares < 20000000;
  }

  function renderMetrics(container, s) {
    if (!container) return;
    let html = '';

    function section(title, items) {
      return '<div class="ms-section"><span class="ms-section-title">' + title +
        '</span><div class="ms-section-row">' + items + '</div></div>';
    }

    let items = '';
    items += msItem('Price', s.current_price ? '$' + s.current_price.toFixed(2) : '—');
    items += msItem('MCap', fmtMktCap(s.market_cap));
    items += msItem('Vol', fmtVol(s.volume));
    items += msItem('$Vol', s.dollar_volume ? fmtMktCap(s.dollar_volume) : '—');
    html += section('Price & Market', items);

    items = '';
    [['1D', 'dr_1'], ['5D', 'dr_5'], ['20D', 'dr_20']].forEach(([l, k]) => {
      items += msItem(l, fmtRet(s[k]), msRetCls(s[k]) + ' ms-val-lg');
    });
    [['60D', 'dr_60'], ['120D', 'dr_120']].forEach(([l, k]) => {
      items += msItem(l, fmtRet(s[k]), msRetCls(s[k]));
    });
    html += section('Returns', items);

    items = '';
    [['T-1', 'rev_growth_t_minus_1'], ['T', 'rev_growth_t'], ['T+1', 'rev_growth_t_plus_1'], ['T+2', 'rev_growth_t_plus_2']].forEach(([l, k]) => {
      items += msItem(l, fmtRet(s[k]), msRetCls(s[k]));
    });
    html += section('Revenue Growth', items);

    items = '';
    [['T-1', 'eps_growth_t_minus_1'], ['T', 'eps_growth_t'], ['T+1', 'eps_growth_t_plus_1'], ['T+2', 'eps_growth_t_plus_2']].forEach(([l, k]) => {
      items += msItem(l, fmtRet(s[k]), msRetCls(s[k]));
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

  function renderTagsStrip(stripEl, stock) {
    if (!stripEl) return;
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
      pills += '<span class="tag-pill ' + cls + '">last ' + label + ': ' + mag + ' (' + stock.last_event_date + ')</span>';
    }

    if (!pills) {
      stripEl.classList.remove('visible');
      stripEl.innerHTML = '';
      return;
    }
    stripEl.innerHTML = pills;
    stripEl.classList.add('visible');
  }

  function setupNotesModal() {
    const overlay = document.getElementById('notesModalOverlay');
    const tickerEl = document.getElementById('notesModalTicker');
    const notesEl = document.getElementById('notesModalNotes');
    const removeBtn = document.getElementById('notesModalRemoveBtn');
    const saveBtn = document.getElementById('notesModalSaveBtn');
    let modalTicker = null;
    let hadNote = false;

    function close() {
      overlay.classList.remove('visible');
      modalTicker = null;
    }

    window._notesOpen = function (ticker, currentNotes, hasExistingNote, onDone) {
      modalTicker = ticker;
      hadNote = !!hasExistingNote;
      tickerEl.textContent = ticker;
      notesEl.value = currentNotes || '';
      removeBtn.style.display = hadNote ? 'inline-block' : 'none';
      saveBtn.textContent = hadNote ? 'Save' : 'Add';
      overlay.classList.add('visible');
      notesEl.focus();
      window._notesOnDone = onDone || null;
    };

    window._notesClose = close;

    async function persist(action) {
      if (!modalTicker) return;
      const notes = notesEl.value.trim();
      try {
        if (action === 'delete') {
          await fetch('/api/frontend/abi-ticker-notes/' + modalTicker, { method: 'DELETE' });
        } else {
          await fetch('/api/frontend/abi-ticker-notes/' + modalTicker, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ notes }),
          });
        }
      } catch (e) {
        console.error('Notes save error', e);
      }
      const t = modalTicker;
      const cb = window._notesOnDone;
      close();
      if (cb) cb(action === 'delete' || !notes ? 'removed' : 'saved', t, notes);
    }

    saveBtn.addEventListener('click', () => persist('save'));
    removeBtn.addEventListener('click', () => persist('delete'));
    document.getElementById('notesModalCancelBtn').addEventListener('click', close);
    overlay.addEventListener('click', e => { if (e.target === overlay) close(); });

    window._wlToggle = async function (ticker, currentlyIn, onDone) {
      try {
        if (currentlyIn) {
          await fetch('/api/frontend/abi-watchlist/' + ticker, { method: 'DELETE' });
          if (onDone) onDone(false, ticker);
        } else {
          await fetch('/api/frontend/abi-watchlist', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ticker }),
          });
          if (onDone) onDone(true, ticker);
        }
      } catch (e) {
        console.error('Watchlist toggle error', e);
      }
    };
  }

  window.MobileUtil = {
    escAttr,
    fmtRet,
    retCls,
    msRetCls,
    msItem,
    fmtVal,
    fmtVol,
    fmtMktCap,
    isLowFloat,
    SECTOR_ABBREV,
    abbrevSector,
    renderMetrics,
    renderTagsStrip,
    setupNotesModal,
  };
})();
