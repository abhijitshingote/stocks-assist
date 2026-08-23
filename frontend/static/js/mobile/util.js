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
      const mag = VsgEvent.magStr(stock, {compact: true});
      pills += '<span class="tag-pill ' + VsgEvent.tagClass(stock) + '">last ' +
        VsgEvent.badge(stock) + ': ' + mag + ' (' + stock.last_event_date + ')</span>';
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
    if (!overlay) return;
    const sheet = overlay.querySelector('.wl-modal.notes-sheet');
    const tickerEl = document.getElementById('notesModalTicker');
    const notesEl = document.getElementById('notesModalNotes');
    const viewEl = document.getElementById('notesModalView');
    const removeBtn = document.getElementById('notesModalRemoveBtn');
    const saveBtn = document.getElementById('notesModalSaveBtn');
    const editBtn = document.getElementById('notesModalEditBtn');
    let modalTicker = null;
    let hadNote = false;
    let savedNotes = '';

    function parseNotesMarkdown(text) {
      if (typeof marked !== 'undefined' && marked.parse) {
        return marked.parse(text, { breaks: true });
      }
      return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\n/g, '<br>');
    }

    function renderView(text) {
      if (!viewEl) return;
      const raw = (text || '').trim();
      if (!raw) {
        viewEl.innerHTML = '<em>No Abi ticker notes</em>';
        viewEl.classList.add('empty');
        return;
      }
      viewEl.classList.remove('empty');
      viewEl.innerHTML = parseNotesMarkdown(raw);
    }

    function setMode(mode) {
      if (sheet) sheet.classList.toggle('editing', mode === 'edit');
    }

    function fillEditor(notes) {
      tickerEl.textContent = modalTicker;
      notesEl.value = notes || '';
      removeBtn.style.display = hadNote ? 'inline-block' : 'none';
      saveBtn.textContent = hadNote ? 'Save' : 'Add';
    }

    function close() {
      overlay.classList.remove('visible');
      if (sheet) sheet.classList.remove('editing');
      modalTicker = null;
    }

    window._notesOpen = function (ticker, currentNotes, hasExistingNote, onDone, opts) {
      const wantEdit = !!(opts && opts.edit);
      if (overlay.classList.contains('visible') && modalTicker === ticker) {
        if (wantEdit) {
          setMode('edit');
          notesEl.focus();
          return;
        }
        close();
        return;
      }
      modalTicker = ticker;
      hadNote = !!hasExistingNote;
      savedNotes = currentNotes || '';
      window._notesOnDone = onDone || null;
      fillEditor(savedNotes);
      renderView(savedNotes);
      overlay.classList.add('visible');
      setMode(wantEdit ? 'edit' : 'view');
      if (sheet && sheet.classList.contains('editing')) notesEl.focus();
    };

    window._notesClose = close;

    async function persist(action) {
      if (!modalTicker) return;
      const notes = notesEl.value.trim();
      const t = modalTicker;
      try {
        if (action === 'delete') {
          await fetch('/api/frontend/abi-ticker-notes/' + t, { method: 'DELETE' });
        } else {
          await fetch('/api/frontend/abi-ticker-notes/' + t, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ notes }),
          });
        }
      } catch (e) {
        console.error('Notes save error', e);
      }
      const cb = window._notesOnDone;
      if (action === 'delete' || !notes) {
        close();
        if (cb) cb('removed', t, '');
        return;
      }
      hadNote = true;
      savedNotes = notes;
      fillEditor(notes);
      renderView(notes);
      setMode('view');
      if (cb) cb('saved', t, notes);
    }

    saveBtn.addEventListener('click', () => persist('save'));
    removeBtn.addEventListener('click', () => persist('delete'));
    editBtn?.addEventListener('click', () => { setMode('edit'); notesEl.focus(); });
    document.getElementById('notesModalCancelBtn').addEventListener('click', () => {
      notesEl.value = savedNotes;
      renderView(savedNotes);
      setMode('view');
    });
    document.getElementById('notesModalCloseBtn')?.addEventListener('click', close);
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

    window._copyWhyPrompt = async function (ticker, company, btn) {
      const t = (ticker || '').trim().toUpperCase();
      if (!t) return;
      const text = 'What does ' + t + ' do? Why has it moved [up/down] recently? Search for current news.\n'
          + 'Format:\n'
          + '\n'
          + '1. Summary: 3-4 sentences max. What the company does (1 sentence) + why the stock is moving (2-3 sentences). Every claim must include a specific date and specific number (price, %, $ amount) — no vague phrases like "recently" or "a prominent strategist."\n'
          + '2. Drivers: 5-6 bullets max, one line each, ranked by importance. Each bullet MUST include a date and a named source (person, bank, or event) — no unnamed "analysts" or "strategists." Format: `[Date] — [specific event/number] → [stock impact if known]`\n'
          + '\n'
          + 'Below is a FORMAT example only — content is illustrative, not factual, do not apply it to ' + t + ':\n'
          + '\n'
          + '* Aug 21 — Fed cut rates 25bps (FOMC) → sector +3%\n'
          + '* Aug 18 — CEO [Name] announced [specific product] → stock +8% that day\n'
          + '\n'
          + 'Be ruthless about cutting: no hedging language, no "not everyone is convinced" filler, no repeating the same catalyst twice across summary and bullets.';
      const original = btn ? btn.textContent : '';
      const flash = (msg, ok) => {
        if (!btn) return;
        btn.classList.toggle('copied', !!ok);
        btn.textContent = msg;
        setTimeout(() => { btn.classList.remove('copied'); btn.textContent = original; }, 1500);
      };
      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(text);
        } else {
          const ta = document.createElement('textarea');
          ta.value = text;
          ta.style.position = 'fixed';
          ta.style.opacity = '0';
          document.body.appendChild(ta);
          ta.select();
          document.execCommand('copy');
          document.body.removeChild(ta);
        }
        flash('Copied', true);
      } catch (e) {
        console.error('Copy why-prompt failed', e);
        flash('Failed', false);
      }
    };

    const dlOverlay = document.getElementById('dlModalOverlay');
    if (dlOverlay) {
      const dlTickerEl = document.getElementById('dlModalTicker');
      const dlNotesEl = document.getElementById('dlModalNotes');
      const dlTitleEl = document.getElementById('dlModalTitleEl');
      const dlRemoveBtn = document.getElementById('dlModalRemoveBtn');
      const dlSaveBtn = document.getElementById('dlModalSaveBtn');
      let dlTicker = null;
      let dlIsDisliked = false;
      let dlOnDone = null;

      function selectedKind() {
        const el = dlOverlay.querySelector('input[name="dlKind"]:checked');
        return (el && el.value) || 'temporary';
      }
      function setKind(kind) {
        const want = kind === 'permanent' ? 'permanent' : 'temporary';
        dlOverlay.querySelectorAll('input[name="dlKind"]').forEach(r => {
          r.checked = r.value === want;
        });
      }

      window._dlOpen = function (ticker, currentNotes, isDisliked, onDone, currentKind) {
        dlTicker = ticker;
        dlIsDisliked = !!isDisliked;
        dlOnDone = onDone || null;
        dlTickerEl.textContent = ticker;
        dlNotesEl.value = currentNotes || '';
        setKind(currentKind || 'temporary');
        dlRemoveBtn.style.display = dlIsDisliked ? 'inline-block' : 'none';
        dlSaveBtn.textContent = dlIsDisliked ? 'Update' : 'Block';
        dlTitleEl.innerHTML = (dlIsDisliked ? 'Edit exclude for ' : 'Block ') +
          '<span class="ticker-hl">' + ticker + '</span>' + (dlIsDisliked ? '' : ' from screeners');
        dlOverlay.classList.add('visible');
        dlNotesEl.focus();
      };

      window._dlClose = function () {
        dlOverlay.classList.remove('visible');
        dlTicker = null;
      };

      window._dlSave = async function () {
        if (!dlTicker) return;
        const notes = dlNotesEl.value.trim();
        const kind = selectedKind();
        try {
          await fetch('/api/frontend/abi-dislikes', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ticker: dlTicker, kind }),
          });
          if (notes) {
            await fetch('/api/frontend/abi-ticker-notes/' + dlTicker, {
              method: 'PUT',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ notes }),
            });
          }
        } catch (e) { console.error('Dislike save error', e); }
        dlOverlay.classList.remove('visible');
        if (dlOnDone) dlOnDone('saved', dlTicker, notes, kind);
        dlTicker = null;
      };

      window._dlRemove = async function () {
        if (!dlTicker) return;
        try {
          await fetch('/api/frontend/abi-dislikes/' + dlTicker, { method: 'DELETE' });
        } catch (e) { console.error('Dislike remove error', e); }
        dlOverlay.classList.remove('visible');
        if (dlOnDone) dlOnDone('removed', dlTicker);
        dlTicker = null;
      };

      window._dlOpenForTicker = async function (ticker, onDone) {
        if (!ticker) return;
        let isDisliked = false;
        let notes = '';
        let kind = 'temporary';
        try {
          const resp = await fetch('/api/frontend/abi-dislikes/batch-check', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tickers: [ticker] }),
          });
          if (resp.ok) {
            const data = await resp.json();
            const entry = data[ticker.toUpperCase()];
            if (entry) {
              isDisliked = true;
              notes = entry.notes || '';
              kind = entry.kind || 'permanent';
            }
          }
        } catch (e) { console.error('Dislike lookup failed', e); }
        window._dlOpen(ticker, notes, isDisliked, function (action, t, newNotes, newKind) {
          const isNowDisliked = action === 'saved';
          const btn = document.getElementById('dlBtn');
          if (btn) {
            btn.classList.toggle('is-disliked', isNowDisliked);
            btn.textContent = isNowDisliked ? 'Excluded' : 'Exclude';
          }
          window.dispatchEvent(new CustomEvent('abi-exclude-changed', {
            detail: { action, ticker: t || ticker, kind: newKind },
          }));
          if (onDone) onDone(action, t, newNotes, newKind);
        }, kind);
      };

      dlSaveBtn.addEventListener('click', () => window._dlSave());
      dlRemoveBtn.addEventListener('click', () => window._dlRemove());
      document.getElementById('dlModalCancelBtn').addEventListener('click', () => window._dlClose());
      dlOverlay.addEventListener('click', e => { if (e.target === dlOverlay) window._dlClose(); });
    }
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
