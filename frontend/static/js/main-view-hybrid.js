(function () {
  'use strict';

  const phone = document.getElementById('phone');
  const listZone = document.getElementById('listZone');
  const filterSummary = document.getElementById('filterSummary');
  const sectorSheet = document.getElementById('sectorSheet');

  let allStocks = [];
  let currentCap = 'all';
  let selectedTicker = null;
  let selectedSector = null;
  let selectedIndustry = null;
  let currentTimeframeDays = getStoredTimeframe();
  let watchlistStatus = {};
  let abiTickerNotesStatus = {};
  let stockChart = null;
  let newsPanel = null;

  const CAP_LABELS = {
    all: 'All', mega: 'Mega', large: 'Large', mid: 'Mid', small: 'Small', micro: 'Micro',
  };

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

  function sortByReturn(data) {
    return [...data].sort((a, b) => {
      const aVal = a.ti65;
      const bVal = b.ti65;
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      return bVal - aVal;
    });
  }

  function passesSectorIndustry(s) {
    if (selectedSector && (s.sector || 'Unknown') !== selectedSector) return false;
    if (selectedIndustry && (s.industry || 'Unknown') !== selectedIndustry) return false;
    return true;
  }

  function visibleStocks() {
    return allStocks.filter(passesSectorIndustry);
  }

  function getStars(ticker) {
    const wl = watchlistStatus[ticker];
    return wl && Number.isFinite(wl.stars) ? wl.stars : 0;
  }

  function starsCell(ticker) {
    const stars = getStars(ticker);
    if (!watchlistStatus[ticker] || stars <= 0) return '';
    return '★'.repeat(stars);
  }

  function updateFilterChip() {
    const cap = CAP_LABELS[currentCap] || currentCap;
    const sec = selectedSector ? abbrevSector(selectedSector) : 'All sectors';
    const n = visibleStocks().length;
    document.getElementById('filterChipText').innerHTML =
      '<b>' + cap + '</b> · ' + escAttr(sec) + ' · <b>' + n + '</b> stocks';
  }

  function updateCounts() {
    const total = allStocks.length;
    const showing = visibleStocks().length;
    ['totalStocks', 'totalStocks2'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = total;
    });
    ['showingCount', 'showingCount2', 'listCount'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = showing;
    });
    updateFilterChip();
  }

  function renderCapPills() {
    document.querySelectorAll('#capStrip [data-cap]').forEach(p => {
      p.classList.toggle('active', p.dataset.cap === currentCap);
    });
  }

  function renderSectorTabs() {
    const bar = document.getElementById('sectorStrip');
    const counts = {};
    allStocks.forEach(s => {
      const sec = s.sector || 'Unknown';
      counts[sec] = (counts[sec] || 0) + 1;
    });
    const sectors = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);
    const top = sectors.slice(0, 4);
    const rest = sectors.length - top.length;

    let html = '<span class="strip-label">Sec</span>';
    const allActive = selectedSector === null ? ' active' : '';
    html += '<button class="pill sector-pill' + allActive + '" type="button" data-sector="">All <span class="n">' + allStocks.length + '</span></button>';
    top.forEach(sec => {
      const isActive = sec === selectedSector ? ' active' : '';
      html += '<button class="pill sector-pill' + isActive + '" type="button" data-sector="' + escAttr(sec) + '">' +
        escAttr(abbrevSector(sec)) + ' <span class="n">' + counts[sec] + '</span></button>';
    });
    if (rest > 0) {
      html += '<button class="pill more" type="button" id="moreSectors">+' + rest + '</button>';
    }
    bar.innerHTML = html;

    bar.querySelectorAll('.sector-pill').forEach(btn => {
      btn.addEventListener('click', () => {
        const sec = btn.dataset.sector || null;
        if (sec === selectedSector) {
          selectedSector = null;
          selectedIndustry = null;
        } else {
          selectedSector = sec;
          selectedIndustry = null;
        }
        renderSectorTabs();
        renderIndustryTabs();
        renderSectorSheet();
        renderList();
        updateCounts();
      });
    });

    const moreBtn = document.getElementById('moreSectors');
    if (moreBtn) {
      moreBtn.addEventListener('click', () => sectorSheet.classList.add('open'));
    }
  }

  function renderSectorSheet() {
    const list = document.getElementById('sectorList');
    const counts = {};
    allStocks.forEach(s => {
      const sec = s.sector || 'Unknown';
      counts[sec] = (counts[sec] || 0) + 1;
    });
    const sectors = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);

    let html = '<div class="sector-item' + (selectedSector === null ? ' active' : '') + '" data-sector=""><span>All</span><span class="cnt">' + allStocks.length + '</span></div>';
    sectors.forEach(sec => {
      const active = sec === selectedSector ? ' active' : '';
      html += '<div class="sector-item' + active + '" data-sector="' + escAttr(sec) + '"><span>' + escAttr(sec) + '</span><span class="cnt">' + counts[sec] + '</span></div>';
    });
    list.innerHTML = html;

    list.querySelectorAll('.sector-item').forEach(item => {
      item.addEventListener('click', () => {
        selectedSector = item.dataset.sector || null;
        selectedIndustry = null;
        sectorSheet.classList.remove('open');
        renderSectorTabs();
        renderIndustryTabs();
        renderSectorSheet();
        renderList();
        updateCounts();
      });
    });
  }

  function renderIndustryTabs() {
    const bar = document.getElementById('industryStrip');
    if (!selectedSector) {
      bar.classList.add('hidden');
      bar.innerHTML = '';
      return;
    }

    const counts = {};
    allStocks.forEach(s => {
      if ((s.sector || 'Unknown') !== selectedSector) return;
      const ind = s.industry || 'Unknown';
      counts[ind] = (counts[ind] || 0) + 1;
    });
    const industries = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);
    const totalInSector = industries.reduce((acc, k) => acc + counts[k], 0);

    let html = '<span class="strip-label">Ind</span>';
    const allActive = selectedIndustry === null ? ' active' : '';
    html += '<button class="pill industry-pill' + allActive + '" type="button" data-industry="">All <span class="n">' + totalInSector + '</span></button>';
    industries.forEach(ind => {
      const isActive = ind === selectedIndustry ? ' active' : '';
      const label = ind.length > 16 ? ind.slice(0, 14) + '…' : ind;
      html += '<button class="pill industry-pill' + isActive + '" type="button" data-industry="' + escAttr(ind) + '">' +
        escAttr(label) + ' <span class="n">' + counts[ind] + '</span></button>';
    });
    bar.innerHTML = html;
    bar.classList.remove('hidden');

    bar.querySelectorAll('.industry-pill').forEach(btn => {
      btn.addEventListener('click', () => {
        const ind = btn.dataset.industry || null;
        selectedIndustry = ind === selectedIndustry ? null : ind;
        renderIndustryTabs();
        renderList();
        updateCounts();
      });
    });
  }

  function renderList() {
    const visible = visibleStocks();
    const chipStrip = document.getElementById('chipStrip');
    const tbody = document.getElementById('stockTableBody');

    chipStrip.innerHTML = visible.slice(0, 24).map(s => {
      const active = s.ticker === selectedTicker ? ' active' : '';
      const cls = retCls(s.dr_1);
      return '<div class="tchip' + active + '" data-ticker="' + escAttr(s.ticker) + '"><span class="tk">' + escAttr(s.ticker) +
        '</span><span class="ret ' + cls + '">' + fmtRet(s.dr_1) + '</span></div>';
    }).join('');

    tbody.innerHTML = visible.map((s, i) => {
      const active = s.ticker === selectedTicker ? ' active' : '';
      const cls = retCls(s.dr_1);
      return '<tr class="' + active.trim() + '" data-ticker="' + escAttr(s.ticker) + '"><td>' + (i + 1) +
        '</td><td>' + escAttr(s.ticker) + '</td><td>' + fmtMktCap(s.market_cap) +
        '</td><td class="' + cls + '">' + fmtRet(s.dr_1) + '</td><td class="stars">' + starsCell(s.ticker) + '</td></tr>';
    }).join('');

    chipStrip.querySelectorAll('.tchip').forEach(c => {
      c.addEventListener('click', () => selectStock(c.dataset.ticker));
    });
    tbody.querySelectorAll('tr').forEach(r => {
      r.addEventListener('click', () => selectStock(r.dataset.ticker));
    });

    updateCounts();
  }

  function updateMetrics(s) {
    const container = document.getElementById('metricsContent');
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

  function updateTagsStrip(stock) {
    const strip = document.getElementById('tagsStrip');
    if (!strip) return;
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
      strip.classList.remove('visible');
      strip.innerHTML = '';
      return;
    }
    strip.innerHTML = pills;
    strip.classList.add('visible');
  }

  function updateAbiNotes(ticker) {
    const body = document.getElementById('abiNotesBody');
    const entry = abiTickerNotesStatus[ticker];
    if (entry && entry.notes) {
      body.innerHTML = marked.parse(entry.notes);
      body.classList.remove('empty');
    } else {
      body.textContent = 'No Abi ticker notes yet.';
      body.classList.add('empty');
    }
  }

  function updateWatchlistBtn(ticker) {
    const btn = document.getElementById('wlBtn');
    const inWl = !!watchlistStatus[ticker];
    btn.classList.toggle('on', inWl);
    btn.textContent = inWl ? '★ WL' : '+ WL';
  }

  async function selectStock(ticker) {
    if (!ticker) return;
    selectedTicker = ticker;
    const stock = allStocks.find(s => s.ticker === ticker);
    if (!stock) return;

    document.getElementById('searchTicker').textContent = ticker;
    document.getElementById('detailTk').textContent = ticker;
    document.getElementById('detailCo').textContent = stock.company_name || '';
    document.getElementById('detailPx').textContent = stock.current_price ? '$' + stock.current_price.toFixed(2) : '—';
    const chg = document.getElementById('detailChg');
    chg.textContent = fmtRet(stock.dr_1);
    chg.className = 'chg ' + retCls(stock.dr_1);
    document.getElementById('detailMcap').textContent = fmtMktCap(stock.market_cap);
    const sectorParts = [stock.sector, stock.industry].filter(Boolean);
    const siEl = document.getElementById('detailSectorIndustry');
    siEl.textContent = sectorParts.length ? sectorParts.join(' · ') : '';
    document.getElementById('detailCoLine').style.display =
      (stock.company_name || sectorParts.length) ? '' : 'none';

    const ti65El = document.getElementById('detailTi65');
    if (stock.ti65) {
      ti65El.textContent = 'TI65 ' + stock.ti65.toFixed(2);
      ti65El.style.display = '';
    } else {
      ti65El.style.display = 'none';
    }

    const lfEl = document.getElementById('detailLowFloat');
    lfEl.style.display = isLowFloat(stock.float_shares) ? '' : 'none';

    document.getElementById('detailLink').href = '/stock/' + ticker;

    updateAbiNotes(ticker);
    updateWatchlistBtn(ticker);
    updateTagsStrip(stock);
    updateMetrics(stock);
    renderList();

    if (newsPanel) {
      newsPanel.reset();
      newsPanel.onTickerChange();
    }

    await loadCharts(ticker);
  }

  async function loadWatchlistForStocks(tickers) {
    if (!tickers.length) return;
    try {
      const [wlResp, cmtResp] = await Promise.all([
        fetch('/api/frontend/abi-watchlist/batch-check', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ tickers }),
        }),
        fetch('/api/frontend/abi-ticker-notes/batch-check', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ tickers }),
        }),
      ]);
      if (wlResp.ok) watchlistStatus = await wlResp.json();
      if (cmtResp.ok) abiTickerNotesStatus = await cmtResp.json();
      renderList();
      if (selectedTicker) {
        updateWatchlistBtn(selectedTicker);
        updateAbiNotes(selectedTicker);
      }
    } catch (e) {
      console.error('Watchlist/notes batch load failed', e);
    }
  }

  function showLoading(on) {
    document.getElementById('loadingOverlay').classList.toggle('hidden', !on);
  }

  async function loadData(cap) {
    showLoading(true);
    currentCap = cap;
    renderCapPills();

    try {
      const resp = await fetch('/api/frontend/main-view/' + cap);
      allStocks = await resp.json();
      if (allStocks.error) allStocks = [];
    } catch (e) {
      console.error('Failed to load Main View data', e);
      allStocks = [];
    }

    allStocks = sortByReturn(allStocks);

    if (selectedSector && !allStocks.some(s => (s.sector || 'Unknown') === selectedSector)) {
      selectedSector = null;
      selectedIndustry = null;
    } else if (selectedIndustry && !allStocks.some(s =>
      (s.sector || 'Unknown') === selectedSector &&
      (s.industry || 'Unknown') === selectedIndustry
    )) {
      selectedIndustry = null;
    }

    renderSectorTabs();
    renderIndustryTabs();
    renderSectorSheet();
    renderList();
    updateCounts();

    const tickers = allStocks.map(s => s.ticker);
    if (tickers.length) await loadWatchlistForStocks(tickers);

    if (allStocks.length && !selectedTicker) {
      await selectStock(allStocks[0].ticker);
    } else if (selectedTicker) {
      const still = allStocks.find(s => s.ticker === selectedTicker);
      if (still) await selectStock(selectedTicker);
      else if (allStocks.length) await selectStock(allStocks[0].ticker);
    }

    showLoading(false);
  }

  function setAllTimeframes(days) {
    currentTimeframeDays = days;
    if (stockChart) stockChart.setTimeframe(days);
  }

  let chartResizeTimers = [];

  function resizeStockChart() {
    if (!stockChart || !stockChart.chart) return;
    const wrap = document.getElementById('stockChartWrap');
    if (!wrap || wrap.clientHeight < 1) return;
    stockChart.resizeToHeight(wrap.clientHeight, wrap.clientWidth);
  }

  function scheduleStockChartResize() {
    chartResizeTimers.forEach(clearTimeout);
    chartResizeTimers = [];
    requestAnimationFrame(resizeStockChart);
    chartResizeTimers.push(setTimeout(() => requestAnimationFrame(resizeStockChart), 50));
    chartResizeTimers.push(setTimeout(() => requestAnimationFrame(resizeStockChart), 300));
  }

  async function loadCharts(ticker) {
    if (stockChart) { stockChart.destroy(); stockChart = null; }

    document.getElementById('stockChartContainer').innerHTML = '';

    try {
      const wrap = document.getElementById('stockChartWrap');
      stockChart = new StockChart('stockChartContainer', {
        height: Math.max(wrap.clientHeight, 140),
        showRSI: false,
        showVolspikeMarkers: false,
        compact: true,
      });
      await stockChart.load(ticker);

      setAllTimeframes(currentTimeframeDays);
      scheduleStockChartResize();
    } catch (e) {
      console.error('Chart load error for ' + ticker, e);
    }
  }

  async function loadHeaderIndicators() {
    try {
      const [vixResp, y10Resp] = await Promise.all([
        fetch('/api/frontend/vix-latest'),
        fetch('/api/frontend/treasury-10y'),
      ]);
      if (vixResp.ok) {
        const data = await vixResp.json();
        const el = document.getElementById('vixVal');
        if (data.close != null) {
          el.textContent = data.close.toFixed(2);
          el.className = data.change_pct != null && data.change_pct < 0 ? 'up' : data.change_pct > 0 ? 'down' : '';
        }
      }
      if (y10Resp.ok) {
        const data = await y10Resp.json();
        if (data.yield_pct != null) {
          document.getElementById('us10yVal').textContent = data.yield_pct.toFixed(2) + '%';
        }
      }
    } catch (e) {
      console.warn('Header indicators failed', e);
    }
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

  function setupNews() {
    newsPanel = window.StockNewsShared.createNewsPanel({
      contentId: 'hybridNewsContent',
      loadBtnId: 'hybridLoadNewsBtn',
      benzingaBtnId: 'hybridBenzingaNewsBtn',
      filterBarId: 'hybridNewsFilters',
      buttonLabels: {
        load: 'Load',
        loadRefresh: 'Reload',
        benzinga: 'Benzinga',
        benzingaRefresh: 'Refresh',
      },
      getTicker: () => selectedTicker,
    });
    newsPanel.setup();
  }

  function copyVisibleTickers() {
    const tickers = visibleStocks().map(s => s.ticker);
    const btn = document.getElementById('copyBtn');
    if (!tickers.length) {
      btn.textContent = 'Empty';
      setTimeout(() => { btn.textContent = 'Copy'; }, 1200);
      return;
    }
    const text = tickers.join(', ');
    (navigator.clipboard ? navigator.clipboard.writeText(text) : Promise.reject())
      .then(() => {
        btn.textContent = '✓';
        setTimeout(() => { btn.textContent = 'Copy'; }, 1200);
      })
      .catch(() => {
        btn.textContent = 'Fail';
        setTimeout(() => { btn.textContent = 'Copy'; }, 1200);
      });
  }

  // ── Preview UI wiring ──
  filterSummary.addEventListener('click', () => {
    const open = phone.classList.toggle('filters-open');
    filterSummary.setAttribute('aria-expanded', open);
  });

  document.querySelectorAll('#capStrip [data-cap]').forEach(p => {
    p.addEventListener('click', () => {
      selectedTicker = null;
      loadData(p.dataset.cap);
    });
  });

  document.getElementById('closeSector').addEventListener('click', () => sectorSheet.classList.remove('open'));

  document.getElementById('sectorSearch').addEventListener('input', e => {
    const q = e.target.value.toLowerCase();
    document.querySelectorAll('#sectorList .sector-item').forEach(item => {
      const name = item.querySelector('span').textContent.toLowerCase();
      item.style.display = name.includes(q) ? '' : 'none';
    });
  });

  document.querySelectorAll('[data-acc]').forEach(h => {
    h.addEventListener('click', () => {
      document.getElementById(h.dataset.acc).classList.toggle('collapsed');
      scheduleStockChartResize();
    });
  });

  document.querySelectorAll('#tfBar button').forEach(b => {
    b.addEventListener('click', () => {
      document.querySelectorAll('#tfBar button').forEach(x => x.classList.remove('active'));
      b.classList.add('active');
      setAllTimeframes(parseInt(b.dataset.days, 10));
    });
  });

  function setMainPanel(panel) {
    document.querySelectorAll('.main-seg button').forEach(x => {
      x.classList.toggle('active', x.dataset.panel === panel);
    });
    document.getElementById('mainPanelMetrics').classList.toggle('active', panel === 'metrics');
    document.getElementById('mainPanelNews').classList.toggle('active', panel === 'news');
    document.getElementById('mainPanelChart').classList.toggle('active', panel === 'chart');
    if (panel === 'chart') scheduleStockChartResize();
  }

  document.querySelectorAll('.main-seg button').forEach(b => {
    b.addEventListener('click', () => setMainPanel(b.dataset.panel));
  });

  document.getElementById('wlBtn').addEventListener('click', () => {
    if (!selectedTicker) return;
    const inWl = !!watchlistStatus[selectedTicker];
    window._wlToggle(selectedTicker, inWl, (nowIn, ticker) => {
      if (nowIn) watchlistStatus[ticker] = watchlistStatus[ticker] || { stars: 0 };
      else delete watchlistStatus[ticker];
      updateWatchlistBtn(ticker);
      renderList();
    });
  });

  document.getElementById('notesBtn').addEventListener('click', () => {
    if (!selectedTicker) return;
    const cmt = abiTickerNotesStatus[selectedTicker];
    const currentNotes = (cmt && cmt.notes) || '';
    window._notesOpen(selectedTicker, currentNotes, !!currentNotes, (action, ticker, newNotes) => {
      if (action === 'saved') abiTickerNotesStatus[ticker] = { notes: newNotes || '' };
      else delete abiTickerNotesStatus[ticker];
      updateAbiNotes(ticker);
    });
  });

  document.getElementById('copyBtn').addEventListener('click', copyVisibleTickers);

  const listModes = ['mode-collapsed', 'mode-half', 'mode-full'];
  let listModeIdx = 0;
  function setListMode(idx) {
    listModeIdx = Math.max(0, Math.min(2, idx));
    listZone.className = 'list-zone ' + listModes[listModeIdx];
    scheduleStockChartResize();
  }

  let listDragStartY = 0;
  let listDragStartIdx = 0;
  document.getElementById('listHandle').addEventListener('pointerdown', e => {
    listDragStartY = e.clientY;
    listDragStartIdx = listModeIdx;
    e.target.setPointerCapture(e.pointerId);
  });
  document.getElementById('listHandle').addEventListener('pointerup', e => {
    const dy = e.clientY - listDragStartY;
    if (dy < -30) setListMode(listDragStartIdx + 1);
    else if (dy > 30) setListMode(listDragStartIdx - 1);
    else setListMode(listDragStartIdx);
  });
  document.getElementById('listHandle').addEventListener('click', () => {
    setListMode((listModeIdx + 1) % 3);
  });

  function setAnalyzeMode() {
    phone.classList.remove('filters-open');
    filterSummary.setAttribute('aria-expanded', 'false');
    setListMode(0);
    setMainPanel('chart');
  }

  function setBrowseMode() {
    phone.classList.add('filters-open');
    filterSummary.setAttribute('aria-expanded', 'true');
    setListMode(1);
  }

  document.querySelectorAll('.mode-toggle button').forEach(b => {
    b.addEventListener('click', () => {
      document.querySelectorAll('.mode-toggle button').forEach(x => x.classList.remove('active'));
      b.classList.add('active');
      if (b.dataset.mode === 'browse') setBrowseMode();
      else setAnalyzeMode();
    });
  });

  listZone.addEventListener('transitionend', e => {
    if (e.propertyName === 'height') scheduleStockChartResize();
  });
  document.getElementById('filterDrawer').addEventListener('transitionend', e => {
    if (e.propertyName === 'max-height') scheduleStockChartResize();
  });

  window.addEventListener('resize', () => scheduleStockChartResize());

  // Sync tf bar with stored timeframe
  document.querySelectorAll('#tfBar button').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.days, 10) === currentTimeframeDays);
  });

  setupNotesModal();
  setupNews();
  loadHeaderIndicators();
  loadData('all');
})();
