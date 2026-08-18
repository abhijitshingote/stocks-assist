(function () {
  'use strict';

  const U = window.MobileUtil;
  const ticker = String(window.__STOCK_TICKER__ || '').toUpperCase();

  let watchlistStatus = {};
  let abiTickerNotesStatus = {};
  let stockChart = null;
  let newsPanel = null;
  let currentTimeframeDays =
    typeof getStoredTimeframe === 'function' ? getStoredTimeframe() : 365;

  function showLoading(on) {
    const el = document.getElementById('loadingOverlay');
    if (el) el.classList.toggle('hidden', !on);
  }

  function showError(message) {
    showLoading(false);
    const zone = document.getElementById('analyzeZone');
    const err = document.getElementById('stockError');
    if (zone) zone.style.display = 'none';
    if (err) {
      if (message) {
        const msg = err.querySelector('p');
        if (msg) msg.innerHTML = message;
      }
      err.hidden = false;
    }
  }

  function parseNotesMarkdown(text) {
    if (typeof marked !== 'undefined' && marked.parse) return marked.parse(text);
    return text;
  }

  function updateAbiNotes() {
    const body = document.getElementById('abiNotesBody');
    if (!body) return;
    const entry = abiTickerNotesStatus[ticker];
    if (entry && entry.notes) {
      body.innerHTML = parseNotesMarkdown(entry.notes);
      body.classList.remove('empty');
    } else {
      body.textContent = 'No Abi ticker notes yet.';
      body.classList.add('empty');
    }
  }

  function updateWatchlistBtn() {
    const btn = document.getElementById('wlBtn');
    if (!btn) return;
    const inWl = !!watchlistStatus[ticker];
    btn.classList.toggle('on', inWl);
    btn.textContent = 'Watch';
  }

  function updateNotesBtn() {
    const btn = document.getElementById('notesBtn');
    if (!btn) return;
    const cmt = abiTickerNotesStatus[ticker];
    btn.classList.toggle('on', !!(cmt && cmt.notes));
  }

  function updateDlBtn(isDisliked) {
    const btn = document.getElementById('dlBtn');
    if (!btn) return;
    btn.classList.toggle('is-disliked', !!isDisliked);
    btn.textContent = isDisliked ? 'Excluded' : 'Exclude';
  }

  function renderDetail(stock) {
    const setText = (id, text) => {
      const el = document.getElementById(id);
      if (el) el.textContent = text;
    };

    setText('detailTk', stock.ticker || ticker);
    setText('detailCo', stock.company_name || '');
    setText(
      'detailPx',
      stock.current_price != null ? '$' + stock.current_price.toFixed(2) : '—'
    );

    const chg = document.getElementById('detailChg');
    if (chg) {
      chg.textContent = U.fmtRet(stock.dr_1);
      chg.className = 'chg ' + U.retCls(stock.dr_1);
    }

    setText('detailMcap', U.fmtMktCap(stock.market_cap));

    const sectorParts = [stock.sector, stock.industry].filter(Boolean);
    setText(
      'detailSectorIndustry',
      sectorParts.length ? sectorParts.join(' · ') : ''
    );
    const coLine = document.getElementById('detailCoLine');
    if (coLine) {
      coLine.style.display =
        stock.company_name || sectorParts.length ? '' : 'none';
    }

    const ti65El = document.getElementById('detailTi65');
    if (ti65El) {
      if (stock.ti65) {
        ti65El.textContent = 'TI65 ' + stock.ti65.toFixed(2);
        ti65El.classList.toggle('hot', stock.ti65 > 1.1);
        ti65El.style.display = '';
      } else {
        ti65El.style.display = 'none';
      }
    }

    const lfEl = document.getElementById('detailLowFloat');
    if (lfEl) lfEl.style.display = U.isLowFloat(stock.float_shares) ? '' : 'none';

    const detailLink = document.getElementById('detailLink');
    if (detailLink) detailLink.href = '/stock/' + (stock.ticker || ticker);

    const searchInput = document.getElementById('tickerSearchInput');
    if (searchInput) searchInput.value = stock.ticker || ticker;

    U.renderTagsStrip(document.getElementById('tagsStrip'), stock);
    U.renderMetrics(document.getElementById('metricsContent'), stock);
    document.title = (stock.ticker || ticker) + ' — Mobile';
  }

  async function loadWatchlistAndNotes() {
    try {
      const [wlResp, cmtResp, dlResp] = await Promise.all([
        fetch('/api/frontend/abi-watchlist/batch-check', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ tickers: [ticker] }),
        }),
        fetch('/api/frontend/abi-ticker-notes/batch-check', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ tickers: [ticker] }),
        }),
        fetch('/api/frontend/abi-dislikes/batch-check', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ tickers: [ticker] }),
        }),
      ]);
      if (wlResp.ok) watchlistStatus = await wlResp.json();
      if (cmtResp.ok) abiTickerNotesStatus = await cmtResp.json();
      if (dlResp.ok) {
        const dl = await dlResp.json();
        updateDlBtn(!!dl[ticker]);
      }
    } catch (e) {
      console.error('Watchlist/notes load failed', e);
    }
    updateWatchlistBtn();
    updateNotesBtn();
    try {
      updateAbiNotes();
    } catch (e) {
      console.error('Notes render failed', e);
    }
  }

  function getStockChartHeight() {
    const wrap = document.getElementById('stockChartWrap');
    const charts = document.querySelector('#mainPanelChart .charts');
    return Math.max(
      wrap ? wrap.clientHeight : 0,
      charts ? charts.clientHeight : 0,
      140
    );
  }

  function resizeStockChart() {
    if (!stockChart || !stockChart.chart) return;
    const wrap = document.getElementById('stockChartWrap');
    const container = document.getElementById('stockChartContainer');
    const h = getStockChartHeight();
    if (!wrap || h < 1) return;
    if (container) {
      container.style.height = h + 'px';
      container.style.maxHeight = h + 'px';
    }
    stockChart.resizeToHeight(h, wrap.clientWidth);
  }

  function scheduleStockChartResize() {
    requestAnimationFrame(resizeStockChart);
    setTimeout(() => requestAnimationFrame(resizeStockChart), 50);
    setTimeout(() => requestAnimationFrame(resizeStockChart), 300);
  }

  async function loadChart() {
    const container = document.getElementById('stockChartContainer');
    if (!container || typeof StockChart === 'undefined') return;

    try {
      if (stockChart) {
        stockChart.destroy();
        stockChart = null;
      }
      container.innerHTML = '';

      const wrap = document.getElementById('stockChartWrap');
      stockChart = new StockChart('stockChartContainer', {
        height: Math.max(wrap ? wrap.clientHeight : 0, 140),
        showRSI: false,
        showVolspikeMarkers: false,
        compact: true,
      });
      await stockChart.load(ticker);
      stockChart.setTimeframe(currentTimeframeDays);
      scheduleStockChartResize();
    } catch (e) {
      console.error('Chart load error for ' + ticker, e);
      container.innerHTML = '<div class="chart-error">Chart unavailable</div>';
    }
  }

  function setMainPanel(panel) {
    document.querySelectorAll('.main-seg button').forEach(x => {
      x.classList.toggle('active', x.dataset.panel === panel);
    });
    const panels = {
      metrics: 'mainPanelMetrics',
      news: 'mainPanelNews',
      chart: 'mainPanelChart',
    };
    Object.keys(panels).forEach(key => {
      const el = document.getElementById(panels[key]);
      if (el) el.classList.toggle('active', panel === key);
    });
    if (panel === 'chart') scheduleStockChartResize();
  }

  function setupNews() {
    if (!window.StockNewsShared) return;
    newsPanel = window.StockNewsShared.createNewsPanel({
      contentId: 'newsContent',
      loadBtnId: 'loadNewsBtn',
      benzingaBtnId: 'benzingaNewsBtn',
      filterBarId: 'newsFilters',
      showSnippet: false,
      buttonLabels: {
        load: 'Load',
        loadRefresh: 'Reload',
        benzinga: 'Benzinga',
        benzingaRefresh: 'Refresh',
      },
      getTicker: () => ticker,
    });
    newsPanel.setup();
  }

  async function loadStock() {
    if (!ticker) {
      showError('No ticker specified.');
      return;
    }
    if (!U) {
      showError('Page scripts failed to load. Refresh and try again.');
      return;
    }

    showLoading(true);
    try {
      const response = await fetch(
        '/api/frontend/stock/' + encodeURIComponent(ticker)
      );
      let data;
      try {
        data = await response.json();
      } catch (e) {
        console.error('Invalid stock API response', e);
        showError('Invalid response for <b>' + ticker + '</b>.');
        return;
      }

      if (!response.ok || data.error) {
        const reason = data.error || 'HTTP ' + response.status;
        showError(
          'Could not load <b>' + ticker + '</b>: ' + reason + '.'
        );
        return;
      }

      renderDetail(data);
      showLoading(false);

      loadWatchlistAndNotes();
      loadChart();
    } catch (e) {
      console.error('Stock load failed', e);
      showError('Could not load <b>' + ticker + '</b>.');
    }
  }

  document.querySelectorAll('#tfBar button').forEach(b => {
    b.classList.toggle(
      'active',
      parseInt(b.dataset.days, 10) === currentTimeframeDays
    );
    b.addEventListener('click', () => {
      document
        .querySelectorAll('#tfBar button')
        .forEach(x => x.classList.remove('active'));
      b.classList.add('active');
      currentTimeframeDays = parseInt(b.dataset.days, 10);
      if (stockChart) stockChart.setTimeframe(currentTimeframeDays);
    });
  });

  document.querySelectorAll('.main-seg button').forEach(b => {
    b.addEventListener('click', () => setMainPanel(b.dataset.panel));
  });

  const wlBtn = document.getElementById('wlBtn');
  if (wlBtn) {
    wlBtn.addEventListener('click', () => {
      const inWl = !!watchlistStatus[ticker];
      window._wlToggle(ticker, inWl, (nowIn, t) => {
        if (nowIn) watchlistStatus[t] = watchlistStatus[t] || { stars: 0 };
        else delete watchlistStatus[t];
        updateWatchlistBtn();
      });
    });
  }

  const notesBtn = document.getElementById('notesBtn');
  if (notesBtn) {
    notesBtn.addEventListener('click', () => {
      const cmt = abiTickerNotesStatus[ticker];
      const currentNotes = (cmt && cmt.notes) || '';
      window._notesOpen(
        ticker,
        currentNotes,
        !!currentNotes,
        (action, t, newNotes) => {
          if (action === 'saved')
            abiTickerNotesStatus[t] = { notes: newNotes || '' };
          else delete abiTickerNotesStatus[t];
          updateAbiNotes();
          updateNotesBtn();
        }
      );
    });
  }

  document.getElementById('dlBtn')?.addEventListener('click', () => {
    if (window._dlOpenForTicker) window._dlOpenForTicker(ticker);
  });

  document.getElementById('whyBtn')?.addEventListener('click', function () {
    if (!window._copyWhyPrompt) return;
    const co = document.getElementById('detailCo');
    window._copyWhyPrompt(ticker, co ? co.textContent : '', this);
  });

  const panel = document.getElementById('mainPanelChart');
  if (panel && typeof ResizeObserver !== 'undefined') {
    new ResizeObserver(() => scheduleStockChartResize()).observe(panel);
  }
  window.addEventListener('resize', () => scheduleStockChartResize());

  if (U && U.setupNotesModal) U.setupNotesModal();
  setupNews();
  loadStock();
})();
