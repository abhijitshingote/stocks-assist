(function () {
  'use strict';

  const U = window.MobileUtil;

  const CAP_LABELS = {
    all: 'All', mega: 'Mega', large: 'Large', mid: 'Mid', small: 'Small', micro: 'Micro',
  };

  function init(userConfig) {
    const config = Object.assign({
      usesCapFilter: true,
      listValueField: 'dr_1',
      listValueLabel: 'Ret',
      showTi65: false,
      showRank: true,
      chartOptions: { compact: true, showVolspikeMarkers: false },
      newsIds: {
        content: 'newsContent',
        loadBtn: 'loadNewsBtn',
        benzingaBtn: 'benzingaNewsBtn',
        filterBar: 'newsFilters',
      },
    }, userConfig);

    if (!config.fetchStocks) {
      throw new Error('MobileScreener.init: fetchStocks is required');
    }

    const phone = document.getElementById('phone');
    const listZone = document.getElementById('listZone');
    const filterSummary = document.getElementById('filterSummary');
    const sectorSheet = document.getElementById('sectorSheet');

    let allStocks = [];
    let currentCap = 'all';
    let selectedTicker = null;
    let selectedSector = null;
    let selectedIndustry = null;
    let currentTimeframeDays = typeof getStoredTimeframe === 'function' ? getStoredTimeframe() : 365;
    let watchlistStatus = {};
    let abiTickerNotesStatus = {};
    let stockChart = null;
    let newsPanel = null;

    if (config.pageTitle) {
      document.title = config.pageTitle;
    }

    const loadingOverlay = document.getElementById('loadingOverlay');
    if (loadingOverlay && config.pageTitle) {
      const label = loadingOverlay.querySelector('span');
      if (label) label.textContent = 'Loading ' + config.pageTitle + ' data…';
    }

    const capStrip = document.getElementById('capStrip');
    if (capStrip && !config.usesCapFilter) {
      capStrip.style.display = 'none';
    }

    const extraFilters = document.getElementById('extraFilters');
    if (extraFilters && config.extraFilterHtml) {
      extraFilters.innerHTML = config.extraFilterHtml;
    }

    function getListValue(stock) {
      if (config.listValueFn) return config.listValueFn(stock);
      return U.fmtRet(stock[config.listValueField]);
    }

    function getListValueRaw(stock) {
      if (config.listValueFn) return null;
      return stock[config.listValueField];
    }

    function passesSectorIndustry(s) {
      if (selectedSector && (s.sector || 'Unknown') !== selectedSector) return false;
      if (selectedIndustry && (s.industry || 'Unknown') !== selectedIndustry) return false;
      return true;
    }

    function filteredStocks() {
      if (config.filterStocks) {
        return config.filterStocks(allStocks, app);
      }
      return allStocks;
    }

    function visibleStocks() {
      return filteredStocks().filter(passesSectorIndustry);
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
      const cap = config.usesCapFilter ? (CAP_LABELS[currentCap] || currentCap) : '';
      const sec = selectedSector ? U.abbrevSector(selectedSector) : 'All sectors';
      const n = visibleStocks().length;
      const capPart = cap ? '<b>' + cap + '</b> · ' : '';
      document.getElementById('filterChipText').innerHTML =
        capPart + U.escAttr(sec) + ' · <b>' + n + '</b> stocks';
    }

    function updateCounts() {
      const base = filteredStocks();
      const total = base.length;
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
      if (!config.usesCapFilter) return;
      document.querySelectorAll('#capStrip [data-cap]').forEach(p => {
        p.classList.toggle('active', p.dataset.cap === currentCap);
      });
    }

    function renderSectorTabs() {
      const bar = document.getElementById('sectorStrip');
      const stocks = filteredStocks();
      const counts = {};
      stocks.forEach(s => {
        const sec = s.sector || 'Unknown';
        counts[sec] = (counts[sec] || 0) + 1;
      });
      const sectors = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);
      const top = sectors.slice(0, 4);
      const rest = sectors.length - top.length;

      let html = '<span class="strip-label">Sec</span>';
      const allActive = selectedSector === null ? ' active' : '';
      html += '<button class="pill sector-pill' + allActive + '" type="button" data-sector="">All <span class="n">' + stocks.length + '</span></button>';
      top.forEach(sec => {
        const isActive = sec === selectedSector ? ' active' : '';
        html += '<button class="pill sector-pill' + isActive + '" type="button" data-sector="' + U.escAttr(sec) + '">' +
          U.escAttr(U.abbrevSector(sec)) + ' <span class="n">' + counts[sec] + '</span></button>';
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
      const stocks = filteredStocks();
      const counts = {};
      stocks.forEach(s => {
        const sec = s.sector || 'Unknown';
        counts[sec] = (counts[sec] || 0) + 1;
      });
      const sectors = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);

      let html = '<div class="sector-item' + (selectedSector === null ? ' active' : '') + '" data-sector=""><span>All</span><span class="cnt">' + stocks.length + '</span></div>';
      sectors.forEach(sec => {
        const active = sec === selectedSector ? ' active' : '';
        html += '<div class="sector-item' + active + '" data-sector="' + U.escAttr(sec) + '"><span>' + U.escAttr(sec) + '</span><span class="cnt">' + counts[sec] + '</span></div>';
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

      const stocks = filteredStocks();
      const counts = {};
      stocks.forEach(s => {
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
        html += '<button class="pill industry-pill' + isActive + '" type="button" data-industry="' + U.escAttr(ind) + '">' +
          U.escAttr(label) + ' <span class="n">' + counts[ind] + '</span></button>';
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

    function updateListHeader() {
      const thead = document.querySelector('.stock-table thead tr');
      if (!thead) return;
      const rankTh = thead.children[0];
      const valueTh = thead.children[3];
      if (rankTh) rankTh.style.display = config.showRank ? '' : 'none';
      if (valueTh) valueTh.textContent = config.listValueLabel;
    }

    function defaultRenderList() {
      const visible = visibleStocks();
      const chipStrip = document.getElementById('chipStrip');
      const tbody = document.getElementById('stockTableBody');

      chipStrip.innerHTML = visible.slice(0, 24).map(s => {
        const active = s.ticker === selectedTicker ? ' active' : '';
        const raw = getListValueRaw(s);
        const cls = raw != null ? U.retCls(raw) : '';
        return '<div class="tchip' + active + '" data-ticker="' + U.escAttr(s.ticker) + '"><span class="tk">' + U.escAttr(s.ticker) +
          '</span><span class="ret ' + cls + '">' + getListValue(s) + '</span></div>';
      }).join('');

      tbody.innerHTML = visible.map((s, i) => {
        const active = s.ticker === selectedTicker ? ' active' : '';
        const raw = getListValueRaw(s);
        const cls = raw != null ? U.retCls(raw) : '';
        const rankTd = config.showRank ? '<td>' + (i + 1) + '</td>' : '';
        return '<tr class="' + active.trim() + '" data-ticker="' + U.escAttr(s.ticker) + '">' + rankTd +
          '<td>' + U.escAttr(s.ticker) + '</td><td>' + U.fmtMktCap(s.market_cap) +
          '</td><td class="' + cls + '">' + getListValue(s) + '</td><td class="stars">' + starsCell(s.ticker) + '</td></tr>';
      }).join('');

      chipStrip.querySelectorAll('.tchip').forEach(c => {
        c.addEventListener('click', () => selectStock(c.dataset.ticker));
      });
      tbody.querySelectorAll('tr').forEach(r => {
        r.addEventListener('click', () => selectStock(r.dataset.ticker));
      });

      updateCounts();
    }

    function renderList() {
      if (config.renderList) {
        config.renderList(visibleStocks(), app);
        updateCounts();
        return;
      }
      defaultRenderList();
    }

    function updateAbiNotes(ticker) {
      const body = document.getElementById('abiNotesBody');
      if (config.notesFromStock) {
        const stock = allStocks.find(s => s.ticker === ticker);
        const notes = stock && stock.watchlist_notes;
        if (notes) {
          body.innerHTML = marked.parse(notes);
          body.classList.remove('empty');
        } else {
          body.textContent = 'No Abi ticker notes yet.';
          body.classList.add('empty');
        }
        return;
      }
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
      chg.textContent = U.fmtRet(stock.dr_1);
      chg.className = 'chg ' + U.retCls(stock.dr_1);
      document.getElementById('detailMcap').textContent = U.fmtMktCap(stock.market_cap);
      const sectorParts = [stock.sector, stock.industry].filter(Boolean);
      const siEl = document.getElementById('detailSectorIndustry');
      siEl.textContent = sectorParts.length ? sectorParts.join(' · ') : '';
      document.getElementById('detailCoLine').style.display =
        (stock.company_name || sectorParts.length) ? '' : 'none';

      const ti65El = document.getElementById('detailTi65');
      if (config.showTi65 && stock.ti65) {
        ti65El.textContent = 'TI65 ' + stock.ti65.toFixed(2);
        ti65El.style.display = '';
      } else {
        ti65El.style.display = 'none';
      }

      const lfEl = document.getElementById('detailLowFloat');
      lfEl.style.display = U.isLowFloat(stock.float_shares) ? '' : 'none';

      document.getElementById('detailLink').href = '/stock/' + ticker;

      updateAbiNotes(ticker);
      updateWatchlistBtn(ticker);
      U.renderTagsStrip(document.getElementById('tagsStrip'), stock);
      U.renderMetrics(document.getElementById('metricsContent'), stock);
      renderList();

      if (newsPanel) {
        newsPanel.reset();
        newsPanel.onTickerChange();
      }

      await loadCharts(ticker);
    }

    function buildWatchlistFromStocks() {
      watchlistStatus = {};
      allStocks.forEach(s => {
        if (s.watchlist_stars != null) {
          watchlistStatus[s.ticker] = { stars: s.watchlist_stars };
        } else if (s.in_watchlist) {
          watchlistStatus[s.ticker] = { stars: 0 };
        }
      });
    }

    async function loadWatchlistForStocks(tickers) {
      if (config.watchlistFromStock) {
        buildWatchlistFromStocks();
      } else if (tickers.length) {
        try {
          const wlResp = await fetch('/api/frontend/abi-watchlist/batch-check', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tickers }),
          });
          if (wlResp.ok) watchlistStatus = await wlResp.json();
        } catch (e) {
          console.error('Watchlist batch load failed', e);
        }
      }

      if (!config.notesFromStock && tickers.length) {
        try {
          const cmtResp = await fetch('/api/frontend/abi-ticker-notes/batch-check', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tickers }),
          });
          if (cmtResp.ok) abiTickerNotesStatus = await cmtResp.json();
        } catch (e) {
          console.error('Notes batch load failed', e);
        }
      }

      renderList();
      if (selectedTicker) {
        updateWatchlistBtn(selectedTicker);
        updateAbiNotes(selectedTicker);
      }
    }

    function showLoading(on) {
      document.getElementById('loadingOverlay').classList.toggle('hidden', !on);
    }

    async function loadData(cap) {
      showLoading(true);
      if (config.usesCapFilter && cap != null) {
        currentCap = cap;
        renderCapPills();
      }

      try {
        const capArg = config.usesCapFilter ? currentCap : undefined;
        allStocks = await config.fetchStocks(capArg);
        if (!Array.isArray(allStocks)) allStocks = [];
      } catch (e) {
        console.error('Failed to load ' + (config.pageTitle || 'screener') + ' data', e);
        allStocks = [];
      }

      if (config.sortStocks) {
        allStocks = config.sortStocks(allStocks);
      }

      const base = filteredStocks();
      if (selectedSector && !base.some(s => (s.sector || 'Unknown') === selectedSector)) {
        selectedSector = null;
        selectedIndustry = null;
      } else if (selectedIndustry && !base.some(s =>
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

      if (config.onDataLoaded) config.onDataLoaded(app);
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
        const chartOpts = Object.assign({
          height: Math.max(wrap.clientHeight, 140),
          showRSI: false,
          showVolspikeMarkers: false,
          compact: true,
        }, config.chartOptions || {});
        stockChart = new StockChart('stockChartContainer', chartOpts);
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

    function setupNews() {
      const ids = config.newsIds;
      newsPanel = window.StockNewsShared.createNewsPanel({
        contentId: ids.content,
        loadBtnId: ids.loadBtn,
        benzingaBtnId: ids.benzingaBtn,
        filterBarId: ids.filterBar,
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

    const app = {
      get allStocks() { return allStocks; },
      get selectedTicker() { return selectedTicker; },
      get selectedSector() { return selectedSector; },
      get selectedIndustry() { return selectedIndustry; },
      get currentCap() { return currentCap; },
      visibleStocks,
      renderList,
      selectStock,
      loadData,
      updateCounts,
    };

    // ── Shell wiring ──
    filterSummary.addEventListener('click', () => {
      const open = phone.classList.toggle('filters-open');
      filterSummary.setAttribute('aria-expanded', open);
    });

    if (config.usesCapFilter) {
      document.querySelectorAll('#capStrip [data-cap]').forEach(p => {
        p.addEventListener('click', () => {
          selectedTicker = null;
          loadData(p.dataset.cap);
        });
      });
    }

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
      let currentNotes = '';
      if (config.notesFromStock) {
        const stock = allStocks.find(s => s.ticker === selectedTicker);
        currentNotes = (stock && stock.watchlist_notes) || '';
      } else {
        const cmt = abiTickerNotesStatus[selectedTicker];
        currentNotes = (cmt && cmt.notes) || '';
      }
      window._notesOpen(selectedTicker, currentNotes, !!currentNotes, (action, ticker, newNotes) => {
        if (config.notesFromStock) {
          const stock = allStocks.find(s => s.ticker === ticker);
          if (stock) {
            if (action === 'saved') stock.watchlist_notes = newNotes || '';
            else delete stock.watchlist_notes;
          }
        } else {
          if (action === 'saved') abiTickerNotesStatus[ticker] = { notes: newNotes || '' };
          else delete abiTickerNotesStatus[ticker];
        }
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

    document.querySelectorAll('#tfBar button').forEach(b => {
      b.classList.toggle('active', parseInt(b.dataset.days, 10) === currentTimeframeDays);
    });

    updateListHeader();
    U.setupNotesModal();
    setupNews();
    loadHeaderIndicators();

    if (config.onSetup) config.onSetup(app);

    loadData(config.usesCapFilter ? 'all' : undefined);

    return app;
  }

  window.MobileScreener = { init };
})();
