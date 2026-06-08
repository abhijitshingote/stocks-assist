(function () {
  'use strict';

  const BREADTH_CHART_HEIGHT = 160;

  let homeData = { main_indices: [], commodities: [], risk_on_sectors: [], risk_off_sectors: [] };
  let breadthData = { history: [], latest: {} };
  let spyData = [];
  let chartInstances = {};
  let spyChart = null;
  let spySeries = {};
  let dmaChart = null;
  let moversChart = null;
  let dmaSeries = {};
  let moversSeries = {};
  let activeChart = null;
  let activeSection = null;
  let sectorTimeframe = 'dr_1';
  let isSyncing = false;

  document.addEventListener('DOMContentLoaded', () => {
    loadHomepageData();
    loadBreadthData();
    setupSortButtons();
    setupSectorsChartListeners();
  });

  function setupSectorsChartListeners() {
    const chartContainer = document.getElementById('sectorsChartContainer');
    setupTfBar(chartContainer);
  }

  function setupTfBar(chartContainer) {
    const tfBar = chartContainer.querySelector('.ctx-tf-bar');
    if (!tfBar) return;
    tfBar.querySelectorAll('button[data-days]').forEach(btn => {
      btn.addEventListener('click', e => {
        e.stopPropagation();
        const days = parseInt(btn.dataset.days, 10);
        tfBar.querySelectorAll('button').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        if (activeChart) updateChart(activeChart, days);
      });
    });
  }

  function setupSortButtons() {
    document.querySelectorAll('#sectorSort .home-sort-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const sortKey = btn.dataset.sort;
        document.querySelectorAll('#sectorSort .home-sort-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        sectorTimeframe = sortKey;
        renderSectorGrid('riskOnGrid', homeData.risk_on_sectors, sortKey);
        renderSectorGrid('riskOffGrid', homeData.risk_off_sectors, sortKey);
      });
    });
  }

  async function loadHomepageData() {
    try {
      const response = await fetch('/api/frontend/homepage');
      const data = await response.json();

      if (data.error) {
        showError();
        return;
      }

      homeData = data;
      renderMainIndices();
      renderCommodities();
      renderSectorGrid('riskOnGrid', data.risk_on_sectors, 'dr_1');
      renderSectorGrid('riskOffGrid', data.risk_off_sectors, 'dr_1');
    } catch (error) {
      console.error('Error:', error);
      showError();
    }
  }

  function showError() {
    ['mainIndicesGrid', 'commoditiesGrid', 'riskOnGrid', 'riskOffGrid'].forEach(id => {
      document.getElementById(id).innerHTML = '<div class="home-loading">Data unavailable</div>';
    });
  }

  function renderMainIndices() {
    const container = document.getElementById('mainIndicesGrid');
    container.innerHTML = homeData.main_indices.map(idx => renderIndexCard(idx)).join('');
    attachCardListeners(container, 'mainIndices');
  }

  function renderCommodities() {
    const container = document.getElementById('commoditiesGrid');
    container.innerHTML = homeData.commodities.map(idx => renderIndexCard(idx)).join('');
    attachCardListeners(container, 'commodities');
  }

  function renderIndexCard(idx) {
    const isVix = idx.symbol === '^VIX';
    return `
      <div class="home-index-card" data-symbol="${idx.symbol}" data-name="${idx.name}">
        <div class="card-head">
          <div>
            <div class="sym">${idx.symbol.replace('^', '')}</div>
            <div class="name">${idx.name}</div>
          </div>
          <div class="price">$${idx.current_price?.toFixed(2) || '—'}</div>
        </div>
        <div class="home-returns">
          <div class="home-ret-cell">
            <div class="lbl">1D</div>
            <div class="val ${retClass(idx.dr_1, isVix)}">${fmtRet(idx.dr_1)}</div>
          </div>
          <div class="home-ret-cell">
            <div class="lbl">5D</div>
            <div class="val ${retClass(idx.dr_5, isVix)}">${fmtRet(idx.dr_5)}</div>
          </div>
          <div class="home-ret-cell">
            <div class="lbl">20D</div>
            <div class="val ${retClass(idx.dr_20, isVix)}">${fmtRet(idx.dr_20)}</div>
          </div>
          <div class="home-ret-cell">
            <div class="lbl">60D</div>
            <div class="val ${retClass(idx.dr_60, isVix)}">${fmtRet(idx.dr_60)}</div>
          </div>
        </div>
        ${!isVix ? `
        <div class="home-dma-row">
          <span class="home-dma-badge ${idx.pct_from_50dma >= 0 ? 'dma-above' : 'dma-below'}">
            50D: ${fmtDma(idx.pct_from_50dma)}
          </span>
          <span class="home-dma-badge ${idx.pct_from_200dma >= 0 ? 'dma-above' : 'dma-below'}">
            200D: ${fmtDma(idx.pct_from_200dma)}
          </span>
        </div>
        ` : ''}
      </div>
    `;
  }

  function renderSectorGrid(containerId, sectors, sortKey) {
    const container = document.getElementById(containerId);

    const sorted = [...sectors].sort((a, b) => {
      const aVal = a[sortKey] ?? -999;
      const bVal = b[sortKey] ?? -999;
      return bVal - aVal;
    });

    container.innerHTML = sorted.map(s => {
      const returnVal = s[sortKey];
      const hotClass = getHotClass(returnVal);
      return `
        <div class="home-sector-tile ${hotClass}" data-symbol="${s.symbol}" data-name="${s.name}">
          <div class="tile-head">
            <span class="sym">${s.symbol}</span>
            <span class="ret ${retClass(returnVal)}">${fmtRet(returnVal)}</span>
          </div>
          <div class="name">${s.name}</div>
          <div class="home-dma-row">
            <span class="home-dma-badge ${s.pct_from_50dma >= 0 ? 'dma-above' : 'dma-below'}">
              50: ${fmtDma(s.pct_from_50dma)}
            </span>
            <span class="home-dma-badge ${s.pct_from_200dma >= 0 ? 'dma-above' : 'dma-below'}">
              200: ${fmtDma(s.pct_from_200dma)}
            </span>
          </div>
        </div>
      `;
    }).join('');

    container.querySelectorAll('.home-sector-tile').forEach(tile => {
      tile.addEventListener('click', () => {
        toggleExpandedChart(tile.dataset.symbol, tile.dataset.name, 'sectors', tile);
      });
    });
  }

  function attachCardListeners(container, section) {
    container.querySelectorAll('.home-index-card').forEach(card => {
      card.addEventListener('click', () => {
        toggleExpandedChart(card.dataset.symbol, card.dataset.name, section, card);
      });
    });

    const chartContainer = document.getElementById(`${section}ChartContainer`);
    setupTfBar(chartContainer);
  }

  function toggleExpandedChart(symbol, name, section, cardElement) {
    const chartContainer = document.getElementById(`${section}ChartContainer`);
    const titleEl = document.getElementById(`${section}ChartTitle`);

    if (activeChart === symbol && activeSection === section) {
      chartContainer.classList.remove('visible');
      cardElement.classList.remove('chart-open');
      activeChart = null;
      activeSection = null;
      return;
    }

    document.querySelectorAll('.home-chart-panel.visible').forEach(c => c.classList.remove('visible'));
    document.querySelectorAll('.home-index-card.chart-open, .home-sector-tile.chart-open').forEach(c => {
      c.classList.remove('chart-open');
    });

    activeChart = symbol;
    activeSection = section;
    chartContainer.classList.add('visible');
    cardElement.classList.add('chart-open');
    titleEl.textContent = `${symbol.replace('^', '')} - ${name}`;

    const tfBar = chartContainer.querySelector('.ctx-tf-bar');
    if (tfBar) {
      tfBar.querySelectorAll('button').forEach(b => b.classList.remove('active'));
      const defaultBtn = tfBar.querySelector('button[data-days="365"]');
      if (defaultBtn) defaultBtn.classList.add('active');
    }

    loadExpandedChart(symbol, section);
  }

  async function loadExpandedChart(symbol, section) {
    const wrapper = document.getElementById(`${section}ChartWrapper`);
    wrapper.innerHTML = '<div class="home-loading"><div class="spinner"></div>Loading chart…</div>';

    try {
      const response = await fetch(`/api/frontend/index-ohlc/${symbol}`);
      const data = await response.json();

      if (data.error || !Array.isArray(data) || data.length === 0) {
        wrapper.innerHTML = '<div style="text-align:center;color:var(--muted);padding:2rem;">No chart data</div>';
        return;
      }

      wrapper.innerHTML = '';
      initExpandedChart(symbol, section, data);
    } catch (error) {
      console.error(`Error loading chart for ${symbol}:`, error);
      wrapper.innerHTML = '<div style="text-align:center;color:var(--muted);padding:2rem;">Failed to load</div>';
    }
  }

  function initExpandedChart(symbol, section, data) {
    const container = document.getElementById(`${section}ChartWrapper`);

    if (chartInstances[symbol]?.chart) {
      chartInstances[symbol].chart.remove();
    }

    const chart = LightweightCharts.createChart(container, {
      width: container.clientWidth,
      height: container.clientHeight,
      layout: {
        background: { type: 'solid', color: 'transparent' },
        textColor: '#8b949e',
        fontFamily: "'JetBrains Mono', monospace",
      },
      grid: {
        vertLines: { visible: false },
        horzLines: { visible: false },
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
      },
      rightPriceScale: {
        borderColor: '#30363d',
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      timeScale: {
        borderColor: '#30363d',
        timeVisible: true,
      },
      handleScroll: true,
      handleScale: true,
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#3fb950',
      downColor: '#f85149',
      borderDownColor: '#f85149',
      borderUpColor: '#3fb950',
      wickDownColor: '#f85149',
      wickUpColor: '#3fb950',
    });

    const sma50Series = chart.addLineSeries({
      color: '#00ff00',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    const sma200Series = chart.addLineSeries({
      color: '#ff0000',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    chartInstances[symbol] = {
      chart,
      series: { candleSeries, sma50Series, sma200Series },
      data,
      timeframe: 365,
      section,
    };

    const resizeObserver = new ResizeObserver(() => {
      if (chartInstances[symbol]?.chart) {
        chartInstances[symbol].chart.applyOptions({
          width: container.clientWidth,
          height: container.clientHeight,
        });
      }
    });
    resizeObserver.observe(container);

    updateChart(symbol, 365);
  }

  function updateChart(symbol, days) {
    const instance = chartInstances[symbol];
    if (!instance) return;

    const { data, series } = instance;
    instance.timeframe = days;

    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - days);
    const cutoffStr = cutoffDate.toISOString().split('T')[0];

    const sma50Full = calculateSMA(data, 50);
    const sma200Full = calculateSMA(data, 200);

    const filtered = data.filter(d => d.time >= cutoffStr);
    const filteredSma50 = sma50Full.filter(d => d.time >= cutoffStr);
    const filteredSma200 = sma200Full.filter(d => d.time >= cutoffStr);

    const candles = filtered.map(d => ({
      time: d.time,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }));

    series.candleSeries.setData(candles);
    series.sma50Series.setData(filteredSma50);
    series.sma200Series.setData(filteredSma200);

    instance.chart.timeScale().fitContent();
  }

  function calculateSMA(data, period) {
    if (data.length < period) return [];

    const smaData = [];
    for (let i = period - 1; i < data.length; i++) {
      let sum = 0;
      for (let j = 0; j < period; j++) {
        sum += data[i - j].close;
      }
      smaData.push({ time: data[i].time, value: sum / period });
    }
    return smaData;
  }

  function retClass(val, invertForVix = false) {
    if (val === null || val === undefined) return 'ret-neutral';
    if (invertForVix) {
      return val >= 0 ? 'ret-neg' : 'ret-pos';
    }
    return val >= 0 ? 'ret-pos' : 'ret-neg';
  }

  function fmtRet(val) {
    if (val === null || val === undefined) return '—';
    const sign = val >= 0 ? '+' : '';
    return sign + val.toFixed(1) + '%';
  }

  function fmtDma(val) {
    if (val === null || val === undefined) return '—';
    const sign = val >= 0 ? '+' : '';
    return sign + val.toFixed(1) + '%';
  }

  function getHotClass(val) {
    if (val === null || val === undefined) return '';
    if (val >= 2) return 'tile-hot-green';
    if (val <= -2) return 'tile-hot-red';
    return '';
  }

  async function loadBreadthData() {
    try {
      const response = await fetch('/api/frontend/market-breadth');
      const data = await response.json();

      if (data.error || !data.history || data.history.length === 0) {
        document.getElementById('dmaChartContainer').innerHTML =
          '<div style="text-align:center;color:var(--muted);padding:2rem;">Breadth data not available</div>';
        document.getElementById('moversChartContainer').innerHTML =
          '<div style="text-align:center;color:var(--muted);padding:2rem;">Breadth data not available</div>';
        return;
      }

      breadthData = data;
      updateBreadthStats(data.latest);
      initBreadthCharts();
    } catch (error) {
      console.error('Error loading breadth data:', error);
      document.getElementById('dmaChartContainer').innerHTML =
        '<div style="text-align:center;color:var(--muted);padding:2rem;">Failed to load breadth data</div>';
      document.getElementById('moversChartContainer').innerHTML =
        '<div style="text-align:center;color:var(--muted);padding:2rem;">Failed to load breadth data</div>';
    }
  }

  function updateBreadthStats(latest) {
    if (!latest) return;

    document.getElementById('above50dma').textContent = latest.above_50dma?.toLocaleString() || '—';
    document.getElementById('above50dmaPct').textContent = latest.pct_above_50dma ? `${latest.pct_above_50dma}%` : '—';
    document.getElementById('above200dma').textContent = latest.above_200dma?.toLocaleString() || '—';
    document.getElementById('above200dmaPct').textContent = latest.pct_above_200dma ? `${latest.pct_above_200dma}%` : '—';
    document.getElementById('up4pct').textContent = latest.up_4pct?.toLocaleString() || '0';
    document.getElementById('down4pct').textContent = latest.down_4pct?.toLocaleString() || '0';
  }

  function initBreadthCharts() {
    initSpyChart();
    initDmaChart();
    initMoversChart();
    setTimeout(syncAllBreadthCharts, 100);
  }

  function syncAllBreadthCharts() {
    if (!breadthData.history || breadthData.history.length === 0) return;

    const startDate = breadthData.history[0].date;
    const endDate = breadthData.history[breadthData.history.length - 1].date;
    const timeRange = { from: startDate, to: endDate };

    [spyChart, dmaChart, moversChart].forEach(chart => {
      if (chart && chart.timeScale()) {
        chart.timeScale().setVisibleRange(timeRange);
      }
    });
  }

  async function initSpyChart() {
    const container = document.getElementById('spyChartContainer');

    try {
      const response = await fetch('/api/frontend/index-ohlc/SPY');
      const data = await response.json();

      if (data.error || !Array.isArray(data) || data.length === 0) {
        container.innerHTML = '<div style="text-align:center;color:var(--muted);padding:2rem;">SPY data not available</div>';
        return;
      }

      if (breadthData.history && breadthData.history.length > 0) {
        const breadthStartDate = breadthData.history[0].date;
        spyData = data.filter(d => d.time >= breadthStartDate);
      } else {
        const cutoffDate = new Date();
        cutoffDate.setFullYear(cutoffDate.getFullYear() - 1);
        const cutoffStr = cutoffDate.toISOString().split('T')[0];
        spyData = data.filter(d => d.time >= cutoffStr);
      }
    } catch (error) {
      console.error('Error loading SPY data:', error);
      container.innerHTML = '<div style="text-align:center;color:var(--muted);padding:2rem;">Failed to load SPY data</div>';
      return;
    }

    spyChart = LightweightCharts.createChart(container, {
      width: container.clientWidth,
      height: BREADTH_CHART_HEIGHT,
      layout: {
        background: { type: 'solid', color: 'transparent' },
        textColor: '#8b949e',
        fontFamily: "'JetBrains Mono', monospace",
      },
      grid: {
        vertLines: { color: 'rgba(48, 54, 61, 0.3)' },
        horzLines: { color: 'rgba(48, 54, 61, 0.3)' },
      },
      rightPriceScale: {
        borderColor: '#30363d',
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      timeScale: {
        borderColor: '#30363d',
        timeVisible: true,
      },
      handleScroll: false,
      handleScale: false,
    });

    spySeries.candles = spyChart.addCandlestickSeries({
      upColor: '#3fb950',
      downColor: '#f85149',
      borderDownColor: '#f85149',
      borderUpColor: '#3fb950',
      wickDownColor: '#f85149',
      wickUpColor: '#3fb950',
      priceLineVisible: false,
      lastValueVisible: false,
    });

    const resizeObserver = new ResizeObserver(() => {
      if (spyChart) {
        spyChart.applyOptions({ width: container.clientWidth });
      }
    });
    resizeObserver.observe(container);

    renderSpyChart();
  }

  function renderSpyChart() {
    if (!spyChart || spyData.length === 0) return;

    const candles = spyData.map(d => ({
      time: d.time,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }));

    spySeries.candles.setData(candles);
    spyChart.timeScale().fitContent();
    spyChart.timeScale().subscribeVisibleTimeRangeChange(syncTimeRange);
  }

  function syncTimeRange(timeRange) {
    if (isSyncing || !timeRange) return;
    isSyncing = true;

    [spyChart, dmaChart, moversChart].filter(c => c).forEach(chart => {
      if (chart.timeScale()) {
        chart.timeScale().setVisibleRange(timeRange);
      }
    });

    setTimeout(() => { isSyncing = false; }, 50);
  }

  function initDmaChart() {
    const container = document.getElementById('dmaChartContainer');

    dmaChart = LightweightCharts.createChart(container, {
      width: container.clientWidth,
      height: BREADTH_CHART_HEIGHT,
      layout: {
        background: { type: 'solid', color: 'transparent' },
        textColor: '#8b949e',
        fontFamily: "'JetBrains Mono', monospace",
      },
      grid: {
        vertLines: { color: 'rgba(48, 54, 61, 0.3)' },
        horzLines: { color: 'rgba(48, 54, 61, 0.3)' },
      },
      rightPriceScale: {
        borderColor: '#30363d',
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      timeScale: {
        borderColor: '#30363d',
        timeVisible: true,
      },
      handleScroll: false,
      handleScale: false,
    });

    dmaSeries.above50dma = dmaChart.addLineSeries({
      color: '#3fb950',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    dmaSeries.above200dma = dmaChart.addLineSeries({
      color: '#58a6ff',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    const resizeObserver = new ResizeObserver(() => {
      if (dmaChart) {
        dmaChart.applyOptions({ width: container.clientWidth });
      }
    });
    resizeObserver.observe(container);

    renderDmaChart();
  }

  function initMoversChart() {
    const container = document.getElementById('moversChartContainer');

    moversChart = LightweightCharts.createChart(container, {
      width: container.clientWidth,
      height: BREADTH_CHART_HEIGHT,
      layout: {
        background: { type: 'solid', color: 'transparent' },
        textColor: '#8b949e',
        fontFamily: "'JetBrains Mono', monospace",
      },
      grid: {
        vertLines: { color: 'rgba(48, 54, 61, 0.3)' },
        horzLines: { color: 'rgba(48, 54, 61, 0.3)' },
      },
      rightPriceScale: {
        borderColor: '#30363d',
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      timeScale: {
        borderColor: '#30363d',
        timeVisible: true,
      },
      handleScroll: false,
      handleScale: false,
    });

    moversSeries.up4pct = moversChart.addHistogramSeries({
      color: '#3fb950',
      priceFormat: { type: 'volume' },
    });

    moversSeries.down4pct = moversChart.addHistogramSeries({
      color: '#f85149',
      priceFormat: { type: 'volume' },
    });

    const resizeObserver = new ResizeObserver(() => {
      if (moversChart) {
        moversChart.applyOptions({ width: container.clientWidth });
      }
    });
    resizeObserver.observe(container);

    renderMoversChart();
  }

  function renderDmaChart() {
    if (!dmaChart || !breadthData.history || breadthData.history.length === 0) return;

    const history = breadthData.history;
    const above50Data = history.map(d => ({ time: d.date, value: d.pct_above_50dma || 0 }));
    const above200Data = history.map(d => ({ time: d.date, value: d.pct_above_200dma || 0 }));

    dmaSeries.above50dma.setData(above50Data);
    dmaSeries.above200dma.setData(above200Data);

    const referenceData = history.map(d => ({ time: d.date, value: 25 }));
    if (!dmaSeries.reference) {
      dmaSeries.reference = dmaChart.addLineSeries({
        color: '#ff0000',
        lineWidth: 3,
        priceLineVisible: false,
        lastValueVisible: false,
        lineStyle: LightweightCharts.LineStyle.Solid,
      });
    }
    dmaSeries.reference.setData(referenceData);

    dmaChart.timeScale().fitContent();
    dmaChart.timeScale().subscribeVisibleTimeRangeChange(syncTimeRange);
  }

  function renderMoversChart() {
    if (!moversChart || !breadthData.history || breadthData.history.length === 0) return;

    const history = breadthData.history;
    const upData = history.map(d => ({ time: d.date, value: d.up_4pct || 0, color: '#3fb950' }));
    const downData = history.map(d => ({ time: d.date, value: -(d.down_4pct || 0), color: '#f85149' }));

    moversSeries.up4pct.setData(upData);
    moversSeries.down4pct.setData(downData);

    moversChart.timeScale().fitContent();
    moversChart.timeScale().subscribeVisibleTimeRangeChange(syncTimeRange);

    const moversLookup = {};
    history.forEach(d => {
      moversLookup[d.date] = { up: d.up_4pct || 0, down: d.down_4pct || 0 };
    });

    moversChart.subscribeCrosshairMove(param => {
      const tooltip = document.getElementById('moversTooltip');
      if (!param || !param.time || param.point === undefined) {
        tooltip.innerHTML = '';
        return;
      }

      const data = moversLookup[param.time];
      if (data) {
        tooltip.innerHTML = `<span style="color:#3fb950">▲ ${data.up}</span> &nbsp; <span style="color:#f85149">▼ ${data.down}</span>`;
      }
    });
  }
})();
