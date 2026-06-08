(function () {
  'use strict';

  const chartInstances = {};

  document.addEventListener('DOMContentLoaded', () => {
    loadContextChart('QQQ', 'qqqChartWrapper');
    loadContextChart('SPY', 'spyChartWrapper');
    setupTimeframeButtons();
  });

  function setupTimeframeButtons() {
    document.querySelectorAll('.ctx-tf-bar').forEach(group => {
      const symbol = group.dataset.symbol;
      group.querySelectorAll('button').forEach(btn => {
        btn.addEventListener('click', () => {
          const days = parseInt(btn.dataset.days, 10);
          group.querySelectorAll('button').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
          if (chartInstances[symbol]) updateContextChart(symbol, days);
        });
      });
    });
  }

  async function loadContextChart(symbol, containerId) {
    const container = document.getElementById(containerId);
    try {
      const response = await fetch(`/api/frontend/index-ohlc/${symbol}`);
      const data = await response.json();
      if (data.error || !Array.isArray(data) || data.length === 0) {
        container.innerHTML = '<div class="md-empty">No data available</div>';
        return;
      }
      container.innerHTML = '';
      initContextChart(symbol, containerId, data);
    } catch (error) {
      console.error(`Error loading ${symbol}:`, error);
      container.innerHTML = '<div class="md-empty">Failed to load</div>';
    }
  }

  function initContextChart(symbol, containerId, data) {
    const container = document.getElementById(containerId);
    if (chartInstances[symbol]?.chart) chartInstances[symbol].chart.remove();

    const chart = LightweightCharts.createChart(container, {
      width: container.clientWidth,
      height: container.clientHeight,
      layout: {
        background: { type: 'solid', color: 'transparent' },
        textColor: '#8b949e',
        fontFamily: "'JetBrains Mono', monospace",
      },
      grid: { vertLines: { visible: false }, horzLines: { visible: false } },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: {
        borderColor: '#30363d',
        scaleMargins: { top: 0.05, bottom: 0.05 },
        minimumWidth: 56,
      },
      timeScale: { borderColor: '#30363d', timeVisible: true },
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
      priceLineVisible: false,
    });

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
      priceLineVisible: false,
      lastValueVisible: false,
    });
    chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });

    const sma50Series = chart.addLineSeries({
      color: '#00ff00',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    });

    const sma200Series = chart.addLineSeries({
      color: '#ff4444',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    });

    chartInstances[symbol] = { chart, series: { candleSeries, volumeSeries, sma50Series, sma200Series }, data, timeframe: 365 };

    const ro = new ResizeObserver(() => {
      if (chartInstances[symbol]?.chart) {
        chartInstances[symbol].chart.applyOptions({
          width: container.clientWidth,
          height: container.clientHeight,
        });
      }
    });
    ro.observe(container);
    updateContextChart(symbol, 365);
  }

  function updateContextChart(symbol, days) {
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

    series.candleSeries.setData(filtered.map(d => ({
      time: d.time, open: d.open, high: d.high, low: d.low, close: d.close,
    })));
    series.volumeSeries.setData(filtered.map(d => ({
      time: d.time,
      value: d.volume,
      color: d.close >= d.open ? 'rgba(63, 185, 80, 0.4)' : 'rgba(248, 81, 73, 0.4)',
    })));
    series.sma50Series.setData(sma50Full.filter(d => d.time >= cutoffStr));
    series.sma200Series.setData(sma200Full.filter(d => d.time >= cutoffStr));
    instance.chart.timeScale().fitContent();
  }

  function calculateSMA(data, period) {
    if (data.length < period) return [];
    const smaData = [];
    for (let i = period - 1; i < data.length; i++) {
      let sum = 0;
      for (let j = 0; j < period; j++) sum += data[i - j].close;
      smaData.push({ time: data[i].time, value: sum / period });
    }
    return smaData;
  }
})();
