/**
 * Shared Stock Chart Module
 * 
 * Provides consistent charting functionality across all pages.
 * Uses TradingView Lightweight Charts library.
 * 
 * Usage:
 *   const chart = new StockChart('container-id', { height: 300 });
 *   await chart.load('AAPL');
 *   chart.setTimeframe(90); // 90 days
 */

// ============================================================================
// Chart Configuration Constants
// ============================================================================

const CHART_CONFIG = {
    // Candlestick colors
    upColor: '#3fb950',
    downColor: '#f85149',
    
    // Moving average colors
    ema10Color: '#ffffff',
    ema20Color: '#ffa500',
    dma50Color: '#00ff00',
    dma200Color: '#ff0000',
    
    // RSI color
    rsiColor: '#00bcd4',
    
    // Volume colors
    volumeUpColor: 'rgba(63, 185, 80, 0.7)',
    volumeDownColor: 'rgba(248, 81, 73, 0.7)',
    
    // Earnings marker colors
    earningsBeatColor: '#90EE90',
    earningsMissColor: '#FFB6C1',
    earningsNeutralColor: '#f5f5dc',
    
    // Spike/Gap marker colors
    spikeMarkerColor: '#bf40bf',
    gapMarkerColor: '#9932cc',
    
    // Grid and text colors
    gridColor: 'rgba(48, 54, 61, 0.3)',
    textColor: '#8b949e',
    borderColor: '#30363d',
    crosshairColor: 'rgba(88, 166, 255, 0.5)',
    
    // Default timeframe in days
    defaultTimeframe: 365,
    
    // Chart dimensions
    defaultHeight: 300,
    rsiHeight: 120,
    
    // Height ratio: candlestick:volume = 3:1
    candlestickRatio: 0.75,
    volumeRatio: 0.25,
};

// ============================================================================
// Timeframe Preference (persists across charts and page loads)
// ============================================================================

const TIMEFRAME_STORAGE_KEY = 'stockChartTimeframe';

function getStoredTimeframe() {
    try {
        const stored = localStorage.getItem(TIMEFRAME_STORAGE_KEY);
        if (stored) {
            const days = parseInt(stored, 10);
            if (!isNaN(days) && days > 0) {
                return days;
            }
        }
    } catch (e) {
        // localStorage not available
    }
    return CHART_CONFIG.defaultTimeframe;
}

function setStoredTimeframe(days) {
    try {
        localStorage.setItem(TIMEFRAME_STORAGE_KEY, days.toString());
    } catch (e) {
        // localStorage not available
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Calculate EMA (Exponential Moving Average) from OHLC data
 * Server provides DMA (50/200), but not EMA (10/20), so we calculate client-side
 */
function calculateEMA(data, period) {
    if (!data || data.length < period) return [];
    
    const emaData = [];
    const multiplier = 2 / (period + 1);
    let ema = null;
    
    for (let i = 0; i < data.length; i++) {
        if (i < period - 1) {
            continue;
        } else if (i === period - 1) {
            // First EMA is SMA
            let sum = 0;
            for (let j = 0; j < period; j++) {
                sum += data[j].close;
            }
            ema = sum / period;
        } else {
            ema = (data[i].close - ema) * multiplier + ema;
        }
        
        if (ema !== null) {
            emaData.push({ time: data[i].time, value: ema });
        }
    }
    
    return emaData;
}

/**
 * Fetch OHLC data from backend API
 */
async function fetchOHLCData(ticker) {
    const response = await fetch(`/api/frontend/ohlc/${ticker}`);
    const data = await response.json();
    
    if (data.error || !Array.isArray(data)) {
        throw new Error(data.error || 'Failed to fetch OHLC data');
    }
    
    return data;
}

/**
 * Fetch earnings EPS data from backend API
 */
async function fetchEarningsData(ticker) {
    try {
        const response = await fetch(`/api/frontend/earnings-eps/${ticker}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
    } catch (e) {
        console.warn(`Could not fetch earnings for ${ticker}:`, e);
        return [];
    }
}

/**
 * Fetch volume spike and gap events from backend API
 */
async function fetchVolspikeEvents(ticker) {
    try {
        const response = await fetch(`/api/frontend/volspike-events/${ticker}`);
        const data = await response.json();
        return {
            spikeDays: data.spike_days || [],
            gapDays: data.gap_days || [],
        };
    } catch (e) {
        console.warn(`Could not fetch volspike events for ${ticker}:`, e);
        return { spikeDays: [], gapDays: [] };
    }
}

// ============================================================================
// StockChart Class
// ============================================================================

class StockChart {
    /**
     * Create a new StockChart instance
     * 
     * @param {string} containerId - ID of the container element
     * @param {Object} options - Configuration options
     * @param {number} options.height - Chart height (default: 300)
     * @param {boolean} options.showRSI - Show RSI sub-chart (default: false)
     * @param {string} options.rsiContainerId - ID of RSI container (required if showRSI is true)
     */
    constructor(containerId, options = {}) {
        this.containerId = containerId;
        this.options = {
            height: options.height || CHART_CONFIG.defaultHeight,
            showRSI: options.showRSI || false,
            rsiContainerId: options.rsiContainerId || null,
        };
        
        this.chart = null;
        this.volumeChart = null;
        this.rsiChart = null;
        this.series = {};
        this.volumeSeries = null;
        this.rsiSeries = null;
        this.allData = [];
        this.earningsData = [];
        this.spikeDays = [];
        this.gapDays = [];
        this.currentTimeframe = getStoredTimeframe();
        this.ticker = null;
        this.resizeHandler = null;
        
        // Dynamic container references
        this.priceContainer = null;
        this.volumeContainer = null;
        this.legendContainer = null;
        
        // Track series visibility
        this.seriesVisibility = {
            ema10: true,
            ema20: true,
            dma50: true,
            dma200: true,
        };
    }
    
    /**
     * Load chart data and render
     * 
     * @param {string} ticker - Stock ticker symbol
     * @returns {Promise<void>}
     */
    async load(ticker) {
        this.ticker = ticker.toUpperCase();
        
        // Fetch all data in parallel (OHLC, earnings, and volspike events)
        const [ohlcData, earningsData, volspikeEvents] = await Promise.all([
            fetchOHLCData(this.ticker),
            fetchEarningsData(this.ticker),
            fetchVolspikeEvents(this.ticker),
        ]);
        
        if (!ohlcData || ohlcData.length === 0) {
            throw new Error('No chart data available');
        }
        
        this.allData = ohlcData;
        this.earningsData = earningsData;
        this.spikeDays = volspikeEvents.spikeDays;
        this.gapDays = volspikeEvents.gapDays;
        
        // Initialize chart
        this._initChart();
        
        // Render with default timeframe
        this.setTimeframe(this.currentTimeframe);
        
        return this;
    }
    
    /**
     * Set the chart timeframe
     * 
     * @param {number} days - Number of days to display
     */
    setTimeframe(days) {
        this.currentTimeframe = days;
        setStoredTimeframe(days); // Persist for next chart
        this._updateChartData();
        
        // Fit content on all charts
        if (this.chart) {
            this.chart.timeScale().fitContent();
        }
        if (this.volumeChart) {
            this.volumeChart.timeScale().fitContent();
        }
        if (this.rsiChart) {
            this.rsiChart.timeScale().fitContent();
        }
    }
    
    /**
     * Get chart statistics for current view
     * 
     * @returns {Object} Stats object with change, high, low
     */
    getStats() {
        const filteredData = this._getFilteredData();
        
        if (!filteredData || filteredData.length === 0) {
            return { change: null, high: null, low: null };
        }
        
        const oldest = filteredData[0];
        const latest = filteredData[filteredData.length - 1];
        const change = ((latest.close - oldest.open) / oldest.open * 100);
        const high = Math.max(...filteredData.map(d => d.high));
        const low = Math.min(...filteredData.map(d => d.low));
        
        return {
            change: change.toFixed(2),
            high: high.toFixed(2),
            low: low.toFixed(2),
        };
    }
    
    /**
     * Destroy the chart and clean up resources
     */
    destroy() {
        if (this.resizeHandler) {
            window.removeEventListener('resize', this.resizeHandler);
            this.resizeHandler = null;
        }
        
        if (this.chart) {
            this.chart.remove();
            this.chart = null;
        }
        
        if (this.volumeChart) {
            this.volumeChart.remove();
            this.volumeChart = null;
        }
        
        if (this.rsiChart) {
            this.rsiChart.remove();
            this.rsiChart = null;
        }
        
        // Clean up dynamic containers
        if (this.legendContainer) {
            this.legendContainer.remove();
            this.legendContainer = null;
        }
        if (this.priceContainer) {
            this.priceContainer.remove();
            this.priceContainer = null;
        }
        if (this.volumeContainer) {
            this.volumeContainer.remove();
            this.volumeContainer = null;
        }
        
        this.series = {};
        this.volumeSeries = null;
        this.rsiSeries = null;
    }
    
    /**
     * Toggle visibility of a moving average series
     * @param {string} seriesName - Name of the series (ema10, ema20, dma50, dma200)
     * @param {boolean} visible - Whether to show the series
     */
    toggleSeries(seriesName, visible) {
        if (this.series[seriesName]) {
            this.seriesVisibility[seriesName] = visible;
            this.series[seriesName].applyOptions({
                visible: visible,
            });
        }
    }
    
    // ========================================================================
    // Private Methods
    // ========================================================================
    
    _initChart() {
        const container = document.getElementById(this.containerId);
        if (!container) {
            throw new Error(`Container not found: ${this.containerId}`);
        }
        
        // Calculate heights: 3:1 ratio for price:volume
        const totalHeight = this.options.height;
        const priceHeight = Math.floor(totalHeight * CHART_CONFIG.candlestickRatio);
        const volumeHeight = totalHeight - priceHeight;
        
        // Create legend with MA toggles
        this._createLegend(container);
        
        // Create inner containers for price and volume charts
        this.priceContainer = document.createElement('div');
        this.priceContainer.style.width = '100%';
        this.priceContainer.style.height = `${priceHeight}px`;
        container.appendChild(this.priceContainer);
        
        this.volumeContainer = document.createElement('div');
        this.volumeContainer.style.width = '100%';
        this.volumeContainer.style.height = `${volumeHeight}px`;
        container.appendChild(this.volumeContainer);
        
        // Create main price chart
        this.chart = LightweightCharts.createChart(this.priceContainer, {
            width: container.clientWidth,
            height: priceHeight,
            layout: {
                background: { type: 'solid', color: 'transparent' },
                textColor: CHART_CONFIG.textColor,
                fontFamily: "'JetBrains Mono', monospace",
            },
            grid: {
                vertLines: { color: CHART_CONFIG.gridColor },
                horzLines: { color: CHART_CONFIG.gridColor },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    width: 1,
                    color: CHART_CONFIG.crosshairColor,
                    style: LightweightCharts.LineStyle.Dashed,
                },
                horzLine: {
                    width: 1,
                    color: CHART_CONFIG.crosshairColor,
                    style: LightweightCharts.LineStyle.Dashed,
                },
            },
            rightPriceScale: {
                borderColor: CHART_CONFIG.borderColor,
                scaleMargins: { top: 0.05, bottom: 0.05 },
            },
            timeScale: {
                borderColor: CHART_CONFIG.borderColor,
                timeVisible: true,
                secondsVisible: false,
                visible: false, // Hide time scale on price chart, show on volume
            },
            handleScroll: false,
            handleScale: false,
        });
        
        // Add candlestick series
        this.series.candlestick = this.chart.addCandlestickSeries({
            upColor: CHART_CONFIG.upColor,
            downColor: CHART_CONFIG.downColor,
            borderDownColor: CHART_CONFIG.downColor,
            borderUpColor: CHART_CONFIG.upColor,
            wickDownColor: CHART_CONFIG.downColor,
            wickUpColor: CHART_CONFIG.upColor,
        });
        
        // Add moving average series (using server-provided DMA for 50/200)
        this.series.ema10 = this.chart.addLineSeries({
            color: CHART_CONFIG.ema10Color,
            lineWidth: 1,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
        });
        
        this.series.ema20 = this.chart.addLineSeries({
            color: CHART_CONFIG.ema20Color,
            lineWidth: 1,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
        });
        
        this.series.dma50 = this.chart.addLineSeries({
            color: CHART_CONFIG.dma50Color,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
        });
        
        this.series.dma200 = this.chart.addLineSeries({
            color: CHART_CONFIG.dma200Color,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
        });
        
        // Create separate volume chart
        this._initVolumeChart(container.clientWidth, volumeHeight);
        
        // Initialize RSI chart if enabled
        if (this.options.showRSI && this.options.rsiContainerId) {
            this._initRSIChart();
        }
        
        // Setup resize handler
        this.resizeHandler = () => {
            const width = container.clientWidth;
            if (this.chart) {
                this.chart.applyOptions({ width });
            }
            if (this.volumeChart) {
                this.volumeChart.applyOptions({ width });
            }
            if (this.rsiChart) {
                const rsiContainer = document.getElementById(this.options.rsiContainerId);
                if (rsiContainer) {
                    this.rsiChart.applyOptions({ width: rsiContainer.clientWidth });
                }
            }
        };
        window.addEventListener('resize', this.resizeHandler);
    }
    
    _createLegend(container) {
        this.legendContainer = document.createElement('div');
        this.legendContainer.style.cssText = `
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            padding: 6px 8px;
            font-size: 11px;
            font-family: 'JetBrains Mono', monospace;
            background: rgba(22, 27, 34, 0.8);
            border-radius: 4px;
            margin-bottom: 4px;
        `;
        
        const maConfigs = [
            { key: 'ema10', label: '10 EMA', color: CHART_CONFIG.ema10Color },
            { key: 'ema20', label: '20 EMA', color: CHART_CONFIG.ema20Color },
            { key: 'dma50', label: '50 DMA', color: CHART_CONFIG.dma50Color },
            { key: 'dma200', label: '200 DMA', color: CHART_CONFIG.dma200Color },
        ];
        
        maConfigs.forEach(config => {
            const item = document.createElement('label');
            item.style.cssText = `
                display: flex;
                align-items: center;
                gap: 4px;
                cursor: pointer;
                user-select: none;
                color: ${CHART_CONFIG.textColor};
            `;
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = this.seriesVisibility[config.key];
            checkbox.style.cssText = `
                cursor: pointer;
                accent-color: ${config.color};
                width: 14px;
                height: 14px;
            `;
            
            const colorDot = document.createElement('span');
            colorDot.style.cssText = `
                display: inline-block;
                width: 10px;
                height: 3px;
                background: ${config.color};
                border-radius: 1px;
            `;
            
            const labelText = document.createElement('span');
            labelText.textContent = config.label;
            labelText.style.color = config.color;
            
            checkbox.addEventListener('change', (e) => {
                e.stopPropagation();
                this.toggleSeries(config.key, checkbox.checked);
            });
            
            // Prevent click from bubbling to row toggle
            item.addEventListener('click', (e) => e.stopPropagation());
            
            item.appendChild(checkbox);
            item.appendChild(colorDot);
            item.appendChild(labelText);
            this.legendContainer.appendChild(item);
        });
        
        container.appendChild(this.legendContainer);
    }
    
    _initVolumeChart(width, height) {
        this.volumeChart = LightweightCharts.createChart(this.volumeContainer, {
            width: width,
            height: height,
            layout: {
                background: { type: 'solid', color: 'transparent' },
                textColor: CHART_CONFIG.textColor,
                fontFamily: "'JetBrains Mono', monospace",
            },
            grid: {
                vertLines: { color: CHART_CONFIG.gridColor },
                horzLines: { color: CHART_CONFIG.gridColor },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    width: 1,
                    color: CHART_CONFIG.crosshairColor,
                    style: LightweightCharts.LineStyle.Dashed,
                },
                horzLine: {
                    width: 1,
                    color: CHART_CONFIG.crosshairColor,
                    style: LightweightCharts.LineStyle.Dashed,
                },
            },
            rightPriceScale: {
                borderColor: CHART_CONFIG.borderColor,
                scaleMargins: { top: 0.1, bottom: 0.1 },
            },
            timeScale: {
                borderColor: CHART_CONFIG.borderColor,
                timeVisible: true,
                secondsVisible: false,
            },
            handleScroll: false,
            handleScale: false,
        });
        
        this.volumeSeries = this.volumeChart.addHistogramSeries({
            priceFormat: { type: 'volume' },
            priceScaleId: 'right',
        });
        
        // Sync time scales between price and volume charts
        this.chart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (range && this.volumeChart) {
                this.volumeChart.timeScale().setVisibleLogicalRange(range);
            }
        });
        
        this.volumeChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (range && this.chart) {
                this.chart.timeScale().setVisibleLogicalRange(range);
            }
        });
        
        // Sync crosshairs between price and volume charts
        this.chart.subscribeCrosshairMove(param => {
            if (param.time && this.volumeChart) {
                this.volumeChart.setCrosshairPosition(param.point?.y || 0, param.time, this.volumeSeries);
            }
        });
        
        this.volumeChart.subscribeCrosshairMove(param => {
            if (param.time) {
                if (this.chart) {
                    this.chart.setCrosshairPosition(param.point?.y || 0, param.time, this.series.candlestick);
                }
                // Also sync with RSI chart if it exists
                if (this.rsiChart && this.rsiSeries) {
                    this.rsiChart.setCrosshairPosition(param.point?.y || 0, param.time, this.rsiSeries);
                }
            }
        });
    }
    
    _initRSIChart() {
        const rsiContainer = document.getElementById(this.options.rsiContainerId);
        if (!rsiContainer) {
            console.warn(`RSI container not found: ${this.options.rsiContainerId}`);
            return;
        }
        
        this.rsiChart = LightweightCharts.createChart(rsiContainer, {
            width: rsiContainer.clientWidth,
            height: CHART_CONFIG.rsiHeight,
            layout: {
                background: { type: 'solid', color: 'transparent' },
                textColor: CHART_CONFIG.textColor,
                fontFamily: "'JetBrains Mono', monospace",
            },
            grid: {
                vertLines: { color: CHART_CONFIG.gridColor },
                horzLines: { color: CHART_CONFIG.gridColor },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    width: 1,
                    color: CHART_CONFIG.crosshairColor,
                    style: LightweightCharts.LineStyle.Dashed,
                },
                horzLine: {
                    width: 1,
                    color: CHART_CONFIG.crosshairColor,
                    style: LightweightCharts.LineStyle.Dashed,
                },
            },
            rightPriceScale: {
                borderColor: CHART_CONFIG.borderColor,
                scaleMargins: { top: 0.1, bottom: 0.1 },
                autoScale: true,
            },
            timeScale: {
                borderColor: CHART_CONFIG.borderColor,
                timeVisible: true,
                secondsVisible: false,
                visible: false,
            },
            handleScroll: false,
            handleScale: false,
        });
        
        this.rsiSeries = this.rsiChart.addAreaSeries({
            lineColor: CHART_CONFIG.rsiColor,
            topColor: 'rgba(0, 188, 212, 0.4)',
            bottomColor: 'rgba(0, 188, 212, 0.05)',
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            crosshairMarkerVisible: true,
        });
        
        // Sync time scale with price chart
        this.chart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (range && this.rsiChart) {
                this.rsiChart.timeScale().setVisibleLogicalRange(range);
            }
        });
        
        // Sync crosshairs between all charts
        this.chart.subscribeCrosshairMove(param => {
            if (param.time && this.rsiChart) {
                this.rsiChart.setCrosshairPosition(param.point?.y || 0, param.time, this.rsiSeries);
            }
        });
        
        this.rsiChart.subscribeCrosshairMove(param => {
            if (param.time) {
                if (this.chart) {
                    this.chart.setCrosshairPosition(param.point?.y || 0, param.time, this.series.candlestick);
                }
                if (this.volumeChart) {
                    this.volumeChart.setCrosshairPosition(param.point?.y || 0, param.time, this.volumeSeries);
                }
            }
        });
    }
    
    _getFilteredData() {
        if (!this.allData || this.allData.length === 0) return [];
        
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - this.currentTimeframe);
        const cutoffStr = cutoffDate.toISOString().split('T')[0];
        
        const filtered = this.allData.filter(d => d.time >= cutoffStr);
        return filtered.length > 0 ? filtered : this.allData;
    }
    
    _updateChartData() {
        const filteredData = this._getFilteredData();
        if (!filteredData || filteredData.length === 0) return;
        
        // Candlestick data
        const candleData = filteredData.map(d => ({
            time: d.time,
            open: d.open,
            high: d.high,
            low: d.low,
            close: d.close,
        }));
        this.series.candlestick.setData(candleData);
        
        // Calculate EMAs from full dataset, then filter to visible range
        const ema10Full = calculateEMA(this.allData, 10);
        const ema20Full = calculateEMA(this.allData, 20);
        
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - this.currentTimeframe);
        const cutoffStr = cutoffDate.toISOString().split('T')[0];
        
        const ema10Filtered = ema10Full.filter(d => d.time >= cutoffStr);
        const ema20Filtered = ema20Full.filter(d => d.time >= cutoffStr);
        
        this.series.ema10.setData(ema10Filtered);
        this.series.ema20.setData(ema20Filtered);
        
        // Use server-provided DMA values (not calculated client-side)
        const dma50Data = filteredData
            .filter(d => d.dma_50 != null)
            .map(d => ({ time: d.time, value: d.dma_50 }));
        this.series.dma50.setData(dma50Data);
        
        const dma200Data = filteredData
            .filter(d => d.dma_200 != null)
            .map(d => ({ time: d.time, value: d.dma_200 }));
        this.series.dma200.setData(dma200Data);
        
        // Volume data (on separate volume chart)
        if (this.volumeSeries) {
            const volumeData = filteredData.map(d => ({
                time: d.time,
                value: d.volume,
                color: d.close >= d.open ? CHART_CONFIG.volumeUpColor : CHART_CONFIG.volumeDownColor,
            }));
            this.volumeSeries.setData(volumeData);
        }
        
        // RSI data
        if (this.rsiSeries) {
            const rsiData = filteredData
                .filter(d => d.rsi_mktcap != null)
                .map(d => ({ time: d.time, value: d.rsi_mktcap }));
            this.rsiSeries.setData(rsiData);
        }
        
        // Build markers
        this._setMarkers(filteredData);
    }
    
    _setMarkers(filteredData) {
        const allMarkers = [];
        const dataTimeSet = new Set(filteredData.map(d => d.time));
        
        // Earnings markers
        const filteredEarnings = this.earningsData.filter(e => dataTimeSet.has(e.time));
        filteredEarnings.forEach(earning => {
            const actualText = earning.eps_actual != null ? `A: $${earning.eps_actual}` : '';
            const estText = earning.eps_estimated != null ? `E: $${earning.eps_estimated}` : '';
            let labelText = '';
            if (actualText && estText) {
                labelText = `${actualText} | ${estText}`;
            } else {
                labelText = actualText || estText || 'EPS';
            }
            
            let color = CHART_CONFIG.earningsNeutralColor;
            if (earning.eps_actual != null && earning.eps_estimated != null) {
                color = earning.eps_actual >= earning.eps_estimated 
                    ? CHART_CONFIG.earningsBeatColor 
                    : CHART_CONFIG.earningsMissColor;
            }
            
            allMarkers.push({
                time: earning.time,
                position: 'belowBar',
                color: color,
                shape: 'arrowUp',
                text: labelText,
            });
        });
        
        // Volume spike markers (fetched from API)
        if (this.spikeDays && this.spikeDays.length > 0) {
            this.spikeDays.forEach(date => {
                if (dataTimeSet.has(date)) {
                    allMarkers.push({
                        time: date,
                        position: 'aboveBar',
                        color: CHART_CONFIG.spikeMarkerColor,
                        shape: 'circle',
                        text: 'Spike',
                        size: 1,
                    });
                }
            });
        }
        
        // Gap markers (fetched from API)
        if (this.gapDays && this.gapDays.length > 0) {
            this.gapDays.forEach(date => {
                if (dataTimeSet.has(date)) {
                    allMarkers.push({
                        time: date,
                        position: 'aboveBar',
                        color: CHART_CONFIG.gapMarkerColor,
                        shape: 'circle',
                        text: 'Gap',
                        size: 1,
                    });
                }
            });
        }
        
        // Sort markers by time (required by lightweight-charts)
        allMarkers.sort((a, b) => a.time.localeCompare(b.time));
        
        this.series.candlestick.setMarkers(allMarkers);
    }
}

// ============================================================================
// Convenience Function for Simple Use Cases
// ============================================================================

/**
 * Draw a stock chart in the specified container
 * 
 * @param {string} ticker - Stock ticker symbol
 * @param {string} containerId - ID of the container element
 * @param {Object} options - Configuration options (same as StockChart constructor)
 * @returns {Promise<StockChart>} The created chart instance
 */
async function drawChart(ticker, containerId, options = {}) {
    const chart = new StockChart(containerId, options);
    await chart.load(ticker);
    return chart;
}

// ============================================================================
// Chart Instance Manager (for pages with multiple inline charts)
// ============================================================================

const ChartManager = {
    instances: {},
    
    /**
     * Create or get a chart instance for a ticker
     */
    async create(ticker, containerId, options = {}) {
        // Clean up existing instance if any
        this.destroy(ticker);
        
        const chart = new StockChart(containerId, options);
        await chart.load(ticker);
        this.instances[ticker] = chart;
        return chart;
    },
    
    /**
     * Get an existing chart instance
     */
    get(ticker) {
        return this.instances[ticker] || null;
    },
    
    /**
     * Destroy a chart instance
     */
    destroy(ticker) {
        if (this.instances[ticker]) {
            this.instances[ticker].destroy();
            delete this.instances[ticker];
        }
    },
    
    /**
     * Destroy all chart instances
     */
    destroyAll() {
        Object.keys(this.instances).forEach(ticker => this.destroy(ticker));
    },
};

// Export for use in modules (if needed)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { StockChart, drawChart, ChartManager, CHART_CONFIG, calculateEMA };
}

