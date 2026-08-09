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
    
    // Height ratio: candlestick:volume = ~72:28 (matches thinkorswim layout
    // measured from reference screenshot).
    candlestickRatio: 0.72,
    volumeRatio: 0.28,

    /** Logical bars of empty space after the last bar (time scale) */
    rightBarOffset: 3,
};

// ============================================================================
// Timeframe Preference (persists across charts and page loads)
// ============================================================================

const TIMEFRAME_STORAGE_KEY = 'stockChartTimeframe';
const MA_VISIBILITY_STORAGE_KEY = 'stockChartMAVisibility';

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

function getStoredMAVisibility() {
    try {
        const stored = localStorage.getItem(MA_VISIBILITY_STORAGE_KEY);
        if (stored) {
            return JSON.parse(stored);
        }
    } catch (e) {
        // localStorage not available or invalid JSON
    }
    return { ema10: false, ema20: false, dma50: false, dma200: false };
}

function setStoredMAVisibility(visibility) {
    try {
        localStorage.setItem(MA_VISIBILITY_STORAGE_KEY, JSON.stringify(visibility));
    } catch (e) {
        // localStorage not available
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Calculate EMA (Exponential Moving Average) from OHLC data
 * Fallback for when server-provided EMA is not available
 * Server now provides EMA (10/20) along with DMA (50/200)
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

/**
 * Fetch headline fundamentals (market cap, current + forward PE/PS, rev
 * growth) for the prominent metrics strip rendered in the chart legend.
 */
async function fetchStockMetrics(ticker) {
    try {
        const response = await fetch(`/api/frontend/stock/${ticker}`);
        const data = await response.json();
        if (!data || data.error) return null;
        return {
            current_price: data.current_price,
            dr_1: data.dr_1,
            company_name: data.company_name,
            sector: data.sector,
            industry: data.industry,
            market_cap: data.market_cap,
            pe_t: data.pe_t,
            pe_t_plus_1: data.pe_t_plus_1,
            ps_t: data.ps_t,
            ps_t_plus_1: data.ps_t_plus_1,
            rev_growth_t_plus_1: data.rev_growth_t_plus_1,
        };
    } catch (e) {
        console.warn(`Could not fetch metrics for ${ticker}:`, e);
        return null;
    }
}

/**
 * Format a market-cap dollar amount using compact suffixes (T/B/M/K).
 */
function formatMarketCapCompact(num) {
    if (num == null) return '--';
    if (num >= 1e12) return '$' + (num / 1e12).toFixed(2) + 'T';
    if (num >= 1e9) return '$' + (num / 1e9).toFixed(2) + 'B';
    if (num >= 1e6) return '$' + (num / 1e6).toFixed(1) + 'M';
    if (num >= 1e3) return '$' + (num / 1e3).toFixed(1) + 'K';
    return '$' + num.toLocaleString();
}

/**
 * Color for market-cap values — bright yellow regardless of magnitude.
 */
function marketCapColor(num) {
    if (num == null) return 'var(--text-muted, #6e7681)';
    return '#ffd60a';
}

/**
 * Format a forward valuation ratio (PE / PS). Returns '--' for non-positive
 * or missing values, which are not meaningful for these multiples.
 */
function formatForwardRatio(value, decimals = 1) {
    if (value == null || value <= 0) return '--';
    return Number(value).toFixed(decimals);
}

/**
 * Format a growth percentage with a leading sign for positive values.
 */
function formatGrowthPct(value) {
    if (value == null) return '--';
    const sign = value >= 0 ? '+' : '';
    return sign + Number(value).toFixed(1) + '%';
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
     * @param {boolean} options.showVolspikeMarkers - Show spike/gap markers on chart (default: true)
     * @param {boolean} options.compact - Narrow price scale for tight layouts (default: false)
     */
    constructor(containerId, options = {}) {
        this.containerId = containerId;
        this.options = {
            height: options.height || CHART_CONFIG.defaultHeight,
            showRSI: options.showRSI || false,
            rsiContainerId: options.rsiContainerId || null,
            showVolspikeMarkers: options.showVolspikeMarkers !== false,
            compact: options.compact || false,
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
        this.metricsData = null;
        this.currentTimeframe = getStoredTimeframe();
        this.ticker = null;
        this.resizeHandler = null;
        
        // Dynamic container references
        this.priceContainer = null;
        this.volumeContainer = null;
        this.legendContainer = null;
        this.identityOverlay = null;
        
        // Track series visibility (restored from localStorage)
        this.seriesVisibility = getStoredMAVisibility();
    }
    
    /**
     * Load chart data and render
     * 
     * @param {string} ticker - Stock ticker symbol
     * @returns {Promise<void>}
     */
    async load(ticker) {
        this.ticker = ticker.toUpperCase();
        
        // Fetch all data in parallel (OHLC, earnings, volspike events, and
        // headline fundamentals for the prominent metrics strip in the legend).
        const fetches = [
            fetchOHLCData(this.ticker),
            fetchEarningsData(this.ticker),
            fetchStockMetrics(this.ticker),
        ];
        if (this.options.showVolspikeMarkers) {
            fetches.push(fetchVolspikeEvents(this.ticker));
        }
        const results = await Promise.all(fetches);
        const ohlcData = results[0];
        const earningsData = results[1];
        this.metricsData = results[2];
        const volspikeEvents = this.options.showVolspikeMarkers
            ? results[3]
            : { spikeDays: [], gapDays: [] };
        
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
        
        // Fit content on all charts (re-apply right offset after fit)
        const ro = { rightOffset: CHART_CONFIG.rightBarOffset };
        if (this.chart) {
            this.chart.timeScale().fitContent();
            this.chart.timeScale().applyOptions(ro);
        }
        if (this.volumeChart) {
            this.volumeChart.timeScale().fitContent();
            this.volumeChart.timeScale().applyOptions(ro);
        }
        if (this.rsiChart) {
            this.rsiChart.timeScale().fitContent();
            this.rsiChart.timeScale().applyOptions(ro);
        }
        this._schedulePriceScaleSync();
    }
    
    /**
     * Resize chart panes to fit the given outer height and width.
     *
     * @param {number} totalHeight - Available height for the chart container
     * @param {number} totalWidth - Available width for the chart container
     */
    resizeToHeight(totalHeight, totalWidth) {
        if (!this.chart || !totalWidth) return;

        const container = document.getElementById(this.containerId);
        const height = Math.max(totalHeight, 120);
        const legendH = this.legendContainer ? this.legendContainer.offsetHeight : 0;
        const chartAreaH = height - legendH;
        if (chartAreaH < 80) return;

        const minVolumeH = this.options.compact ? 44 : 40;
        let volumeH = Math.floor(chartAreaH * CHART_CONFIG.volumeRatio);
        if (volumeH < minVolumeH && chartAreaH > minVolumeH + 100) {
            volumeH = minVolumeH;
        }
        let priceH = chartAreaH - volumeH;
        if (priceH < 60) {
            priceH = Math.max(chartAreaH - minVolumeH, 60);
            volumeH = chartAreaH - priceH;
        }

        if (container) {
            container.style.height = height + 'px';
            container.style.maxHeight = height + 'px';
            container.style.flex = '0 0 auto';
        }
        if (this.priceContainer) {
            this.priceContainer.style.flex = '0 0 auto';
            this.priceContainer.style.height = priceH + 'px';
            this.priceContainer.style.marginBottom = '0';
        }
        if (this.volumeContainer) {
            this.volumeContainer.style.flex = '0 0 auto';
            this.volumeContainer.style.height = volumeH + 'px';
            this.volumeContainer.style.marginTop = '0';
        }

        this.chart.applyOptions({ height: priceH, width: totalWidth });
        if (this.volumeChart) {
            this.volumeChart.applyOptions({ height: volumeH, width: totalWidth });
        }
        this._refitTimeScales();
        this._schedulePriceScaleSync();
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
        if (this.identityOverlay) {
            this.identityOverlay.remove();
            this.identityOverlay = null;
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
            setStoredMAVisibility(this.seriesVisibility);
        }
    }
    
    // ========================================================================
    // Private Methods
    // ========================================================================

    _layoutOptions() {
        return {
            background: { type: 'solid', color: 'transparent' },
            textColor: CHART_CONFIG.textColor,
            fontFamily: "'JetBrains Mono', monospace",
            ...(this.options.compact ? { fontSize: 10 } : {}),
        };
    }

    _priceScaleOptions(scaleMargins) {
        return {
            borderColor: CHART_CONFIG.borderColor,
            scaleMargins,
            minimumWidth: this.options.compact ? 36 : 80,
        };
    }

    _timeScaleOptions(visible) {
        return {
            borderColor: CHART_CONFIG.borderColor,
            timeVisible: true,
            secondsVisible: false,
            visible,
            rightOffset: CHART_CONFIG.rightBarOffset,
        };
    }

    _compactPriceFormatter(price) {
        if (price >= 1000) return price.toFixed(0);
        if (price >= 100) return price.toFixed(1);
        return price.toFixed(2);
    }

    _compactVolumeFormatter(volume) {
        if (volume >= 1e9) return (volume / 1e9).toFixed(1) + 'B';
        if (volume >= 1e6) return (volume / 1e6).toFixed(0) + 'M';
        if (volume >= 1e3) return (volume / 1e3).toFixed(0) + 'K';
        return String(Math.round(volume));
    }

    /**
     * Keep price and volume panes sharing the same right-gutter width.
     */
    syncPriceScaleWidths() {
        if (!this.chart || !this.volumeChart) return;
        const priceW = this.chart.priceScale('right').width();
        const volumeW = this.volumeChart.priceScale('right').width();
        let target = Math.max(priceW, volumeW);
        if (target < 1) return;
        if (this.options.compact) {
            target = Math.max(target, 36);
        }
        const opts = { minimumWidth: target };
        this.chart.priceScale('right').applyOptions(opts);
        this.volumeChart.priceScale('right').applyOptions(opts);
    }

    _schedulePriceScaleSync() {
        requestAnimationFrame(() => {
            this._syncLayoutAfterScaleChange();
            requestAnimationFrame(() => this._syncLayoutAfterScaleChange());
        });
    }

    _syncLayoutAfterScaleChange() {
        const container = document.getElementById(this.containerId);
        const w = container?.clientWidth;
        this.syncPriceScaleWidths();
        if (w && this.chart) {
            this.chart.applyOptions({ width: w });
            if (this.volumeChart) this.volumeChart.applyOptions({ width: w });
        }
    }

    _refitTimeScales() {
        const ro = { rightOffset: CHART_CONFIG.rightBarOffset };
        if (this.chart) {
            this.chart.timeScale().fitContent();
            this.chart.timeScale().applyOptions(ro);
        }
        if (this.volumeChart) {
            this.volumeChart.timeScale().fitContent();
            this.volumeChart.timeScale().applyOptions(ro);
        }
    }
    
    /**
     * Floating identity watermark drawn over the top-left of the price pane.
     *
     * Absolutely positioned so it consumes no layout height — the top-left of a
     * price pane is nearly always empty, and pointer-events are disabled so it
     * never intercepts crosshair or pan/zoom interaction.
     */
    _createIdentityOverlay() {
        if (!this.priceContainer) return;

        // The panel header title would now duplicate the overlay, so drop it
        // and reclaim its row for the chart.
        const panelTitle = document.getElementById('stockChartTitle');
        if (panelTitle) panelTitle.style.display = 'none';

        const m = this.metricsData || {};
        const overlay = document.createElement('div');
        overlay.className = 'chart-identity-overlay';
        overlay.style.cssText = `
            position: absolute;
            top: ${this.options.compact ? 4 : 6}px;
            left: ${this.options.compact ? 8 : 10}px;
            z-index: 3;
            pointer-events: none;
            line-height: 1.15;
            font-family: 'Outfit', sans-serif;
        `;

        const tickerEl = document.createElement('div');
        tickerEl.textContent = this.ticker || '';
        tickerEl.style.cssText = `
            font-size: ${this.options.compact ? 30 : 42}px;
            font-weight: 800;
            font-family: 'JetBrains Mono', monospace;
            letter-spacing: 0.04em;
            color: rgba(230, 237, 243, 0.5);
        `;
        overlay.appendChild(tickerEl);

        if (m.company_name) {
            const coEl = document.createElement('div');
            coEl.textContent = m.company_name;
            coEl.style.cssText = `
                font-size: ${this.options.compact ? 13 : 15}px;
                font-weight: 600;
                color: rgba(139, 148, 158, 0.75);
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                max-width: ${this.options.compact ? 320 : 460}px;
                margin-top: 2px;
            `;
            overlay.appendChild(coEl);
        }

        const sectorIndustry = [m.sector, m.industry].filter(Boolean).join(' · ');
        if (sectorIndustry) {
            const siEl = document.createElement('div');
            siEl.textContent = sectorIndustry;
            siEl.style.cssText = `
                font-size: ${this.options.compact ? 12 : 13}px;
                font-weight: 500;
                color: rgba(139, 148, 158, 0.55);
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                max-width: ${this.options.compact ? 320 : 460}px;
            `;
            overlay.appendChild(siEl);
        }

        this.identityOverlay = overlay;
        this.priceContainer.appendChild(overlay);
    }

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
        this.priceContainer.style.flexShrink = '0';
        this.priceContainer.style.position = 'relative';
        container.appendChild(this.priceContainer);
        
        this.volumeContainer = document.createElement('div');
        this.volumeContainer.style.width = '100%';
        this.volumeContainer.style.height = `${volumeHeight}px`;
        this.volumeContainer.style.flexShrink = '0';
        container.appendChild(this.volumeContainer);

        this._createIdentityOverlay();

        // Create main price chart
        this.chart = LightweightCharts.createChart(this.priceContainer, {
            width: container.clientWidth,
            height: priceHeight,
            layout: this._layoutOptions(),
            ...(this.options.compact ? {
                localization: { priceFormatter: (price) => this._compactPriceFormatter(price) },
            } : {}),
            grid: {
                vertLines: { visible: false },
                horzLines: { visible: false },
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
            rightPriceScale: this._priceScaleOptions({ top: 0.05, bottom: 0.05 }),
            // Compact: dates on price pane (volume pane too short for labels)
            timeScale: this._timeScaleOptions(this.options.compact),
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
            priceLineVisible: false,
        });
        
        // Add moving average series (using server-provided DMA for 50/200)
        this.series.ema10 = this.chart.addLineSeries({
            color: CHART_CONFIG.ema10Color,
            lineWidth: 1,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
            visible: this.seriesVisibility.ema10,
        });
        
        this.series.ema20 = this.chart.addLineSeries({
            color: CHART_CONFIG.ema20Color,
            lineWidth: 1,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
            visible: this.seriesVisibility.ema20,
        });
        
        this.series.dma50 = this.chart.addLineSeries({
            color: CHART_CONFIG.dma50Color,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
            visible: this.seriesVisibility.dma50,
        });
        
        this.series.dma200 = this.chart.addLineSeries({
            color: CHART_CONFIG.dma200Color,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
            visible: this.seriesVisibility.dma200,
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
            this._schedulePriceScaleSync();
        };
        window.addEventListener('resize', this.resizeHandler);
    }
    
    _createLegend(container) {
        this.legendContainer = document.createElement('div');
        const legendGap = this.options.compact ? 8 : 12;
        const legendPad = this.options.compact ? '4px 6px' : '6px 8px';
        const legendFont = this.options.compact ? 10 : 11;
        this.legendContainer.style.cssText = `
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: ${legendGap}px;
            padding: ${legendPad};
            font-size: ${legendFont}px;
            font-family: 'JetBrains Mono', monospace;
            background: rgba(22, 27, 34, 0.8);
            border-radius: 4px;
            margin-bottom: 2px;
            flex-shrink: 0;
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

        // ── Prominent fundamentals strip ──────────────────────────────
        // Rendered on the same legend row as the MA toggles, but with a
        // larger, cleaner font so market cap / forward valuation / growth
        // are readable at a glance.
        const metrics = this.metricsData || {};
        const metricsRow = document.createElement('div');
        metricsRow.className = 'chart-metrics-strip';
        metricsRow.style.cssText = `
            display: flex;
            flex-wrap: wrap;
            align-items: baseline;
            gap: ${this.options.compact ? 14 : 22}px;
            margin-left: auto;
            padding-left: ${this.options.compact ? 8 : 12}px;
            border-left: 1px solid ${CHART_CONFIG.borderColor};
            font-family: 'Outfit', sans-serif;
        `;

        // ── Price cell (price + day-change in parentheses, two-color) ──
        const priceCell = document.createElement('div');
        priceCell.style.cssText = `display: flex; flex-direction: column; line-height: 1.1;`;
        const priceLabelEl = document.createElement('span');
        priceLabelEl.textContent = 'Price';
        priceLabelEl.style.cssText = `
            font-size: ${this.options.compact ? 9 : 10}px;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: ${CHART_CONFIG.textColor};
            margin-bottom: 2px;
        `;
        const priceValueRow = document.createElement('span');
        priceValueRow.style.cssText = `
            display: flex;
            align-items: baseline;
            gap: 4px;
            font-size: ${this.options.compact ? 15 : 18}px;
            font-weight: 600;
            font-family: 'JetBrains Mono', monospace;
            letter-spacing: 0.01em;
        `;
        const priceNumEl = document.createElement('span');
        priceNumEl.textContent = metrics.current_price != null
            ? '$' + metrics.current_price.toFixed(2)
            : '--';
        priceNumEl.style.color = 'var(--text-primary, #e6edf3)';
        const changeEl = document.createElement('span');
        const dr1 = metrics.dr_1;
        if (dr1 != null) {
            const sign = dr1 >= 0 ? '+' : '';
            changeEl.textContent = `(${sign}${dr1.toFixed(2)}%)`;
            changeEl.style.color = dr1 >= 0
                ? 'var(--accent-green, #3fb950)'
                : 'var(--accent-red, #f85149)';
        } else {
            changeEl.textContent = '(--)';
            changeEl.style.color = 'var(--text-muted, #6e7681)';
        }
        priceValueRow.appendChild(priceNumEl);
        priceValueRow.appendChild(changeEl);
        priceCell.appendChild(priceLabelEl);
        priceCell.appendChild(priceValueRow);
        metricsRow.appendChild(priceCell);

        const metricItems = [
            {
                label: 'Mkt Cap',
                value: formatMarketCapCompact(metrics.market_cap),
                color: marketCapColor(metrics.market_cap),
                highlightSuffix: true,
            },
            {
                label: 'PE (T / T+1)',
                value: formatForwardRatio(metrics.pe_t, 1) + ' / ' + formatForwardRatio(metrics.pe_t_plus_1, 1),
                color: 'var(--text-primary, #e6edf3)',
            },
            {
                label: 'P/S (T / T+1)',
                value: formatForwardRatio(metrics.ps_t, 1) + ' / ' + formatForwardRatio(metrics.ps_t_plus_1, 1),
                color: 'var(--text-primary, #e6edf3)',
            },
            {
                label: 'Rev Gr (T+1)',
                value: formatGrowthPct(metrics.rev_growth_t_plus_1),
                color: metrics.rev_growth_t_plus_1 == null
                    ? 'var(--text-muted, #6e7681)'
                    : (metrics.rev_growth_t_plus_1 >= 0
                        ? 'var(--accent-green, #3fb950)'
                        : 'var(--accent-red, #f85149)'),
            },
        ];

        metricItems.forEach(m => {
            const cell = document.createElement('div');
            cell.style.cssText = `
                display: flex;
                flex-direction: column;
                line-height: 1.1;
            `;
            const labelEl = document.createElement('span');
            labelEl.textContent = m.label;
            labelEl.style.cssText = `
                font-size: ${this.options.compact ? 9 : 10}px;
                font-weight: 600;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: ${CHART_CONFIG.textColor};
                margin-bottom: 2px;
            `;
            const valueEl = document.createElement('span');
            valueEl.style.cssText = `
                font-size: ${this.options.compact ? 15 : 18}px;
                font-weight: 600;
                font-family: 'JetBrains Mono', monospace;
                color: ${m.color};
                letter-spacing: 0.01em;
            `;
            // Add a touch of breathing room between the digits and the
            // magnitude letter (T/B/M/K) so it doesn't read as part of the
            // number, without changing size/weight/color.
            const suffixMatch = m.highlightSuffix ? m.value.match(/^(.*\d)([TBMK])$/) : null;
            if (suffixMatch) {
                const numSpan = document.createElement('span');
                numSpan.textContent = suffixMatch[1];
                const suffixSpan = document.createElement('span');
                suffixSpan.textContent = suffixMatch[2];
                suffixSpan.style.marginLeft = '2px';
                valueEl.appendChild(numSpan);
                valueEl.appendChild(suffixSpan);
            } else {
                valueEl.textContent = m.value;
            }
            cell.appendChild(labelEl);
            cell.appendChild(valueEl);
            metricsRow.appendChild(cell);
        });

        this.legendContainer.appendChild(metricsRow);

        container.appendChild(this.legendContainer);
    }
    
    _initVolumeChart(width, height) {
        this.volumeChart = LightweightCharts.createChart(this.volumeContainer, {
            width: width,
            height: height,
            layout: this._layoutOptions(),
            ...(this.options.compact ? {
                localization: { priceFormatter: (v) => this._compactVolumeFormatter(v) },
            } : {}),
            grid: {
                vertLines: { visible: false },
                horzLines: { visible: false },
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
            rightPriceScale: this._priceScaleOptions({ top: 0.1, bottom: 0.1 }),
            timeScale: this._timeScaleOptions(!this.options.compact),
            handleScroll: false,
            handleScale: false,
        });
        
        this.volumeSeries = this.volumeChart.addHistogramSeries({
            priceFormat: this.options.compact
                ? { type: 'custom', formatter: (v) => this._compactVolumeFormatter(v) }
                : { type: 'volume' },
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
                vertLines: { visible: false },
                horzLines: { visible: false },
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
                rightOffset: CHART_CONFIG.rightBarOffset,
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
        
        // Use server-provided EMA values (with fallback to client-side calculation)
        let ema10Data = filteredData
            .filter(d => d.ema_10 != null)
            .map(d => ({ time: d.time, value: d.ema_10 }));
        let ema20Data = filteredData
            .filter(d => d.ema_20 != null)
            .map(d => ({ time: d.time, value: d.ema_20 }));
        
        // Fallback to client-side calculation if server EMAs not available
        if (ema10Data.length === 0) {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - this.currentTimeframe);
            const cutoffStr = cutoffDate.toISOString().split('T')[0];
            ema10Data = calculateEMA(this.allData, 10).filter(d => d.time >= cutoffStr);
        }
        if (ema20Data.length === 0) {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - this.currentTimeframe);
            const cutoffStr = cutoffDate.toISOString().split('T')[0];
            ema20Data = calculateEMA(this.allData, 20).filter(d => d.time >= cutoffStr);
        }
        
        this.series.ema10.setData(ema10Data);
        this.series.ema20.setData(ema20Data);
        
        // Use server-provided DMA values
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
        
        // Volume spike and gap markers (fetched from API)
        if (this.options.showVolspikeMarkers) {
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

