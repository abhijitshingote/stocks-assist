/* Fast RS.
   Clone of rs-screener Fast mode: frozen weights 10/25/50/50/50 on raw rs_*.
   Sort toggle: mcap-adjusted RS score (TI65-style) vs monthly setup/readiness.
   No sliders, no Slow RS, no SPY subchart.
*/
(function () {
    const SORT_KEY = 'fastRsSort';
    const TIMEFRAMES = ['2d', '5d', '10d', '20d', '60d'];
    const MA_LABELS = { ema_10: 'E10', ema_20: 'E20', dma_50: 'D50', dma_200: 'D200' };
    const SETUP_W = { prox: 0.35, tight: 0.25, hold: 0.20, trend: 0.20 };
    const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

    function setupParts(m) {
        if (!m) return { prox: 0, tight: 0, hold: 0, trend: 0, total: 0 };
        const distAtr = m.ma_dist_atr != null ? m.ma_dist_atr : Math.abs(m.ma_dist_pct) / 2;
        const prox = clamp(1 - distAtr / 1.5, 0, 1);
        const tight = clamp((3.5 - m.range10_atr) / 2, 0, 1);
        const pos = m.pos_in_range10 == null ? 0.5 : m.pos_in_range10;
        const hold = clamp((pos - 0.4) / 0.5, 0, 1);
        const trend = m.above_all_ma ? 1 : m.above_ema20 ? 0.6 : 0;
        return {
            prox, tight, hold, trend,
            total: SETUP_W.prox * prox + SETUP_W.tight * tight +
                SETUP_W.hold * hold + SETUP_W.trend * trend,
        };
    }

    let sortMode = 'adj';
    try {
        const saved = localStorage.getItem(SORT_KEY);
        if (saved === 'ready' || saved === 'adj') sortMode = saved;
    } catch (e) {}

    let screenerApi = null;
    let setupMap = null;
    let setupResortPending = false;
    let rsiChart = null, rsiSeries = null;
    let tfDays = 365;
    let extraResizeHandler = null;
    let metricsFetchToken = 0;

    fetch('/api/frontend/strong-stocks-setup')
        .then(r => r.json())
        .then(m => {
            if (!m || m.error) return;
            setupMap = m;
            if (screenerApi) screenerApi.resortWithFn();
            else setupResortPending = true;
        })
        .catch(() => {});

    function readiness(s) {
        return setupParts(setupMap ? setupMap[s.ticker] : null);
    }

    function sortKey(s) {
        if (sortMode === 'ready') return readiness(s).total;
        const v = s.adjusted_rs_score;
        return v == null ? -Infinity : v;
    }

    function blueDot(s) {
        if (!s.rs_line_new_high) return '';
        return '<span class="rs-blue-dot" title="RS line at 52-week high (MarketSmith blue dot)"></span>';
    }

    function setupChipHtml(s) {
        if (!setupMap) return '';
        const m = setupMap[s.ticker];
        if (!m || !m.nearest_ma) return '';
        const p = setupParts(m);
        const dist = (m.ma_dist_pct >= 0 ? '+' : '') + m.ma_dist_pct.toFixed(1) + '%';
        const tip = `${m.nearest_ma} ${dist} (${m.ma_dist_atr} ATR), ` +
            `10-bar range ${m.range10_atr.toFixed(1)} ATR, ` +
            `close at ${((m.pos_in_range10 == null ? 0.5 : m.pos_in_range10) * 100).toFixed(0)}% of range` +
            (m.above_all_ma ? ', above all 4 MAs' : m.above_ema20 ? ', above ema_20' : ', below ema_20');
        const cls = p.total >= 0.7 ? ' tight' : '';
        return `<span class="vsg-setup${cls}" title="${tip}">` +
            `${MA_LABELS[m.nearest_ma] || m.nearest_ma} ${dist}</span>`;
    }

    function scoreChipHtml(s) {
        if (sortMode === 'ready') {
            const p = readiness(s);
            const tip = `setup ${p.total.toFixed(2)} = ` +
                `prox ${p.prox.toFixed(2)} tight ${p.tight.toFixed(2)} ` +
                `hold ${p.hold.toFixed(2)} trend ${p.trend.toFixed(2)}`;
            return `<span class="vsg-score" title="${tip}">${p.total.toFixed(2)}</span>`;
        }
        const v = s.adjusted_rs_score;
        if (v == null) return '';
        return `<span class="vsg-score" title="adjusted RS ${v.toFixed(2)} = ` +
            `rs_score / (13.66 × (clip(mcap,$500M,$100B)/$100B)^-0.192)">${v.toFixed(2)}</span>`;
    }

    function extraHtml(s) {
        return `<span class="vsg-right">` +
            setupChipHtml(s) +
            scoreChipHtml(s) +
        `</span>`;
    }

    function listValueFn(s) {
        const v = s.rs_score;
        return {
            text: v != null ? (v >= 0 ? '+' : '') + v.toFixed(1) + '%' : '—',
            cls: v != null ? (v >= 0 ? 'positive' : 'negative') : 'muted',
        };
    }

    function miniChartOpts(container, height, showTimeAxis) {
        return {
            width: container.clientWidth,
            height: height,
            layout: {
                background: { type: 'solid', color: 'transparent' },
                textColor: CHART_CONFIG.textColor,
                fontFamily: "'JetBrains Mono', monospace",
            },
            grid: { vertLines: { visible: false }, horzLines: { visible: false } },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: { width: 1, color: CHART_CONFIG.crosshairColor, style: LightweightCharts.LineStyle.Dashed },
                horzLine: { width: 1, color: CHART_CONFIG.crosshairColor, style: LightweightCharts.LineStyle.Dashed },
            },
            rightPriceScale: { borderColor: CHART_CONFIG.borderColor, scaleMargins: { top: 0.1, bottom: 0.1 } },
            timeScale: { borderColor: CHART_CONFIG.borderColor, visible: !!showTimeAxis },
            handleScroll: false,
            handleScale: false,
        };
    }

    function getCutoffStr(days) {
        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - days);
        return cutoff.toISOString().split('T')[0];
    }

    function updateRsiChartData(stockChart, days) {
        if (!rsiSeries || !stockChart || !stockChart.allData) return;
        const cutoffStr = getCutoffStr(days);
        let lastVal = 0;
        const rsiData = stockChart.allData
            .filter(d => d.time >= cutoffStr)
            .map(d => {
                if (d.rsi_mktcap != null) lastVal = d.rsi_mktcap;
                return { time: d.time, value: lastVal };
            });
        rsiSeries.setData(rsiData);
    }

    function setAllTimeframes(days, stockChart) {
        tfDays = days;
        updateRsiChartData(stockChart, days);
        if (stockChart) stockChart.setTimeframe(days);
        if (rsiChart) rsiChart.timeScale().fitContent();
    }

    function syncAllCharts(stockChart) {
        if (!stockChart || !stockChart.chart) return;
        const mainChart = stockChart.chart;
        const volChart = stockChart.volumeChart;
        const allExtras = [];
        if (rsiChart && rsiSeries) allExtras.push({ chart: rsiChart, series: rsiSeries });

        mainChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (!range) return;
            allExtras.forEach(e => { try { e.chart.timeScale().setVisibleLogicalRange(range); } catch (_) {} });
        });
        if (volChart) {
            volChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
                if (!range) return;
                allExtras.forEach(e => { try { e.chart.timeScale().setVisibleLogicalRange(range); } catch (_) {} });
            });
        }

        const allCharts = [{ chart: mainChart, series: stockChart.series.candlestick }];
        if (volChart && stockChart.volumeSeries) allCharts.push({ chart: volChart, series: stockChart.volumeSeries });
        allExtras.forEach(e => allCharts.push(e));
        allCharts.forEach(source => {
            source.chart.subscribeCrosshairMove(param => {
                if (!param.time) return;
                allCharts.forEach(target => {
                    if (target.chart !== source.chart) {
                        try { target.chart.setCrosshairPosition(0, param.time, target.series); } catch (_) {}
                    }
                });
            });
        });
    }

    function destroyRsiChart() { if (rsiChart) { rsiChart.remove(); rsiChart = null; rsiSeries = null; } }

    function setupExtraResize() {
        if (extraResizeHandler) window.removeEventListener('resize', extraResizeHandler);
        extraResizeHandler = () => {
            if (rsiChart) {
                const c = document.getElementById('rsiSubChart');
                if (c) rsiChart.applyOptions({ width: c.clientWidth });
            }
        };
        window.addEventListener('resize', extraResizeHandler);
    }

    function buildRsMetricsHtml(rsStock, helpers) {
        let html = '';
        let items = '';
        TIMEFRAMES.forEach(tf => {
            const raw = rsStock[`rs_${tf}`];
            const rank = rsStock[`rs_${tf}_rank`];
            const v = raw != null ? (raw >= 0 ? '+' : '') + raw.toFixed(1) + '%' : '—';
            items += helpers.msItem(tf.toUpperCase(), v, helpers.retCls(raw), rank != null ? 'P' + rank : '');
        });
        const score = rsStock.rs_score;
        const adj = rsStock.adjusted_rs_score;
        items += helpers.msItem('Score', score != null ? (score >= 0 ? '+' : '') + score.toFixed(1) + '%' : '—',
            helpers.retCls(score));
        items += helpers.msItem('Adj', adj != null ? adj.toFixed(2) : '—');
        html += `<div class="ms-section"><span class="ms-section-title">RS</span><div class="ms-section-row">${items}</div></div>`;

        items = '';
        const rr = rsStock.rs_rating;
        const rrCls = rr == null ? '' : rr >= 80 ? 'ms-positive' : rr < 40 ? 'ms-negative' : '';
        items += helpers.msItem('Rating', rr != null ? rr : '—', rrCls);
        const vspy = rsStock.rs_vs_spy;
        items += helpers.msItem('vs SPX', vspy != null ? vspy.toFixed(0) : '—',
            vspy == null ? '' : vspy >= 100 ? 'ms-positive' : 'ms-negative');
        items += helpers.msItem('RS NH', rsStock.rs_line_new_high ? 'Yes' : 'No',
            rsStock.rs_line_new_high ? 'ms-positive' : '');
        html += `<div class="ms-section"><span class="ms-section-title">IBD</span><div class="ms-section-row">${items}</div></div>`;
        return html;
    }

    function buildFullMetricsHtml(rsStock, s, helpers) {
        function section(title, items) {
            return `<div class="ms-section"><span class="ms-section-title">${title}</span><div class="ms-section-row">${items}</div></div>`;
        }
        let html = buildRsMetricsHtml(rsStock, helpers);
        let items = '';

        items += helpers.msItem('Price', s.current_price ? '$' + s.current_price.toFixed(2) : '—');
        items += helpers.msItem('MCap', helpers.fmtMktCap(s.market_cap));
        items += helpers.msItem('Vol', helpers.fmtVol(s.volume));
        items += helpers.msItem('$Vol', s.dollar_volume ? helpers.fmtMktCap(s.dollar_volume) : '—');
        html += section('Price & Market', items);

        items = '';
        [['1D', 'dr_1'], ['5D', 'dr_5'], ['20D', 'dr_20']].forEach(([l, k]) => {
            items += helpers.msItem(l, helpers.fmtRet(s[k]), helpers.retCls(s[k]) + ' ms-val-lg');
        });
        [['60D', 'dr_60'], ['120D', 'dr_120']].forEach(([l, k]) => {
            items += helpers.msItem(l, helpers.fmtRet(s[k]), helpers.retCls(s[k]));
        });
        html += section('Returns', items);

        items = '';
        [['T-1', 'rev_growth_t_minus_1'], ['T', 'rev_growth_t'], ['T+1', 'rev_growth_t_plus_1'], ['T+2', 'rev_growth_t_plus_2']].forEach(([l, k]) => {
            items += helpers.msItem(l, helpers.fmtRet(s[k]), helpers.retCls(s[k]));
        });
        html += section('Revenue Growth', items);

        items = '';
        [['T-1', 'eps_growth_t_minus_1'], ['T', 'eps_growth_t'], ['T+1', 'eps_growth_t_plus_1'], ['T+2', 'eps_growth_t_plus_2']].forEach(([l, k]) => {
            items += helpers.msItem(l, helpers.fmtRet(s[k]), helpers.retCls(s[k]));
        });
        html += section('EPS Growth', items);

        items = '';
        [['T-1', 'ps_t_minus_1'], ['T', 'ps_t'], ['T+1', 'ps_t_plus_1'], ['T+2', 'ps_t_plus_2']].forEach(([l, k]) => {
            items += helpers.msItem(l, helpers.fmtVal(s[k], 1));
        });
        html += section('P/S Ratio', items);

        items = '';
        [['T-1', 'pe_t_minus_1'], ['T', 'pe_t'], ['T+1', 'pe_t_plus_1'], ['T+2', 'pe_t_plus_2']].forEach(([l, k]) => {
            items += helpers.msItem(l, helpers.fmtVal(s[k], 0));
        });
        html += section('P/E Ratio', items);

        items = '';
        items += helpers.msItem('RSI', s.rsi_mktcap || '—', s.rsi_mktcap >= 70 ? 'ms-positive' : s.rsi_mktcap <= 30 ? 'ms-negative' : '');
        items += helpers.msItem('ATR%', s.atr20 ? s.atr20.toFixed(1) + '%' : '—');
        items += helpers.msItem('V/Avg', s.vol_vs_10d_avg ? s.vol_vs_10d_avg.toFixed(1) + 'x' : '—');
        html += section('Technical', items);

        items = '';
        items += helpers.msItem('Float', helpers.fmtVol(s.float_shares));
        items += helpers.msItem('Free%', s.free_float ? s.free_float.toFixed(1) + '%' : '—');
        items += helpers.msItem('Short%', s.short_float ? s.short_float.toFixed(1) + '%' : '—');
        items += helpers.msItem('S.Ratio', s.short_ratio ? s.short_ratio.toFixed(1) : '—');
        html += section('Float & Short', items);

        return html;
    }

    document.querySelectorAll('.recency-btn[data-sort]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.sort === sortMode);
    });

    DesktopScreener.init({
        endpoint: 'fast-rs',
        accentCss: 'var(--accent-green)',
        label: 'RS Score',

        sortFn: (stocks) => stocks.sort((a, b) => {
            const d = sortKey(b) - sortKey(a);
            if (d) return d;
            return (b.market_cap || 0) - (a.market_cap || 0) || a.ticker.localeCompare(b.ticker);
        }),

        listPrefixFn: blueDot,
        listValueFn: listValueFn,
        listExtraFn: extraHtml,

        updateMetricsFn: (rsStock, container, helpers) => {
            container.innerHTML = buildRsMetricsHtml(rsStock, helpers) +
                '<div class="ms-section"><span class="ms-section-title">…</span><div class="ms-section-row">' +
                helpers.msItem('Loading', 'full metrics…') +
                '</div></div>';
            const token = ++metricsFetchToken;
            fetch('/api/frontend/stock/' + encodeURIComponent(rsStock.ticker))
                .then(r => r.json())
                .then(s => {
                    if (token !== metricsFetchToken) return;
                    if (!s || s.error) return;
                    container.innerHTML = buildFullMetricsHtml(rsStock, s, helpers);
                })
                .catch(() => {});
        },

        onChartLoaded: async (ticker, stockChart) => {
            destroyRsiChart();
            const rsiC = document.getElementById('rsiSubChart');
            rsiC.innerHTML = '';

            rsiChart = LightweightCharts.createChart(rsiC, miniChartOpts(rsiC, 70, false));
            rsiSeries = rsiChart.addAreaSeries({
                lineColor: CHART_CONFIG.rsiColor,
                topColor: 'rgba(0, 188, 212, 0.4)',
                bottomColor: 'rgba(0, 188, 212, 0.05)',
                lineWidth: 2,
                priceLineVisible: false,
                lastValueVisible: true,
                crosshairMarkerVisible: true,
            });

            setupExtraResize();
            syncAllCharts(stockChart);
            setAllTimeframes(tfDays, stockChart);
        },

        onTimeframeChange: (days, stockChart) => { setAllTimeframes(days, stockChart); },

        onListRendered: ({ visible }) => {
            document.getElementById('totalStocks').textContent = visible.length;
        },

        onReady: (api) => {
            screenerApi = api;
            if (setupResortPending) { setupResortPending = false; api.resortWithFn(); }

            document.querySelectorAll('.recency-btn[data-sort]').forEach(btn => {
                btn.addEventListener('click', () => {
                    sortMode = btn.dataset.sort;
                    try { localStorage.setItem(SORT_KEY, sortMode); } catch (e) {}
                    document.querySelectorAll('.recency-btn[data-sort]').forEach(b => {
                        b.classList.toggle('active', b === btn);
                    });
                    api.resortWithFn();
                });
            });

            document.getElementById('rsiCollapseBtn')?.addEventListener('click', () => {
                const panel = document.getElementById('rsiPanel');
                const icon = document.getElementById('rsiCollapseIcon');
                panel.classList.toggle('collapsed');
                const isCollapsed = panel.classList.contains('collapsed');
                icon.textContent = isCollapsed ? '▶' : '▼';
                requestAnimationFrame(() => {
                    window.dispatchEvent(new Event('resize'));
                    if (!isCollapsed && rsiChart) {
                        const c = document.getElementById('rsiSubChart');
                        if (c) rsiChart.applyOptions({ width: c.clientWidth });
                        rsiChart.timeScale().fitContent();
                    }
                });
            });
        },
    });
})();
