(function (global) {
    'use strict';

    var MEGA_MIN = 100e9;

    var WINDOWS = [
        { key: 'dr_1',  label: '1D',  thresh: 1.0 },
        { key: 'dr_5',  label: '5D',  thresh: 2.5 },
        { key: 'dr_20', label: '20D', thresh: 6.0 },
    ];

    var BANDS = [
        { key: 'tight',   label: 'Tight',   mul: 0.75 },
        { key: 'default', label: 'Default', mul: 1 },
        { key: 'wide',    label: 'Wide',    mul: 1.5 },
    ];

    function esc(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function fmtRet(v, digits) {
        if (v == null || !isFinite(v)) return '—';
        var d = digits == null ? 1 : digits;
        return (v > 0 ? '+' : '') + Number(v).toFixed(d) + '%';
    }

    function fmtMktCap(v) {
        if (!v) return '—';
        if (v >= 1e12) return '$' + (v / 1e12).toFixed(1) + 'T';
        if (v >= 1e9)  return '$' + (v / 1e9).toFixed(0) + 'B';
        if (v >= 1e6)  return '$' + (v / 1e6).toFixed(0) + 'M';
        return '$' + v.toLocaleString();
    }

    function sizeClass(mcap) {
        if (mcap >= 1e12) return 'xl';
        if (mcap >= 400e9) return 'lg';
        return 'md';
    }

    function retOf(s, windowKey) {
        var v = s[windowKey];
        if (v == null || !isFinite(v)) return null;
        return v;
    }

    function classify(ret, thresh) {
        if (ret == null) return null;
        if (ret > thresh) return 'up';
        if (ret < -thresh) return 'down';
        return 'flat';
    }

    function filterMega(stocks) {
        return (stocks || []).filter(function (s) {
            return (s.market_cap || 0) >= MEGA_MIN;
        });
    }

    function mean(arr, weightFn) {
        var num = 0, den = 0;
        for (var i = 0; i < arr.length; i++) {
            var r = arr[i].ret;
            if (r == null) continue;
            var w = weightFn ? weightFn(arr[i]) : 1;
            if (!w) continue;
            num += r * w;
            den += w;
        }
        return den ? num / den : null;
    }

    function modalBucket(nUp, nFlat, nDown) {
        var best = 'flat', n = nFlat;
        if (nUp > n) { best = 'up'; n = nUp; }
        if (nDown > n) best = 'down';
        return best;
    }

    function sortTiles(list, bucket) {
        return list.slice().sort(function (a, b) {
            if (bucket === 'up') return (b.ret || -Infinity) - (a.ret || -Infinity);
            if (bucket === 'down') return (a.ret || Infinity) - (b.ret || Infinity);
            return (b.market_cap || 0) - (a.market_cap || 0);
        });
    }

    function windowByKey(key) {
        for (var i = 0; i < WINDOWS.length; i++) {
            if (WINDOWS[i].key === key) return WINDOWS[i];
        }
        return WINDOWS[0];
    }

    function build(stocks, opts) {
        opts = opts || {};
        var windowKey = opts.windowKey || 'dr_1';
        var bandMul = opts.bandMul == null ? 1 : opts.bandMul;
        var weight = opts.weight || 'eq';
        var laneFilter = opts.laneFilter || 'all';
        var sectorSort = opts.sectorSort || 'abs';

        var win = windowByKey(windowKey);
        var thresh = win.thresh * bandMul;

        var mega = filterMega(stocks).map(function (s) {
            var ret = retOf(s, windowKey);
            return {
                ticker: s.ticker,
                company_name: s.company_name,
                sector: s.sector || 'Unknown',
                industry: s.industry || '',
                market_cap: s.market_cap || 0,
                ti65: s.ti65,
                at_52w_high: !!s.at_52w_high,
                ret: ret,
                bucket: classify(ret, thresh),
                size: sizeClass(s.market_cap || 0),
            };
        }).filter(function (s) { return s.bucket != null; });

        var bySector = {};
        mega.forEach(function (s) {
            if (!bySector[s.sector]) bySector[s.sector] = [];
            bySector[s.sector].push(s);
        });

        function wfn(s) { return weight === 'mcap' ? (s.market_cap || 0) : 1; }

        var sectors = Object.keys(bySector).map(function (name) {
            var names = bySector[name];
            var up = names.filter(function (s) { return s.bucket === 'up'; });
            var down = names.filter(function (s) { return s.bucket === 'down'; });
            var flat = names.filter(function (s) { return s.bucket === 'flat'; });
            var modal = modalBucket(up.length, flat.length, down.length);
            names.forEach(function (s) {
                s.diverge = (s.bucket === 'up' || s.bucket === 'down') && s.bucket !== modal;
            });
            return {
                name: name,
                n: names.length,
                nUp: up.length,
                nDown: down.length,
                nFlat: flat.length,
                mean: mean(names, wfn),
                modal: modal,
                up: sortTiles(up, 'up'),
                down: sortTiles(down, 'down'),
                flat: sortTiles(flat, 'flat'),
            };
        });

        sectors.sort(function (a, b) {
            if (sectorSort === 'name') return a.name.localeCompare(b.name);
            if (sectorSort === 'lopsided') return (b.nUp - b.nDown) - (a.nUp - a.nDown);
            if (sectorSort === 'mean') {
                if (a.mean == null) return 1;
                if (b.mean == null) return -1;
                return b.mean - a.mean;
            }
            var aa = a.mean == null ? -1 : Math.abs(a.mean);
            var ba = b.mean == null ? -1 : Math.abs(b.mean);
            return ba - aa;
        });

        if (laneFilter !== 'all') {
            sectors = sectors.filter(function (sec) {
                if (laneFilter === 'up') return sec.nUp > 0;
                if (laneFilter === 'down') return sec.nDown > 0;
                return sec.nFlat > 0;
            });
        }

        return {
            mega: mega,
            sectors: sectors,
            totals: {
                n: mega.length,
                nUp: mega.filter(function (s) { return s.bucket === 'up'; }).length,
                nDown: mega.filter(function (s) { return s.bucket === 'down'; }).length,
                nFlat: mega.filter(function (s) { return s.bucket === 'flat'; }).length,
                mean: mean(mega, wfn),
                thresh: thresh,
                windowKey: windowKey,
                windowLabel: win.label,
            },
            laneFilter: laneFilter,
        };
    }

    function mag(s, thresh) {
        if (s.ret == null || !thresh) return 0;
        if (s.bucket === 'flat') return Math.min(1, Math.abs(s.ret) / thresh);
        return Math.min(1, Math.abs(s.ret) / (3 * thresh));
    }

    function tileHtml(s, hrefPrefix, thresh) {
        var href = hrefPrefix + encodeURIComponent(s.ticker);
        var title = [s.ticker, s.company_name, s.industry, fmtMktCap(s.market_cap)]
            .filter(Boolean).join(' · ');
        var cls = 'mp-tile ' + s.bucket + ' sz-' + s.size;
        if (s.diverge) cls += ' diverge';
        if (s.at_52w_high) cls += ' hi52';
        var m = mag(s, thresh);
        return '<a class="' + cls + '" href="' + href + '" title="' + esc(title) +
            '" style="--mag:' + m.toFixed(2) + '">' +
            '<span class="mp-tk">' + esc(s.ticker) +
            (s.at_52w_high ? '<i class="mp-pip"></i>' : '') +
            (s.diverge ? '<i class="mp-neq">≠</i>' : '') + '</span>' +
            '<span class="mp-ret">' + fmtRet(s.ret, 1) + '</span>' +
            '<span class="mp-cap">' + fmtMktCap(s.market_cap) + '</span>' +
            '</a>';
    }

    function barHtml(nDown, nFlat, nUp) {
        var tot = nDown + nFlat + nUp;
        if (!tot) return '<div class="mp-bar"></div>';
        return '<div class="mp-bar" title="▼ ' + nDown + '  — ' + nFlat + '  ▲ ' + nUp + '">' +
            '<span class="down" style="flex:' + nDown + '"></span>' +
            '<span class="flat" style="flex:' + nFlat + '"></span>' +
            '<span class="up" style="flex:' + nUp + '"></span>' +
            '</div>';
    }

    global.MegaPulse = {
        MEGA_MIN: MEGA_MIN,
        WINDOWS: WINDOWS,
        BANDS: BANDS,
        esc: esc,
        fmtRet: fmtRet,
        fmtMktCap: fmtMktCap,
        build: build,
        tileHtml: tileHtml,
        barHtml: barHtml,
    };
})(window);
