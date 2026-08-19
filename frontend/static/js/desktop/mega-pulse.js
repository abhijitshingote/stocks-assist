(function () {
    'use strict';

    var MP = window.MegaPulse;
    var raw = [];
    var windowKey = 'dr_1';
    var bandKey = 'default';
    var weight = 'eq';
    var laneFilter = 'all';
    var sectorSort = 'abs';

    function bandMul() {
        for (var i = 0; i < MP.BANDS.length; i++) {
            if (MP.BANDS[i].key === bandKey) return MP.BANDS[i].mul;
        }
        return 1;
    }

    function setActive(rowId, value) {
        var row = document.getElementById(rowId);
        if (!row) return;
        row.querySelectorAll('.mp-chip').forEach(function (b) {
            b.classList.toggle('active', b.getAttribute('data-val') === value);
        });
    }

    function bindRow(rowId, setter) {
        var row = document.getElementById(rowId);
        if (!row) return;
        row.addEventListener('click', function (e) {
            var btn = e.target.closest('.mp-chip');
            if (!btn) return;
            setter(btn.getAttribute('data-val'));
            render();
        });
    }

    function meanCls(v) {
        if (v == null) return 'flat';
        if (v > 0.05) return 'up';
        if (v < -0.05) return 'down';
        return 'flat';
    }

    function laneHtml(label, bucket, tiles, thresh, force) {
        if (!force && laneFilter !== 'all' && laneFilter !== bucket) return '';
        var inner = tiles.length
            ? tiles.map(function (s) { return MP.tileHtml(s, '/stock/', thresh); }).join('')
            : '<span class="mp-empty">—</span>';
        return '<div class="mp-lane ' + bucket + '">' +
            '<div class="mp-lane-label">' + label + ' · ' + tiles.length + '</div>' +
            '<div class="mp-tiles">' + inner + '</div></div>';
    }

    function render() {
        var state = MP.build(raw, {
            windowKey: windowKey,
            bandMul: bandMul(),
            weight: weight,
            laneFilter: laneFilter,
            sectorSort: sectorSort,
        });
        var t = state.totals;

        document.getElementById('mpCount').textContent = String(t.n);
        document.getElementById('mpUp').textContent = String(t.nUp);
        document.getElementById('mpFlat').textContent = String(t.nFlat);
        document.getElementById('mpDown').textContent = String(t.nDown);
        var meanEl = document.getElementById('mpMean');
        meanEl.textContent = MP.fmtRet(t.mean, 2);
        meanEl.className = meanCls(t.mean);
        document.getElementById('mpThresh').textContent =
            '|' + t.windowLabel + '| ≤ ' + t.thresh.toFixed(2) + '% → nonchalant';

        var tot = t.n || 1;
        var bar = document.getElementById('mpBreadth');
        bar.innerHTML =
            '<span class="down" style="flex:' + t.nDown + '"></span>' +
            '<span class="flat" style="flex:' + t.nFlat + '"></span>' +
            '<span class="up" style="flex:' + t.nUp + '"></span>';
        bar.title = '▼ ' + t.nDown + '  — ' + t.nFlat + '  ▲ ' + t.nUp +
            '  (' + tot + ')';

        setActive('mpWin', windowKey);
        setActive('mpBand', bandKey);
        setActive('mpLane', laneFilter);
        setActive('mpSort', sectorSort);
        setActive('mpWeight', weight);

        var board = document.getElementById('mpBoard');
        if (!state.sectors.length) {
            board.innerHTML = '<div class="mp-status">No mega-cap names in this filter.</div>';
            return;
        }

        var laneClass = 'mp-lanes' + (laneFilter !== 'all' ? ' filter-' + laneFilter : '');
        board.innerHTML = state.sectors.map(function (sec) {
            var lanes = laneHtml('Down', 'down', sec.down, t.thresh, false) +
                laneHtml('Nonchalant', 'flat', sec.flat, t.thresh, false) +
                laneHtml('Up', 'up', sec.up, t.thresh, false);
            return '<section class="mp-sector">' +
                '<div class="mp-sector-head">' +
                    '<span class="mp-sector-name">' + MP.esc(sec.name) + '</span>' +
                    '<span class="mp-sector-n">' + sec.n + '</span>' +
                    MP.barHtml(sec.nDown, sec.nFlat, sec.nUp) +
                    '<span class="mp-sector-counts">' +
                        '<span class="down">▼' + sec.nDown + '</span>' +
                        '<span class="flat">—' + sec.nFlat + '</span>' +
                        '<span class="up">▲' + sec.nUp + '</span>' +
                    '</span>' +
                    '<span class="mp-sector-mean ' + meanCls(sec.mean) + '">' +
                        MP.fmtRet(sec.mean, 2) + '</span>' +
                '</div>' +
                '<div class="' + laneClass + '">' + lanes + '</div>' +
            '</section>';
        }).join('');
    }

    async function load() {
        var board = document.getElementById('mpBoard');
        try {
            var resp = await fetch('/api/frontend/all-stocks');
            var data = await resp.json();
            raw = Array.isArray(data) ? data : [];
        } catch (e) {
            board.innerHTML = '<div class="mp-status">Failed to load /api/frontend/all-stocks</div>';
            return;
        }
        render();
    }

    document.addEventListener('DOMContentLoaded', function () {
        bindRow('mpWin', function (v) { windowKey = v; });
        bindRow('mpBand', function (v) { bandKey = v; });
        bindRow('mpLane', function (v) { laneFilter = v; });
        bindRow('mpSort', function (v) { sectorSort = v; });
        bindRow('mpWeight', function (v) { weight = v; });
        load();
    });
})();
