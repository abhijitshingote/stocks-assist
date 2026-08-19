(function () {
  'use strict';

  var MP = window.MegaPulse;
  var raw = [];
  var windowKey = 'dr_1';
  var bandKey = 'default';
  var laneFilter = 'all';

  function bandMul() {
    for (var i = 0; i < MP.BANDS.length; i++) {
      if (MP.BANDS[i].key === bandKey) return MP.BANDS[i].mul;
    }
    return 1;
  }

  function setActive(rowId, value) {
    var row = document.getElementById(rowId);
    if (!row) return;
    row.querySelectorAll('.home-sort-btn').forEach(function (b) {
      b.classList.toggle('active', b.getAttribute('data-val') === value);
    });
  }

  function bindRow(rowId, setter) {
    var row = document.getElementById(rowId);
    if (!row) return;
    row.addEventListener('click', function (e) {
      var btn = e.target.closest('.home-sort-btn');
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

  function laneHtml(label, bucket, tiles, thresh) {
    if (laneFilter !== 'all' && laneFilter !== bucket) return '';
    var inner = tiles.length
      ? tiles.map(function (s) { return MP.tileHtml(s, '/m/stock/', thresh); }).join('')
      : '<span class="mp-empty">—</span>';
    return '<div class="mp-lane ' + bucket + '">' +
      '<div class="mp-lane-label">' + label + ' · ' + tiles.length + '</div>' +
      '<div class="mp-tiles">' + inner + '</div></div>';
  }

  function render() {
    var state = MP.build(raw, {
      windowKey: windowKey,
      bandMul: bandMul(),
      weight: 'eq',
      laneFilter: laneFilter,
      sectorSort: 'abs',
    });
    var t = state.totals;
    var meta = document.getElementById('mpMeta');
    meta.textContent = t.n + ' mega · ▼' + t.nDown + ' —' + t.nFlat +
      ' ▲' + t.nUp + ' · ' + MP.fmtRet(t.mean, 2) +
      ' · |' + t.windowLabel + '|≤' + t.thresh.toFixed(2) + '%';

    document.getElementById('mpBreadth').innerHTML =
      '<span class="down" style="flex:' + t.nDown + '"></span>' +
      '<span class="flat" style="flex:' + t.nFlat + '"></span>' +
      '<span class="up" style="flex:' + t.nUp + '"></span>';

    setActive('mpWin', windowKey);
    setActive('mpBand', bandKey);
    setActive('mpLane', laneFilter);

    var board = document.getElementById('mpBoard');
    if (!state.sectors.length) {
      board.innerHTML = '<div class="md-empty">No mega-cap names in this filter.</div>';
      return;
    }

    board.innerHTML = state.sectors.map(function (sec) {
      return '<section class="mp-sector">' +
        '<div class="mp-sector-head">' +
          '<span class="mp-sector-name">' + MP.esc(sec.name) + '</span>' +
          '<span class="mp-sector-n">' + sec.n + '</span>' +
          MP.barHtml(sec.nDown, sec.nFlat, sec.nUp) +
          '<span class="mp-sector-mean ' + meanCls(sec.mean) + '">' +
            MP.fmtRet(sec.mean, 2) + '</span>' +
        '</div>' +
        '<div class="mp-lanes filter-stack">' +
          laneHtml('Down', 'down', sec.down, t.thresh) +
          laneHtml('Nonchalant', 'flat', sec.flat, t.thresh) +
          laneHtml('Up', 'up', sec.up, t.thresh) +
        '</div></section>';
    }).join('');
  }

  async function load() {
    var board = document.getElementById('mpBoard');
    try {
      var resp = await fetch('/api/frontend/all-stocks');
      var data = await resp.json();
      raw = Array.isArray(data) ? data : [];
    } catch (e) {
      board.innerHTML = '<div class="md-empty">Failed to load all-stocks</div>';
      return;
    }
    render();
  }

  document.addEventListener('DOMContentLoaded', function () {
    bindRow('mpWin', function (v) { windowKey = v; });
    bindRow('mpBand', function (v) { bandKey = v; });
    bindRow('mpLane', function (v) { laneFilter = v; });
    load();
  });
})();
