(function () {
  'use strict';

  function isMobileRoute() {
    const p = window.location.pathname;
    return p === '/m' || p.startsWith('/m/');
  }

  function stockUrl(ticker) {
    const t = String(ticker || '').trim().toUpperCase();
    if (!t) return null;
    return (isMobileRoute() ? '/m/stock/' : '/stock/') + encodeURIComponent(t);
  }

  function setupTickerSearch(inputEl) {
    if (!inputEl || inputEl.dataset.searchBound === '1') return;
    inputEl.dataset.searchBound = '1';

    inputEl.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter') return;
      e.preventDefault();
      const url = stockUrl(inputEl.value);
      if (url) window.location.href = url;
    });

    // Uppercase on input for nicer feedback
    inputEl.addEventListener('input', function () {
      const start = inputEl.selectionStart;
      const end = inputEl.selectionEnd;
      const upper = inputEl.value.toUpperCase();
      if (upper !== inputEl.value) {
        inputEl.value = upper;
        try { inputEl.setSelectionRange(start, end); } catch (e) { /* noop */ }
      }
    });
  }

  window.MobileSearch = { setupTickerSearch, stockUrl, isMobileRoute };

  function init() {
    const input = document.getElementById('tickerSearchInput');
    if (input) setupTickerSearch(input);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
