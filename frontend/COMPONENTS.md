# Frontend Component Catalog

> Living document — when the agent asks "which component do you want to change?", name the **Component Name** below and it will know exactly which file(s) to edit.

---

## Viewports

| Tier    | Width       | Behavior |
|---------|-------------|----------|
| Phone   | ≤ 640px     | Dedicated mobile site at `/m/*`; separate Jinja templates |
| Tablet  | 641–1024px  | Desktop layout with tighter spacing; responsive overrides in `responsive.css` |
| Desktop | ≥ 1025px    | Full layout; content max-width capped at ~1400px and centered |

---

## Global Shell Components (every desktop page)

### Nav Bar
| | |
|---|---|
| **File** | `templates/base.html` |
| **CSS** | Inline `<style>` in `base.html` (`.nav-*` selectors) |
| **Responsive** | `static/css/responsive.css` (hamburger at ≤640px, hide toggle) |
| **Used by** | All desktop pages via `{% extends "base.html" %}` |

### Ticker Search Box
| | |
|---|---|
| **File** | `templates/base.html` — `<input id="navTickerInput">` + JS `initNavSearch()` |
| **Used by** | All desktop pages |

### Market Indicators Strip  *(VIX / 10-Year)*
| | |
|---|---|
| **File** | `templates/base.html` — `#navVix`, `#navTen` + JS fetch |
| **Used by** | All desktop pages |

### Theme Toggle  *(dark/light)*
| | |
|---|---|
| **File** | `templates/base.html` — `#themeToggle` button + `initTheme()` JS |
| **Used by** | All desktop pages |

---

## Shared Tokens

### Design Tokens
| | |
|---|---|
| **File** | `static/css/tokens.css` |
| **Contains** | `:root` color palette (`--bg-dark`, `--accent-green`, …), typography vars, `--screener-accent` |
| **Light theme** | `body.light-theme { … }` overrides in the same file |

### Shared Components CSS
| | |
|---|---|
| **File** | `static/css/components.css` |
| **Contains** | Global reset, Card, Btn, utility classes, Watchlist Button, Dislike Button, Ticker Notes Modal, Dislike Modal |

---

## Action Buttons & Modals  *(global, all screener pages)*

### Watchlist Button
| | |
|---|---|
| **CSS class** | `.abi-wl-btn` / `.abi-wl-btn.in-watchlist` |
| **CSS file** | `static/css/components.css` |
| **JS logic** | `window._wlToggle()` in `templates/base.html` |
| **Screener hook** | `window._screener_toggleWatchlist()` in `static/js/desktop/screener-app.js` |
| **Used by** | All desktop screener pages via the shared shell |

### Dislike Button
| | |
|---|---|
| **CSS class** | `.abi-dl-btn` / `.abi-dl-btn.is-disliked` |
| **CSS file** | `static/css/components.css` |
| **JS logic** | `window._dlOpen()` / `window._dlOpenForTicker()` in `templates/base.html` |
| **Used by** | All desktop screener pages |

### Ticker Notes Modal
| | |
|---|---|
| **CSS class** | `.wl-modal-overlay` + `.wl-modal` |
| **CSS file** | `static/css/components.css` |
| **JS logic** | `window._notesOpen()` in `templates/base.html` |
| **Screener hook** | `window._screener_openNotes()` in `static/js/desktop/screener-app.js` |
| **Used by** | All desktop screener pages |

### Dislike Modal
| | |
|---|---|
| **CSS class** | `.wl-modal.dl-theme` |
| **CSS file** | `static/css/components.css` |
| **JS logic** | `window._dlOpen()` in `templates/base.html` |

---

## Desktop Screener Components

One layout system for all 9 desktop screener pages: shared shell + `screener.css` + `DesktopScreener.init(config)`.

| Page | Template | Route |
|------|----------|-------|
| Main View | `main_view.html` | `/main-view` |
| Vol Spike & Gaps | `volspike_gapper.html` | `/volspike-gapper` |
| Top Returns | `top_performance.html` | `/top-performance` |
| High Growth | `high_sales_growth.html` | `/high-sales-growth` |
| Slow/Fast RS | `rs_screener.html` | `/rs-screener` |
| Top Losers | `top_losers.html` | `/top-losers` |
| All Stocks | `all_stocks.html` | `/all-stocks` |
| Technical | `technical_screener.html` | `/technical-screener` |
| Abi Watchlist | `abi_watchlist.html` | `/abi-watchlist` |

### Screener Page  *(shared shell)*
| | |
|---|---|
| **HTML** | `templates/desktop/_screener_shell.html` |
| **CSS** | `static/css/screener.css` |
| **JS engine** | `static/js/desktop/screener-app.js` — `DesktopScreener.init(config)` |
| **Jinja slots** | `screener_label`, `screener_stats_label`, `show_cap_tabs`, `show_sector_bar`, `topbar_extra_controls`, `below_topbar_extra`, `detail_header_extras`, `subchart_top`, `subchart_bottom`, `left_header_extra` |
| **Config params** | `endpoint`, `endpointFn`, `capFilter`, `defaultSort`, `accentCss`, `wideStorageKey`, `listValueFn`, `listExtraFn`, `listPrefixFn`, `groupByFn`, `groupLabelFn`, `groupCollapseStorageKey`, `onListRendered`, `extraControlsHtml`, `sortFn`, `filterFn`, `renderListFn` (escape hatch), `updateMetricsFn`, `prependMetricsFn`, `onChartLoaded`, `onTimeframeChange`, `onReady`, `onStockSelected`, `resortOnStarChange`, `seedWatchlistFromData`, `removeOnUnwatch` |
| **Used by** | All 9 desktop screener templates above (thin wrappers) |

### Cap Tab Strip
| | |
|---|---|
| **CSS class** | `.cap-tabs` / `.cap-tab` / `.cap-tab.active` |
| **CSS file** | `static/css/screener.css` |
| **Accent color** | `var(--screener-accent)` — set per page via `DesktopScreener.init({ accentCss })` |
| **JS** | `screener-app.js` |
| **Hide** | `{% set show_cap_tabs = false %}` before the shell include |

### Sector / Industry Filter
| | |
|---|---|
| **CSS class** | `.si-bar` / `.si-tab` / `.industry-bar` |
| **CSS file** | `static/css/screener.css` |
| **JS** | `renderSectorTabs()` / `renderIndustryTabs()` in `screener-app.js` |
| **Hide** | `{% set show_sector_bar = false %}` before the shell include |

### Stock List Panel
| | |
|---|---|
| **CSS class** | `.tp-left` / `.stock-list` / `.stock-item.two-row` / `.list-extra` / `.day-group-*` |
| **CSS file** | `static/css/screener.css` |
| **JS** | Canonical `stockRowHtml()` + `renderList()` in `screener-app.js`. Page hooks: `listValueFn`, `listExtraFn`, `listPrefixFn`, `groupByFn`. Prefer those over `renderListFn`. |

### Star Rating Widget
| | |
|---|---|
| **CSS class** | `.star-rating` / `.star` / `.star.filled` |
| **CSS file** | `static/css/screener.css` |
| **JS** | `starsHtml()` + `setStarsForTicker()` in `screener-app.js` |

### Stock Detail Panel Header
| | |
|---|---|
| **CSS class** | `.chart-ticker-info`, `.chart-timeframes` |
| **CSS file** | `static/css/screener.css` |
| **HTML** | `templates/desktop/_screener_shell.html` — `#detailHeader` |

### Price Chart
| | |
|---|---|
| **File** | `static/js/stock-chart.js` — `StockChart` class |
| **CSS** | `.chart-panel`, `.charts-container` in `screener.css` |
| **Config** | `CHART_CONFIG` constant in `stock-chart.js` |
| **Used by** | All screener pages, `stock.html` detail page |

### News Panel
| | |
|---|---|
| **File** | `static/js/stock-news-shared.js` — `StockNewsShared.createNewsPanel()` |
| **CSS** | `.tp-stack-panel`, `.tp-news-inner`, `.news-load-btn` in `screener.css`; `.benzinga-*` in `benzinga-news.css` |
| **Used by** | All screener pages |
| **Stack collapse** | `#tpNewsCollapseBtn` toggles `.collapsed` on `#tpNewsStack` |

### Metrics Panel
| | |
|---|---|
| **CSS class** | `.tp-metrics-stack` / `.ms-section` / `.ms-item` / `.ms-val` |
| **CSS file** | `static/css/screener.css` |
| **JS** | `updateMetrics()` in `screener-app.js` |
| **Stack collapse** | `#tpMetricsHead` toggles `.collapsed` on `#tpMetricsStack` |

### Right Column Collapse
| | |
|---|---|
| **HTML** | `#tpColToggle` on `#tpRightColumn` in `_screener_shell.html` |
| **CSS** | `.tp-right-column.col-collapsed` in `screener.css` |
| **JS** | `setupColumnToggle()` in `screener-app.js` |
| **Persistence** | `localStorage['tpColCollapsed']` |
| **Effect** | Collapses news+metrics to a thin rail so the chart column expands |

### Tags Strip
| | |
|---|---|
| **CSS class** | `.ms-tags` / `.tag-pill` (and modifiers: `.high-growth`, `.spike`, `.gapper`, `.event`, `.ti65-tag`) |
| **CSS file** | `static/css/screener.css` |
| **JS** | `updateTagsStrip()` in `screener-app.js` |

---

## Card Grid Components

### Theme Card Grid
| | |
|---|---|
| **File** | `templates/themes.html` |
| **CSS** | `static/css/card-grid.css` + `.theme-card-*` overrides inline |
| **Accent** | `--card-grid-accent: var(--accent-purple)` set in `themes.html` |

### ETF Card Grid
| | |
|---|---|
| **File** | `templates/etfs.html` |
| **CSS** | `static/css/card-grid.css` + `.etf-card-*` overrides inline |
| **Accent** | `--card-grid-accent: var(--accent-blue)` set in `etfs.html` |

---

## RSI Index Table

### RSI Index Table  *(SPX / NDX / DJI)*
| | |
|---|---|
| **Template** | `templates/rsi_index.html` — single template for all 3 indices |
| **Routes** | `/rsi-spx`, `/rsi-ndx`, `/rsi-dji` in `app.py` |
| **Config vars** | `rsi_page_title`, `rsi_slug`, `rsi_label`, `rsi_gradient`, `rsi_color`, `rsi_description`, `rsi_subtitle`, `rsi_info_text` |
| **Accent** | `--rsi-color` CSS var injected from `rsi_color` route param |

---

## Other Pages

### Market Brief Viewer
| | |
|---|---|
| **File** | `templates/market_brief.html` |
| **Route** | `/market-brief` |

### Market Brief History
| | |
|---|---|
| **File** | `templates/market_brief_history.html` |
| **Route** | `/market-brief/history` |

---

## Mobile Components  *(separate /m/* site)*

### Mobile Screener Shell
| | |
|---|---|
| **File** | `templates/mobile/_screener_shell.html` |
| **CSS** | `static/css/mobile/shell.css` |
| **JS engine** | `static/js/mobile/screener-app.js` |
| **Used by** | All mobile screener pages (thin config wrappers in `templates/mobile/`) |

---

## Adding a New Component

1. Add CSS to the appropriate file (`components.css`, `screener.css`, or `card-grid.css`)
2. Add the HTML to the appropriate partial (`_screener_shell.html`, `_nav_drawer.html`, etc.)
3. Add JS to the appropriate engine (`screener-app.js`, `base.html`)
4. Update this catalog with the new entry
