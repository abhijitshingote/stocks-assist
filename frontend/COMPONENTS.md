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
| **Layout** | Single ~42px row; all page links in a Menu overlay panel (no scroll strip / no wrap into the bar); short `thru Aug 7` next to logo. Panel rows: Daily / Weekly / Screeners / Abi / Research / Ops |
| **Responsive** | `static/css/responsive.css` (minor indicator tweaks); Menu panel owned by `base.html` |
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

One layout system for all 14 desktop screener pages: shared shell + `screener.css` + `DesktopScreener.init(config)`.

| Page | Template | Route |
|------|----------|-------|
| Daily Review | `daily_review.html` | `/daily-review` |
| Main View | `main_view.html` | `/main-view` |
| Vol Spike & Gaps | `volspike_gapper.html` | `/volspike-gapper` |
| Vol Spike & Gaps (90d) | `volspike_gapper_90d.html` | `/volspike-gapper-90d` |
| Strong Stocks | `strong_stocks.html` | `/strong-stocks` |
| Top Returns | `top_performance.html` | `/top-performance` |
| Top 5D/20D | `top_returns_5_20.html` | `/top-returns-5-20` |
| High Growth | `high_sales_growth.html` | `/high-sales-growth` |
| Slow/Fast RS | `rs_screener.html` | `/rs-screener` |
| Fast RS | `fast_rs.html` | `/fast-rs` |
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
| **Jinja slots** | `screener_label`, `screener_stats_label`, `show_cap_tabs`, `show_sector_bar`, `topbar_extra_controls`, `below_topbar_extra`, `detail_header_extras`, `subchart_top`, `subchart_bottom`, `left_header_extra`. Shell always includes `#screenerExcludes` (Biotech exclude chip). |
| **Config params** | `endpoint`, `endpointFn`, `capFilter`, `defaultSort`, `accentCss`, `listValueFn`, `listExtraFn`, `listPrefixFn`, `groupByFn`, `groupLabelFn`, `groupCollapseStorageKey`, `onListRendered`, `extraControlsHtml`, `sortFn`, `filterFn`, `renderListFn` (escape hatch), `updateMetricsFn`, `prependMetricsFn`, `onChartLoaded`, `onTimeframeChange`, `onReady`, `onStockSelected`, `resortOnStarChange`, `seedWatchlistFromData`, `removeOnUnwatch`. Shared `− Biotech` chip is AND-ed with `filterFn`. |
| **Used by** | All desktop screener templates above (thin wrappers) |

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
| **HTML** | `templates/desktop/_screener_shell.html` — `#detailHeader` (`#detailTi65` / `#detailLowFloat` populated by `screener-app.js`) |

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

> Rule: mobile consumes `static/css/tokens.css` read-only via `@import` in `static/css/mobile/tokens.css`. Mobile-only custom properties go in `static/css/mobile/tokens.css`. Never add mobile-only variables to the shared `static/css/tokens.css`.

### CSS Layer Stack

Every mobile page loads these layers in order:

| File | Contents |
|------|----------|
| `static/css/mobile/tokens.css` | `@import '../tokens.css'`; aliases `--bg-dark → --bg`, `--text-primary → --fg`, `--accent-green → --success`, etc.; mobile-only vars (radius, motion, safe-area, Inter font override) |
| `static/css/mobile/base.css` | Reset, `html/body`, `.phone`, `.app-header`, logo, search box, menu button, `header-row2` |
| `static/css/mobile/screener.css` | Filter chip/drawer, list zone, detail zone, tabs, charts panel, sector sheet, mode toggle |
| `static/css/mobile/panels.css` | Loading overlay, spinner, news tab, metrics rows, notes modal, tag pills, star rating |
| `static/css/mobile/nav.css` | Nav drawer backdrop, sheet, links, footer, drawer filter strips |
| `static/css/mobile/benzinga-overrides.css` | Dark-mode overrides for `benzinga-news.css` (must load after it) |
| `static/css/mobile/pages.css` | Utility-page layouts: master-detail, context charts, logs viewer, market brief, market news, home dashboard |

Screener pages also prepend `static/css/benzinga-news.css` before the layer stack. Utility pages append `pages.css` after the layer stack. Both handled by `templates/mobile/_mobile_styles.html`.

### Central Style Partial
| | |
|---|---|
| **File** | `templates/mobile/_mobile_styles.html` |
| **Loads** | `tokens.css` → `base.css` → `screener.css` → `panels.css` → `nav.css` → `benzinga-overrides.css` |
| **Used by** | `_shell.html` (utility pages) and `_screener_styles.html` (screener pages) |

### Central Script Partial
| | |
|---|---|
| **File** | `templates/mobile/_page_libs.html` |
| **Usage** | Set `page_libs` list in the page; always appends `nav.js` + `search.js` |
| **Tokens** | `charts`, `markdown`, `stock-chart`, `news`, `master-detail`, `util`, `screener` |
| **Used by** | `_shell.html` (utility pages) and `_screener_libs.html` (screener pages) |

---

### Mobile Page Shell  *(utility pages)*
| | |
|---|---|
| **File** | `templates/mobile/_shell.html` |
| **Extends** | `templates/mobile/_base.html` |
| **CSS** | `_mobile_styles.html` + `pages.css` (default; overrideable via `styles` block) |
| **Blocks** | `title`, `page_styles`, `body_class`, `before_header`, `content` (required), `outside_phone`, `page_scripts` |
| **Used by** | All 8 utility mobile pages |

### Mobile App Header
| | |
|---|---|
| **File** | `templates/mobile/_app_header.html` |
| **Variants** | `header_stats=true` → VIX/10Y/count row2 (screener); `page_title` set → subtitle + `utility-header` class; neither → bare header-row1 (stock page) |
| **Used by** | `_shell.html`, `_screener_shell.html` |

### Mobile Nav Drawer
| | |
|---|---|
| **File** | `templates/mobile/_nav_drawer.html` |
| **CSS** | `static/css/mobile/nav.css` |
| **JS** | `static/js/mobile/nav.js` |
| **Sections** | Home, Daily, Weekly, Screeners, Abi, Research |
| **Used by** | `_shell.html`, `_screener_shell.html` |

### Shared Detail Panel
| | |
|---|---|
| **File** | `templates/mobile/_detail_panel.html` |
| **Variables** | `detail_ticker` (default `—`), `detail_link_href` (default `#`), `detail_link_label` (default `Detail →`) |
| **Used by** | `_screener_shell.html`, `stock.html` |

### Shared Notes Modal
| | |
|---|---|
| **File** | `templates/mobile/_notes_modal.html` |
| **CSS** | `.wl-modal-overlay` in `panels.css` |
| **Used by** | `_screener_shell.html` (inside `.phone`), `stock.html` (via `outside_phone` block) |

---

### Mobile Screener Shell
| | |
|---|---|
| **File** | `templates/mobile/_screener_shell.html` |
| **CSS** | `_screener_styles.html` → benzinga-news.css + `_mobile_styles.html` |
| **JS engine** | `static/js/mobile/screener-app.js` |
| **Script partial** | `templates/mobile/_screener_libs.html` (sets `page_libs` and calls `_page_libs.html`) |
| **Exclude** | `#screenerExcludes` filled by the engine (Biotech excluded on every page load; chip-off is session-only). AND-ed with `filterStocks`. |
| **Used by** | All 5 mobile screener pages (thin config wrappers) |

### Mobile Screener Pages

| Page | Template | Route |
|------|----------|-------|
| Main View | `mobile/main_view.html` | `/m/main-view` |
| Vol Spike & Gaps | `mobile/volspike_gapper.html` | `/m/volspike-gapper` |
| Vol Spike & Gaps (W) | `mobile/volspike_gapper_weekly.html` | `/m/volspike-gapper-weekly` |
| Vol Spike & Gaps (90d) | `mobile/volspike_gapper_90d.html` | `/m/volspike-gapper-90d` |
| Strong Stocks | `mobile/strong_stocks.html` | `/m/strong-stocks` |
| Top Returns | `mobile/top_performance.html` | `/m/top-performance` |
| Top 5D/20D | `mobile/top_returns_5_20.html` | `/m/top-returns-5-20` |
| Top Losers | `mobile/top_losers.html` | `/m/top-losers` |
| High Growth | `mobile/high_sales_growth.html` | `/m/high-sales-growth` |
| Slow/Fast RS | `mobile/rs_screener.html` | `/m/rs-screener` |
| Fast RS | `mobile/fast_rs.html` | `/m/fast-rs` |
| All Stocks | `mobile/all_stocks.html` | `/m/all-stocks` |
| Technical | `mobile/technical_screener.html` | `/m/technical-screener` |
| Abi Watchlist | `mobile/abi_watchlist.html` | `/m/abi-watchlist` |

### Mobile Utility Pages

| Page | Template | Route | `page_libs` |
|------|----------|-------|-------------|
| Dashboard | `mobile/index.html` | `/m` | `['charts']` |
| Context | `mobile/context.html` | `/m/context` | `['charts']` |
| Context 2 | `mobile/context2.html` | `/m/context-2` | `[]` |
| Market Brief | `mobile/market_brief.html` | `/m/market-brief` | `[]` |
| Market News | `mobile/market_news.html` | `/m/market-news` | `['news']` |
| Logs | `mobile/logs.html` | `/m/logs` | `['master-detail']` |
| Abi Notes | `mobile/abi_general_notes.html` | `/m/abi-general-notes` | `['master-detail']` |
| Stock Detail | `mobile/stock.html` | `/m/stock/<ticker>` | `['charts','markdown','stock-chart','news','util']` |

---

### Adding a New Mobile Page

**Utility page:**
1. Create `templates/mobile/my_page.html` extending `_shell.html`; set `page_title`, `page_libs`, fill `content` and `page_scripts` blocks
2. Add CSS to `pages.css`; add JS to `static/js/mobile/pages/my-page.js`
3. Add route in `frontend/app.py` under `/m/...`
4. Add link in `templates/mobile/_nav_drawer.html`
5. Update this catalog

**Screener page (stock list + detail panel):**
1. Create a 5-line wrapper (see `volspike_gapper.html` as a template)
2. Create `static/js/mobile/pages/my-screener.js` calling `MobileScreener.init({...})`
3. Add route and nav link as above; update this catalog

---

## Adding a New Component

1. Add CSS to the appropriate file (`components.css`, `screener.css`, or `card-grid.css`)
2. Add the HTML to the appropriate partial (`_screener_shell.html`, `_nav_drawer.html`, etc.)
3. Add JS to the appropriate engine (`screener-app.js`, `base.html`)
4. Update this catalog with the new entry
