"""Search plan + prompts for Market Brief - Px (``perplexity_brief.py``).

Phases: A = narrow broad probes + small ticker batches · B = thread follow-ups planned from A ·
C = single-ticker gap fills for unexplained movers · compaction → synthesis → eval vs Benzinga.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

CATEGORY_VOCAB = (
    "AI Compute · Memory & Interconnect · Optical & Photonics · Chip Equipment · "
    "Fab & Foundry · Wireless & Mobile · Analog & Mixed-Signal · Power & Wide-Bandgap · "
    "Test & Advanced Packaging · Specialty Materials & IP Licensing · Quantum Computing · "
    "EdgeAI · Semiconductors · AI Infrastructure · Software & SaaS · Internet & Platforms · "
    "Communications & Networking"
)

# Perplexity ``search_domain_filter`` (max 20; "-" prefix = exclude). Social posts, penny-stock
# promo, SEO aggregators seen in 2026-09-24/25 research returning stale or wrong figures.
DOMAIN_DENYLIST = [
    "-x.com", "-twitter.com", "-stocktwits.com", "-reddit.com", "-timothysykes.com",
    "-stockstotrade.com", "-tickeron.com", "-tistory.com", "-note.com", "-dovejournal.com",
    "-thecompanychronicle.com", "-zarnewsdesk.com", "-ts2.tech", "-news.stocktradersdaily.com",
    "-tradingtips.com", "-premarketprice.com", "-svmuu.com", "-news.futunn.com",
    "-weissratings.com", "-quartercharts.com",
]

# group: compaction bucket. window: "session" (session-1d..asof+1d) | "premarket" (session..asof+1d).
BROAD_PROBES: list[dict[str, str]] = [
    # --- macro ---
    {"slug": "index_closes", "group": "macro", "window": "session", "label": "Index closes & futures",
     "focus": "S&P 500, Nasdaq Composite, Dow, Russell 2000, PHLX SOX closes (level, % change); "
              "S&P sector leaders/laggards; biggest index point contributors; index futures "
              "pre-market on the brief date (% change, time quoted)."},
    {"slug": "rates_fed", "group": "macro", "window": "session", "label": "Rates & Fed",
     "focus": "2-yr, 10-yr, 30-yr Treasury yields (level, bp change); Treasury auction results "
              "(high yield, bid-to-cover, tail); CME FedWatch probabilities for the next FOMC; "
              "every Fed official who spoke, with verbatim quotes."},
    {"slug": "fed_policy_context", "group": "macro", "window": "session",
     "label": "Fed policy & inflation context",
     "focus": "The most recent FOMC decision (date, bp change, vote) and the Fed chair's latest "
              "statements on inflation and rates (verbatim quotes); latest CPI, core CPI, PCE "
              "prints; named economists and investors (e.g. El-Erian, Dimon, Summers, bank chief "
              "economists) commenting on the yield move or inflation in the window, with quotes; "
              "CEOs quoted on inflation or consumer demand."},
    {"slug": "econ_data", "group": "macro", "window": "session", "label": "Economic data",
     "focus": "Every US economic release on the session date and the brief date through the "
              "cutoff (PMIs, jobless claims, durable goods, housing, GDP, inflation, sentiment): "
              "actual vs consensus vs prior, and release time."},
    {"slug": "oil_energy", "group": "macro", "window": "session", "label": "Oil & energy",
     "focus": "WTI and Brent (settle, % change, contract month), natural gas, gasoline and diesel "
              "prices (AAA/EIA), EIA inventory data, OPEC+ actions, energy policy (export bans, "
              "SPR), energy-stock analyst actions, energy M&A (acquirer, target, $ value), "
              "activist stakes in energy companies."},
    {"slug": "geopolitics_trade", "group": "macro", "window": "session",
     "label": "Geopolitics & trade",
     "focus": "US-China relations (summits, trade truce, tariffs, export controls, chip rules, "
              "restrictions on foreign-made products or components — with the US stocks that "
              "benefit or lose), "
              "Iran / Middle East / Strait of Hormuz, Russia-Ukraine, sanctions — named officials, "
              "dates, terms, deadlines; latest status as of the cutoff."},
    {"slug": "crypto", "group": "macro", "window": "session", "label": "Crypto",
     "focus": "Bitcoin and Ether price (level, 24h % change, time quoted); spot ETF flows; SEC/CFTC/"
              "Congress crypto actions; tokenization news; crypto-treasury company offerings."},
    {"slug": "global_fx_metals", "group": "macro", "window": "session", "label": "Global, FX, metals",
     "focus": "DXY, USD/JPY, EUR/USD; gold, silver, copper (price, % change); ECB, BoE, BoJ, PBoC "
              "actions and quotes; Europe/Asia index moves overnight into the brief date."},
    # --- corporate ---
    {"slug": "movers_session", "group": "corporate", "window": "session",
     "label": "Session movers",
     "focus": "US stocks >= $2B market cap making the biggest moves in the regular session "
              "(CNBC / MarketWatch / Barron's / Investopedia / Reuters mover roundups): % move and "
              "the stated catalyst for each."},
    {"slug": "movers_premarket", "group": "corporate", "window": "premarket",
     "label": "After-hours & pre-market movers",
     "focus": "US stocks >= $1B moving after hours on the session date and pre-market on the "
              "brief date: % move, time quoted, and catalyst (earnings, offerings, M&A, guidance, "
              "rating changes, withdrawn deals)."},
    {"slug": "earnings", "group": "corporate", "window": "premarket", "label": "Earnings reported",
     "focus": "Every company that reported after the close on the session date or before the "
              "open on the brief date: EPS and revenue actual vs consensus, guidance old → new vs "
              "consensus, key KPIs, stock reaction."},
    {"slug": "analyst_actions", "group": "corporate", "window": "session",
     "label": "Analyst actions",
     "focus": "Upgrades, downgrades, initiations and price-target changes for US stocks >= $2B "
              "on the session date and brief date: firm, rating old → new, PT old → new, thesis "
              "in one line."},
    {"slug": "deals_filings", "group": "corporate", "window": "session",
     "label": "Deals, offerings, filings",
     "focus": "M&A (terms, value, withdrawn or approved deals), equity/convert/debt offerings "
              "(size, price, coupon), activist stakes, 13D/13G, buybacks, index adds/deletes, IPOs, "
              "lockup expiries, large contracts with $ value."},
    {"slug": "fda_healthcare", "group": "corporate", "window": "session", "label": "FDA & healthcare",
     "focus": "FDA approvals, CRLs, advisory panel votes, PDUFA outcomes, trial readouts, pharma "
              "licensing deals ($ upfront/milestones), drug pricing policy, diagnostics news."},
    {"slug": "consumer_industrials", "group": "corporate", "window": "session",
     "label": "Consumer, industrials, financials",
     "focus": "Material company news in retail, restaurants, travel, autos/EV, industrials, "
              "defense, space, banks, insurers, gaming — recalls, investor days, guidance, "
              "management quotes with numbers."},
    {"slug": "overnight_corporate", "group": "corporate", "window": "premarket",
     "label": "Overnight & weekend company announcements",
     "focus": "Company announcements for US stocks >= $2B published after the session close "
              "through the cutoff (press releases, 8-Ks, wire reports): buyback authorizations, "
              "CEO/CFO departures and appointments, takeover bids, bid rejections, merger terms, "
              "FDA approvals and trial readouts, guidance changes, activist letters, major "
              "contracts and partnerships. Company, figure, and pre-market move."},
    {"slug": "premarket_analyst_actions", "group": "corporate", "window": "premarket",
     "label": "Brief-date analyst actions",
     "focus": "Upgrades, downgrades, initiations and PT changes published on the brief date "
              "before the open (and over the weekend if the brief date is a Monday): firm, "
              "rating old → new, PT old → new, thesis, and the stock's pre-market move."},
    {"slug": "megacap_premarket", "group": "corporate", "window": "premarket",
     "label": "Mega-cap pre-market",
     "focus": "NVDA, AAPL, MSFT, GOOGL, AMZN, META, TSLA, AVGO, AMD, MU, ORCL, NFLX: each one's "
              "pre-market % move on the brief date and any news since the session close "
              "(announcements, executive interviews and quotes, product events, analyst notes), "
              "and the reason reporters give for each move — including analyst warnings on the "
              "company or its sector (capex returns, valuation) and product controversies "
              "(privacy, safety, regulatory complaints)."},
    {"slug": "overnight_policy", "group": "macro", "window": "premarket",
     "label": "Overnight & weekend policy",
     "focus": "Statements and actions since the session close by the President, Treasury "
              "Secretary, Commerce, USTR, Energy Secretary, Fed officials, and foreign leaders "
              "(China, Iran, Russia, EU): Sunday TV interviews, weekend summits, executive "
              "orders, scheduled White House meetings or announcements for the coming days, "
              "AI regulation remarks. Verbatim quotes with names and dates."},
    *[
        {"slug": f"sector_{slug}", "group": "corporate", "window": "session",
         "label": f"Company news: {label}",
         "focus": f"Every material company-specific news item for US-listed {label} companies "
                  ">= $1B in the window, as many as you can find: press releases, 8-K filings, "
                  "executive changes, activist campaigns, legal settlements, contracts, product "
                  "launches, guidance, analyst rating/PT changes (firm, old → new), and the stock "
                  "move. One bullet per company."}
        for slug, label in [
            ("semis", "semiconductor and semiconductor-equipment"),
            ("software", "software, SaaS, database and cybersecurity"),
            ("internet_media", "internet, media, gaming and telecom"),
            ("hardware_networking", "IT hardware, networking, data-center and power-equipment"),
            ("biotech_pharma", "biotech, pharma and medical-device"),
            ("financials", "bank, broker, insurer, payments and fintech"),
            ("energy_materials", "oil & gas, mining, chemicals and agriculture"),
            ("consumer", "retail, restaurant, apparel, travel and leisure"),
            ("industrials", "industrial, aerospace, defense, autos/EV and transport"),
            ("realestate_utilities", "REIT, homebuilder and utility"),
        ]
    ],
    {"slug": "press_releases", "group": "corporate", "window": "session",
     "label": "Press releases: deals & contracts",
     "focus": "Press releases (BusinessWire, PR Newswire, GlobeNewswire, company IR) from US-listed "
              "companies >= $1B and large foreign ADRs: acquisitions and take-privates (price per "
              "share, CVRs), contract awards >= $100M, final investment decisions, JV awards, "
              "licensing and exchange/index agreements, dealer or asset purchases. One bullet per "
              "company with the $ figure and stock move."},
    {"slug": "macro_secondary", "group": "macro", "window": "session",
     "label": "Secondary macro & officials",
     "focus": "Regional Fed surveys (Dallas, Richmond, Kansas City, Empire, Philly) actual vs "
              "prior; every Fed governor/president speech with verbatim quotes; Treasury and White "
              "House economic appointments; fiscal watchdog reports (CBO, CRFB) on debt and "
              "rates; foreign CEOs of US-listed companies commenting on trade/export controls."},
    {"slug": "earnings_next", "group": "calendar", "window": "premarket",
     "label": "Earnings tonight & tomorrow",
     "focus": "Companies reporting after the close on the brief date or before the open on the "
              "next session: EPS and revenue consensus, year-ago figures, options-implied move, "
              "and analyst previews (firm, rating, PT)."},
    {"slug": "credit_housing", "group": "macro", "window": "session",
     "label": "Credit & housing",
     "focus": "High-yield and CCC bond yields, credit spreads, leveraged-loan and private-credit "
              "stress, named credit strategists (e.g. Apollo's Torsten Slok) with quotes; "
              "mortgage rates (Freddie Mac, MBA, MND), housing data, and the rate impact on "
              "REITs, homebuilders and mortgage lenders with stock moves."},
    # --- themes ---
    {"slug": "ai_capex", "group": "themes", "window": "session", "label": "AI capex & cloud",
     "focus": "Hyperscaler capex estimates and commentary (with firm/analyst name), AI cloud and "
              "compute deals ($ value, term), data-center projects, delays or cancellations, AI "
              "financing (bonds, converts, private credit), OpenAI/Anthropic/xAI funding and "
              "compute commitments, GPU cloud pricing."},
    {"slug": "semis_supply", "group": "themes", "window": "session",
     "label": "Semis & supply chain",
     "focus": "TSMC / Samsung / SK hynix / Micron / Intel news; wafer and memory (DRAM, NAND, HBM) "
              "pricing; chip supply/demand estimates; equipment orders; supply-chain reports "
              "(Nikkei, DigiTimes, TrendForce, CLSA); China chip probes and export-control actions."},
    {"slug": "ai_platforms", "group": "themes", "window": "session",
     "label": "AI platforms & disruption",
     "focus": "Consumer and enterprise AI product launches (agents, models, devices), adoption "
              "metrics, big-tech event announcements; analyst notes naming stocks that win or "
              "lose from these launches (disruption baskets, second-order beneficiaries) with "
              "the stock moves."},
    {"slug": "software_security", "group": "themes", "window": "session",
     "label": "Software & cybersecurity",
     "focus": "SaaS and cybersecurity company news, analyst notes and PT changes, AI-agent impact "
              "on software seats and pricing, sector ETF (IGV) move and drivers."},
    {"slug": "ai_policy", "group": "themes", "window": "session", "label": "AI policy & safety",
     "focus": "Government actions on AI (White House, Congress, EU, UK, China), AI safety "
              "statements by named executives, antitrust actions against tech platforms, "
              "court rulings affecting tech."},
    {"slug": "strategist_views", "group": "themes", "window": "session",
     "label": "Strategists & positioning",
     "focus": "Named strategist and investor commentary with numbers (index targets, rate "
              "calls, sector calls), fund flows, positioning/short-interest data, sentiment "
              "gauges, research notes on market-wide themes (AI capex returns, AI productivity "
              "surveys, bubble/misallocation debates by named academics or firms)."},
    {"slug": "market_voices", "group": "themes", "window": "session",
     "label": "Named investor & analyst calls",
     "focus": "Stock picks and pans by named investors and TV/podcast analysts (CNBC Halftime, "
              "Fast Money, Squawk Box, Bloomberg TV — e.g. Josh Brown, Gene Munster, Dan Ives, "
              "Cathie Wood, Michael Burry positions) with the tickers named; research-firm "
              "baskets and theses (Citrini, Hunterbrook, short-seller reports) with tickers."},
    {"slug": "speculative_growth", "group": "themes", "window": "session",
     "label": "Quantum, space, nuclear, robotics",
     "focus": "Quantum computing (IonQ, Rigetti, D-Wave, Quantum Computing Inc, Infleqtion, IBM, "
              "Google), space (SpaceX, Rocket Lab, AST SpaceMobile), nuclear/SMR, eVTOL, "
              "robotics/humanoids, drones: technical milestones with figures, contracts, "
              "offerings, analyst actions, conferences, and each stock's move including sympathy "
              "moves."},
    {"slug": "legal_ip", "group": "themes", "window": "session", "label": "Legal, IP, regulatory",
     "focus": "Patent suits and ITC investigations (named parties, products at risk of import "
              "bans), antitrust cases, major court rulings, SEC/DOJ/FTC enforcement, foreign "
              "regulator probes of US companies (China SAMR/SASAC/MOFCOM, EU) — named companies."},
    # --- calendar ---
    {"slug": "calendar_earnings", "group": "calendar", "window": "session",
     "label": "Earnings calendar",
     "focus": "Earnings from the brief date through the next 8 NYSE sessions for US stocks >= "
              "$10B plus notable mid-caps: date, BMO/AMC, EPS and revenue consensus vs year-ago, "
              "and analyst preview notes / PT changes ahead of the print (firm, rating, PT). "
              "Only events that have NOT yet reported as of the cutoff."},
    {"slug": "calendar_events", "group": "calendar", "window": "session",
     "label": "Macro & event calendar",
     "focus": "Economic releases (time, consensus, prior), Fed speakers (time), Treasury auctions, "
              "investor/analyst days, FDA PDUFA dates, conferences, options expiry, index "
              "rebalances, lockup expiries from the brief date through the next 5 NYSE sessions."},
]

COMPACT_GROUPS: dict[str, str] = {
    "macro": "Macro, rates, commodities, crypto, geopolitics, calendar",
    "corporate": "Movers, earnings, analyst actions, deals, FDA, sector company news",
    "themes": "Cross-cutting themes, second-order effects, thread follow-ups",
}


def pretty(d: str) -> str:
    return datetime.strptime(d, "%Y-%m-%d").strftime("%A %B %-d, %Y")


def window_text(session_date: str, asof: str, cutoff: str, *, premarket: bool = False) -> str:
    start = (
        f"{pretty(session_date)} 4:00 PM ET (after the close)" if premarket
        else f"{pretty(session_date)} regular session (9:30-16:00 ET)"
    )
    gap = (datetime.strptime(asof, "%Y-%m-%d") - datetime.strptime(session_date, "%Y-%m-%d")).days
    weekend = (
        f" This spans {gap - 1} non-trading day(s): news published on those days (e.g. Saturday "
        "and Sunday) is in scope and often the most important." if gap > 1 else ""
    )
    return (
        f"{start} through {pretty(asof)} {cutoff} ET, including after-hours, overnight and "
        f"pre-market news.{weekend} Anything published after {cutoff} ET on {pretty(asof)} is "
        "out of scope."
    )


FACT_RULES = """RULES
- Facts only. Every bullet has a specific number, name, or date. No bullet without a data point.
- End every bullet with the named source and publish time if known: (Reuters, Sep 24 07:12 ET).
  Convert every time to ET (ET = GMT-4 / UTC-4 in EDT; 09:59 GMT = 05:59 ET) before judging whether
  it is inside the window. For prices quoted several times, report the latest reading before the
  cutoff with its time.
  "Analysts said" / "reports suggest" without a name is not allowed.
- Earnings: actual vs consensus for EPS and revenue; guidance old → new.
- Analyst actions: firm, rating old → new, PT old → new.
- Mark unconfirmed items (people familiar, leaks, social media, M&A speculation) with [rumor]. A
  Bloomberg/Reuters/WSJ report citing sources is still reportable: state it and mark [report].
- If one event moved several stocks, name every stock and its move in the same bullet.
- Only facts published in the window. Older background only if it is the direct cause of a move in
  the window, labelled with its date.
- Do not interpret price action. Do not write "strong", "beat expectations", "investors cheered",
  "remains", "continues to", "well-positioned", "tailwinds". Write the number.
- Do not write bullets about what you could not find. Omit the topic instead.
- Do not drop facts to save space."""


def _ticker_row(sym: str, d: dict[str, Any], moves: dict[str, float], cap_label: str) -> str:
    d1 = moves.get(sym, d.get("dr_1"))
    ev = f"{d.get('last_event_type')} {d.get('last_event_date')}" if d.get("last_event_date") else "—"
    return "| {t} | {c} | {cap} | {sec} | {d1} | {d5} | {vol} | {ev} |".format(
        t=sym,
        c=d.get("company_name") or "",
        cap=cap_label,
        sec=d.get("label") or d.get("section") or "",
        d1=f"{d1:+.2f}%" if d1 is not None else "n/a",
        d5=f"{d['dr_5']:+.1f}%" if d.get("dr_5") is not None else "n/a",
        vol=f"{d['vol_vs_10d_avg']:.1f}×" if d.get("vol_vs_10d_avg") is not None else "n/a",
        ev=ev,
    )


def tape_table(
    tickers: list[str], lineage: dict[str, Any], moves: dict[str, float], cap_of: Any
) -> str:
    by_ticker = lineage.get("by_ticker") or {}
    rows = [
        "| Ticker | Company | Cap | Screen | 1D close | 5D | Vol× 10d | Vol/gap event |",
        "|---|---|---|---|---:|---:|---:|---|",
    ]
    for sym in tickers:
        d = by_ticker.get(sym, {})
        rows.append(_ticker_row(sym, d, moves, cap_of(d.get("market_cap"))))
    return "\n".join(rows)


def ticker_batch_prompt(table: str, session_date: str, asof: str, cutoff: str) -> str:
    return f"""Search the web now for news on each stock below. You are a financial fact extractor for a
pre-market brief. Brief date: {pretty(asof)}.

Window: {window_text(session_date, asof, cutoff)}

VERIFIED TAPE (from exchange OHLC data; authoritative, do not restate different prices):
{table}

TASK
Run a separate search for EACH ticker ("<company> stock news", "<company> shares <session date>").
Find why it moved on {pretty(session_date)}, any after-hours / pre-market move on {pretty(asof)}, and
other material news in the window. Search press releases (BusinessWire, PR Newswire, GlobeNewswire),
SEC filings, FDA, Reuters, Bloomberg, CNBC, WSJ, Barron's, MarketWatch, Investor's Business Daily,
company IR pages, analyst-action roundups.
If a mover has no company news, check whether another company's event moved it (customer, supplier,
competitor, partner, sector report) and name that company and event.

OUTPUT (markdown; one section per ticker, in the order given; no intro or conclusion):

## Company Name (TICKER) `[Category]`
- fact (source)

Category: pick from {CATEGORY_VOCAB}, or a concise sector name (Pharma, Energy, Retail, ...).
If you find nothing, write exactly one bullet: "- No company-specific catalyst found in window" and,
if applicable, a second bullet naming the sympathy driver.

Include when available: catalyst for the move, earnings figures, guidance, analyst actions, contracts
with $ size, filings, regulatory decisions, management quotes with numbers, next catalyst date.

{FACT_RULES}"""


def gap_prompt(row_table: str, sym: str, company: str, session_date: str, asof: str,
               cutoff: str, prior: str) -> str:
    return f"""Search the web now. You are a financial fact extractor for a pre-market brief.
Brief date: {pretty(asof)}. Target: {company} ({sym}).

Window: {window_text(session_date, asof, cutoff)}

VERIFIED TAPE (authoritative):
{row_table}

A first search found no catalyst, or only a generic reason (profit-taking, reversal, sector move):
{prior or "(nothing)"}

TASK
Search harder for why {sym} moved. Run several searches: "why is {company} stock down/up",
"{company} stock", "{sym} shares {pretty(session_date)}", "{company} concerns", "{company} FDA",
"{company} analyst", "{company} price target", "{company} deal", "{company} offering". Check:
analyst warnings on the company or its sector (capex returns, AI monetization, valuation), product
controversies (privacy, safety, lawsuits, verdicts), government action on the company's industry
(rules, restrictions on rivals, trade measures, legislation, contracts) and broker notes on it.
Even if an analyst action already explains the move, also search why the whole group moved
("{company} peers rally/fall why", "<industry> stocks <date>") — a group-wide catalyst often sits
behind a single-firm explanation, regulatory approvals, product launches, licensing deals, analyst
actions (including notes on the prior day), index changes, filings, insider transactions, and news
about customers, suppliers, competitors or the sector (name the lead stock and its event).
Also report any after-hours / pre-market move on {pretty(asof)} with its catalyst.

OUTPUT (markdown; no intro):

## {company} ({sym}) `[Category]`
- fact (source)

Category: pick from {CATEGORY_VOCAB}, or a concise sector name.
If you still find nothing, write exactly: "- No company-specific catalyst found in window".

{FACT_RULES}"""


def channel_prompt(probe: dict[str, str], session_date: str, asof: str, cutoff: str) -> str:
    return f"""Search the web now. You are a financial fact extractor for a US-equity pre-market brief.
Brief date: {pretty(asof)}. Topic: {probe['label']}.

Window: {window_text(session_date, asof, cutoff, premarket=probe['window'] == 'premarket')}

FIND
{probe['focus']}

Run multiple searches with different phrasings and dates. Prefer primary sources and wire services
(Reuters, Bloomberg, AP, CNBC, WSJ, MarketWatch, Barron's, Investopedia, company press releases, SEC
EDGAR, BLS/BEA/Census/Fed releases, FDA). For every item give the latest status as of the cutoff.

OUTPUT (markdown; no intro or conclusion). Group by ticker or topic:

## TICKER or Topic `[Category]`
- fact (source)

Category: pick from {CATEGORY_VOCAB}, or Macro / Commodities / Geopolitics / Crypto / Calendar /
Pharma / Energy / Retail, or a concise sector name. Bold tickers.

{FACT_RULES}"""


def thread_prompt(thread: dict[str, Any], session_date: str, asof: str, cutoff: str) -> str:
    qs = "\n".join(f"- {q}" for q in thread.get("questions") or [])
    tickers = ", ".join(thread.get("tickers") or []) or "—"
    return f"""Search the web now. You are a financial fact extractor for a US-equity pre-market brief.
Brief date: {pretty(asof)}. Story: {thread.get('title')}. Related tickers: {tickers}.

Window: {window_text(session_date, asof, cutoff)}

Why this needs follow-up: {thread.get('why') or '—'}

ANSWER EACH QUESTION (run a separate search for each):
{qs}

Name every stock affected by this story with its move, analyst notes that name winners and losers
(firm, rating, PT), exact figures (deal values, estimates, dates), and the latest status as of the
cutoff. Prefer Reuters, Bloomberg, CNBC, WSJ, Barron's, MarketWatch, company releases, filings.

OUTPUT (markdown; no intro or conclusion). Group by ticker or sub-topic:

## TICKER or Sub-topic `[Category]`
- fact (source)

{FACT_RULES}"""


PLANNER_SYSTEM = """\
You plan follow-up web searches for a pre-market equity brief. You receive a verified price tape and
first-pass research notes. Identify the stories where one more targeted search would add material
facts a sophisticated investor would expect in the brief. Prioritize, in order:

1. Causal links across tickers: an event at one company (deal, force majeure, product launch,
   analyst note) that likely moved other stocks — ask who else moved and why.
2. Second-order effects of major launches or policy moves: which stocks analysts named as winners
   or losers, disruption baskets, supplier/customer read-throughs.
3. Tape moves >= 3% (or mega-cap moves >= 1.5%) whose stated catalyst is thin, generic
   ("sector bid"), contested, or cites an unconfirmed report.
4. Big stories mentioned without numbers (capex estimates, deal terms, data prints, poll odds).
5. Contradictions between notes (different prices, different catalysts, reported vs "upcoming").
6. Developing stories (summits, negotiations, policy threats) — latest status as of the cutoff.

Do not plan searches for facts already covered with numbers and a named source.
Do not plan threads on events published after the brief cutoff.
Do not plan a thread per ticker for simple single-ticker moves; a separate gap-fill step covers
unexplained movers.

Output only a JSON object in a ```json fenced block:
{"threads": [{"slug": "snake_case_max_30_chars", "title": "...", "why": "one sentence",
              "tickers": ["SYM", ...], "questions": ["specific search question", ...]}]}
Each thread: 2-4 questions, each a concrete searchable question with names and dates.
"""


def planner_user(*, asof: str, cutoff: str, tape: str, research: str, max_threads: int) -> str:
    return (
        f"<brief_date>{pretty(asof)} (cutoff {cutoff} ET)</brief_date>\n\n"
        f"<tape>\n{tape}\n</tape>\n\n"
        f"<research>\n{research}\n</research>\n\n"
        f"Plan at most {max_threads} follow-up threads, most valuable first."
    )


COMPACT_SYSTEM = """\
You consolidate raw web-research notes into a deduplicated fact ledger for a pre-market brief.
The notes come from many overlapping searches, so the same fact often appears several times with
slightly different wording or numbers.

Rules:
- Keep every distinct material fact with its numbers, names, dates and source. Do not summarize,
  do not shorten figures, do not drop minor tickers. Losing a fact is worse than keeping a duplicate.
- Merge duplicates into one bullet; keep the most specific figure and list all sources.
- Merge bullets about the same story (e.g. one event that moved several stocks) under one heading,
  so causal links between companies stay visible.
- Conflicting figures for the same thing: keep the one from the most authoritative and most recent
  source (wire service / primary release > aggregator > blog) and append
  "[conflict: other source said X]".
- Timing: anything published after the brief cutoff is out of scope — drop it. An event that has
  already been reported (earnings, data print) must not also appear as upcoming.
- Drop bullets that say information was not found, and drop meta commentary.
- Keep [rumor] / [report] tags.

Output markdown: `## Topic or TICKER `[Category]`` headings, one fact per bullet, most market-moving
topics first. No intro or conclusion.
"""


def compact_user(*, asof: str, cutoff: str, group_label: str, notes: str) -> str:
    return (
        f"<brief_date>{pretty(asof)} (cutoff {cutoff} ET)</brief_date>\n"
        f"<group>{group_label}</group>\n\n"
        f"<notes>\n{notes}\n</notes>\n\n"
        "Write the fact ledger."
    )


SYNTH_ADDENDUM = """

---

ADDITIONAL RULES FOR THIS RUN (override the above where they conflict):

INPUT: channel_summaries holds research notes from ~70 web searches; ticker_summaries
lists every universe ticker with its tape move, most important first. The input is larger than the
brief should be — your job is to select, not to transcribe everything.

LENGTH BUDGET (hard caps):
- Top Movers: at most 35 rows, only rows with an identified catalyst. Every stock >= $2B moving
  >= 5% pre-market on the brief date on earnings/deal/guidance news must be included (mark "PM"),
  whether or not it is in the universe. Mega-caps with brief-date news (buyback, product launch,
  exec interview, downgrade) get a row even on a small pre-market move.
- Narrative Threads: at most 12 theme blocks, each at most 12 bullets. Undercurrents: at most 12.
- Channel Pulse: at most 10 rows. On The Radar: at most 16 bullets. What to Ignore: 2-3 bullets.
- Quick Hits: add a section `### 📌 Quick Hits` right before On The Radar — up to 50 one-line
  bullets for material facts in the input that are not covered elsewhere in the brief: earnings
  reported since the last close (EPS/revenue vs est, move), small/mid-cap deals and contracts, exec
  changes, secondary data prints, Fed/official quotes, legal/regulatory actions, analyst actions,
  launches/milestones. One fact per line: **TICKER** or name — what happened (key figure). This is
  where breadth goes; do not repeat thread content.
Choose by market significance, in this order:
1. Brief-date and non-trading-day news (overnight, weekend, pre-market): company announcements
   (buybacks, CEO/CFO changes, bids and bid rejections, FDA approvals, activist campaigns,
   settlements), analyst actions, official statements. These are what the reader has not seen.
2. Cross-sector implications, mega/large-cap moves, causal chains between companies.
3. Session recaps of mid/small caps — shortest treatment; drop first.
Spend the budget on numbers, not prose: a bullet carries every figure the input has for that
story (e.g. capex old → new, store openings, segment growth, every analyst PT change).

NUANCE:
- Where one event moved several stocks (a launch hurting incumbents, a deal lifting a supplier, a
  force majeure hitting peers), make that chain explicit in one thread: event → each stock's move,
  including stocks outside the universe that the ledger names (losers as well as winners).
- Keep macro context that explains the tape: the latest Fed decision and inflation print, named
  policymaker and strategist quotes, data prints with consensus.
- Prefer the causal explanation that links to another company's event over a generic one (insider
  sale, "sector bid") when both are present; state both if unclear.
- When several stocks in one group move together, lead with the catalyst that explains the whole
  group (policy or regulation, a competitor/customer event, industry data) over a note on one or a
  few names; name both when the input has both.
- Never attribute a mega/large-cap move only to "profit-taking", "mechanical reversal" or "no new
  catalyst" when the input has an analyst warning, research note or controversy that names the
  company or its group (e.g. a hyperscaler capex/AI-revenue warning for META/MSFT/GOOGL/AMZN/ORCL,
  a product privacy complaint). Lead with that; profit-taking may follow as a secondary reason.

TIMING:
- The brief is published at the cutoff time on the brief date. An event already reported (earnings,
  data print) is a fact, never "upcoming". Pre-market moves on the brief date belong in the brief.
- Conflicting figures: never print two different values for the same thing. For quotes that move
  through the morning (futures, oil, gold, FX, yields, pre-market % moves, FedWatch odds), use the
  latest reading timestamped before the cutoff and state its time ("Brent +3.8% at 05:59 ET").
  A pre-market move quoted after a company announcement supersedes one quoted before it.
- Earnings: label adjusted vs GAAP EPS when sources differ ("adj. EPS $6.60 vs $6.48 est; GAAP
  $6.75"); never merge the two into one figure.
- Weekdays: use the <calendar> block for every weekday/date pair; never infer a weekday.
"""


def calendar_block(asof: str, days: int = 14) -> str:
    from datetime import timedelta

    d0 = datetime.strptime(asof, "%Y-%m-%d")
    rows = [(d0 + timedelta(days=i)).strftime("%a %b %-d") for i in range(-4, days)]
    return "<calendar>\n" + " · ".join(rows) + "\n</calendar>"


VERIFY_SYSTEM = """\
You fact-check a finished pre-market brief against its research notes, the verified price tape and
a calendar. Find only concrete errors:
- a number that contradicts the verified tape (session % moves are authoritative there)
- a fast-moving quote (futures, oil, gold, yields, pre-market move, FedWatch) where the notes have
  a later reading still before the cutoff — convert GMT/UTC to ET (ET = GMT-4 in EDT) before
  judging whether a reading is before the cutoff
- a pre-market move quoted from before a company announcement when a post-announcement quote exists
- a weekday/date pair that contradicts the calendar
- an adjusted vs GAAP EPS mix-up, or a consensus figure that contradicts the most-cited source
- an event stated as upcoming that already happened before the cutoff, or vice versa
Do not restyle, do not add stories, do not flag omissions. Ignore price-level and % differences
under 1 point (snapshot timing) unless the direction is wrong.

Output JSON only: {"fixes": [{"find": "<exact substring of the brief, short but unique>",
"replace": "<corrected text>", "why": "<source + time>"}]}. find must be copied verbatim from
<brief> (including ** markup), never from the notes.
If there are no errors, output {"fixes": []}.
"""


def verify_user(*, brief: str, notes: str, tape: str, calendar: str, asof: str, cutoff: str) -> str:
    return (
        f"<brief_date>{pretty(asof)} (cutoff {cutoff} ET)</brief_date>\n{calendar}\n\n"
        f"<tape>\n{tape}\n</tape>\n\n<notes>\n{notes}\n</notes>\n\n<brief>\n{brief}\n</brief>\n\n"
        "List the fixes."
    )



COVERAGE_SYSTEM = """\
You audit a draft pre-market brief against the fact ledger it was written from. Find material
stories in the ledger that the draft omits or mentions without their key fact. Material means:
- a stock >= $2B moving >= 4% (session or pre-market) with an identified catalyst
- a mega-cap move with an identified catalyst, including stocks outside the ticker universe
- a cross-ticker causal link: one event and the other stocks it moved (winners and losers)
- a macro/policy development with a number or a named official's quote (Fed chair, Treasury,
  central banks), a named investor/economist view on the day's main macro move
- an M&A deal, offering, activist stake, regulatory/legal action (ITC, antitrust, foreign probe)
  involving a company >= $2B
- an upcoming catalyst within 8 sessions with consensus figures
Ignore stories the draft already covers with their key fact, and anything the ledger marks as
published after the cutoff.

Output at most 20 lines, most material first, nothing else:
- <section to place it in> | <story with every number, name, date and source from the ledger>
If nothing material is missing, output exactly: NONE
"""


def coverage_user(*, ledger: str, brief: str) -> str:
    return (
        f"<ledger>\n{ledger}\n</ledger>\n\n<draft_brief>\n{brief}\n</draft_brief>\n\n"
        "List the material omissions."
    )


REVISE_ADDENDUM = """

---

REVISION TASK: You receive a draft brief and a list of material omissions found in its source
ledger. Return the complete revised brief in the same structure. Integrate every omission into
the named section (merge into an existing thread when it belongs there; causal links go in the
thread of the triggering event). The length caps may be exceeded by up to 20% for this. Do not
remove Top Movers rows that have a catalyst, or any other fact with a number; make room by
trimming prose, repeated facts and the What to Ignore section. Output only the brief.
"""


def revise_user(*, draft: str, omissions: str) -> str:
    return (
        f"<draft_brief>\n{draft}\n</draft_brief>\n\n<omissions>\n{omissions}\n</omissions>\n\n"
        "Return the revised brief."
    )


EVAL_ITEMS_SYSTEM = """\
You extract the material items from a pre-market brief so other briefs can be graded against it.

An item is one story an investor would want to know: a ticker's move with its catalyst, a
cross-ticker causal link (event → stocks it moved), a macro or geopolitical development, a policy
action, an analyst action cluster, an upcoming catalyst with a date. Merge minor details into their
parent item (all PT changes on one stock = one item; one product event = one item). Skip color
(ceremony details, attendance lists) and the "What to Ignore" section. Aim for 40-60 items.

Tier: major = a story a reader must not miss (leads a thread, a mega-cap or >= 5% move with a
catalyst, the day's main macro/geopolitical driver, a big deal/CEO change/FDA decision); minor =
everything else. Expect roughly 10-20 major items.

Output one item per line, nothing else:
I01 | major | Section | Item (with its key number / name / date)
"""

EVAL_SYSTEM = """\
You grade a candidate pre-market brief (TEST) against a fixed list of items extracted from a
reference brief (BASELINE) written from a different news source for the same date. The goal: TEST
must not miss the material nuance in BASELINE. Extra material in TEST is fine.

Grade the qualitative content: is the story there, is the move attributed to the same cause, and
is the nuance there (who did what, why it matters, the causal chain to other stocks, the named
source or official). Ignore numeric differences that come from snapshot timing or source choice —
% moves, price levels, odds, yields, EPS decimals, consensus figures. A number difference never
lowers a grade unless it flips direction (up vs down) or the story itself.

Status:
- found: story present with the same attribution and its key nuance
- partial: story present but the attribution, causal link or key qualitative detail is missing
- missing: story absent
- misattributed: TEST explains the move or event with a different cause than BASELINE
- baseline_error: TEST differs because BASELINE is wrong (e.g. a weekday that contradicts the
  <calendar>)

Output markdown exactly (no score section; it is computed from the table):

## Items
| ID | Tier | Item | Status | What TEST lacks or says instead |
|---|---|---|---|---|

One row per item, every item in the list, in list order — including found items (last column
"—"). Tier is copied from the item list. Status is exactly one lowercase word: found, partial,
missing, misattributed or baseline_error.

## TEST-only material
- up to 15 material items in TEST that are not in the item list
"""


def eval_user(*, items: str, test: str, calendar: str) -> str:
    return (
        f"{calendar}\n\n<baseline_items>\n{items}\n</baseline_items>\n\n<test>\n{test}\n</test>\n\n"
        "Grade TEST against the baseline items."
    )
