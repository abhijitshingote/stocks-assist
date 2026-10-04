# Px vs Benzinga — experiment notes (resume here)

Goal: decide whether Market Brief - Px (`perplexity_brief.py`) can replace the Benzinga brief
(`run_pipeline.py`). Px may carry extra material; it must not miss Benzinga's nuance or get numbers
wrong. Branch `px-brief-experiment`, local only.

**What matters (user, 9/28):** qualitative parity — nearly every Benzinga story captured,
especially the big ones, with the same attribution for each move and the same nuance/depth
(causal chains, named sources/officials, "why it matters"). Exact % / price levels / odds depend
on snapshot time and are not critical. Judge runs on the Major-tier coverage + attribution first.

## How to run the loop (weekday, live)

1. ~8 AM ET the user triggers both briefs (or: `POST localhost:5001/api/market-brief/generate` and
   `/api/market-brief-px/generate`). Note the Px start time → that is the cutoff for re-runs.
2. Re-run Px with the same cutoff so no later news leaks in (Perplexity date filters are
   day-granular only):
   `docker compose exec -T backend sh -c 'cd /app && python -m market_brief.perplexity_brief --date YYYY-MM-DD --cutoff HH:MM --no-compact'`
   - `--resume-followups` reuses phase A, re-runs B/C onward. `--skip-research` reuses all research.
   - `--eval-only` re-grades the current brief.
3. Snapshot each iteration: `cp -r user_data/market_brief_perplexity/<date> .../<date>_itN`.
4. Before a live run, check the DB tape is current (`tape.md` header = previous session). The
   container cron is broken (`python: not found`, `run_triggers.py` renamed); on 9/28 prices came
   from a prod restore (`./manage-env.sh prod restore`).
5. The backend container has no `kill` binary; use `sh -c 'kill PID'`.

## Eval (compare_nuance.md)

- `eval_items.md` (per date, cached): Sonnet extracts ~60 items from the Benzinga brief.
- Sonnet grades every item: found / partial / missing / conflict / baseline_error. Score is
  computed in code from the table (the LLM's own tally was wrong before 9/28 — all pre-9/28
  scores, incl. "60%" on 9/24–25, are inflated and not comparable).
- Recall = (found + baseline_error + 0.5·partial) / N. Story coverage = not-missing / N.
- Benzinga is not ground truth: on 9/28 it had NKE "Wednesday Oct 1" and MU "Tuesday Sep 30"
  (both wrong weekdays). `baseline_error` + a calendar block in the eval handle this.
- Run-to-run noise is ±4 pts (Perplexity result variance + synthesis selection). One run per
  change cannot separate changes smaller than that; average 2–3 runs.

## Results 2026-09-28 (cutoff 08:41 ET, 59 items)

| Run | Change | Recall | found/partial/missing/conflict |
|---|---|---|---|
| it1 (live 8:41) | baseline (26 probes, compaction) | 41.5% | 11/27/19/1 |
| it2 | +4 overnight probes (corporate, analyst, megacap, policy), weekend window text | 39.8% | 13/21/22/2 |
| it3 | it2 research; `--no-compact`; synth priority order + larger caps | 41.5% | 14/21/20/3 |
| it4 | +12 probes (10 sector company-news, earnings_next, credit_housing); planner bug → no phase B | **45.8%** | 10/34/12/2 |
| it5 | it4 phase A + phase B/C (planner fixed) | 39.8% | 8/31/17/2 |
| it6 | full run: ET-conversion rule, latest-reading rule, adj/GAAP rule, calendar, verify pass, eval baseline_error | 43.2% (story coverage 74.6%) | 9/33/14/2 |

it6 accuracy: oil now Brent +3.82% $108.30 / WTI +3.81% at 05:59 ET (it4: +1.5% from a stale
07:49 print); FedWatch 68–70% (vs 70.3%); NKE Thu Oct 1 / MU Wed Sep 30 correct; verify fixed
2 wrong weekdays (FDS, JBL "Tue Sep 30" → Wed). Remaining conflicts: DELL catalyst (MS bull case
vs Oracle force-majeure relief) and Brent level (Benzinga $100.93 likely a different contract;
Px's +3.8% matches its move). NVDA PM +0.8% vs Benzinga +1.6% (different snapshot). The eval
still grades Benzinga's wrong weekdays as partial, not baseline_error — grader is conservative.
Recall moved within noise; the gain is fewer wrong numbers, not more items.

Cost ≈ $4/run full (Perplexity ~$2 for ~100 calls, Anthropic ~$2), ~35 min with Opus synth + revise.

## Qualitative eval (v3, 9/28 13:30) — use this going forward

Eval now ignores snapshot-timing numbers, grades attribution (`misattributed` status) and tags
each item major/minor. Report: All and Major recall + story coverage. Re-grades
(`compare_nuance_v3.md`):

| Run | All recall · coverage | Major recall · coverage |
|---|---|---|
| it4 | 49.2% · 78.0% | 52.9% · 94.1% (16/17) |
| it6 | 51.7% · 72.9% | 53.6% · 85.7% (12/14) |

Caveat: the grader assigns tiers per grading (17 vs 14 major) and still dings some numbers
(COST EPS, gasoline price) → move tier into `eval_items.md` extraction so the major set is fixed
per date; tighten the "ignore numbers" rule.

**Big stories — qualitative read (it4/it6):**
- Captured with same attribution: NVDA $150B buyback, MSFT Copilot + Stifel upgrade, Trump–Xi
  tariff deal/AI dialogue, Trump rejects Iran Hormuz proposal → oil spike, AKAM–Anthropic deal core
  terms, ZS CRO change, COST beat, MU earnings preview, PPLI/MGM, AESI AI-lab power, SHMD.
- Missed in every run: **MDB CEO Desai → Meta, Ittycheria interim** (weekend 8-K).
- Wrong attribution: **GFI −16%** — Px blames the gold drop; Benzinga: Northern Star rejected
  GFI's $27.1B bid. **DELL +5%** — Px: MS bull case/$95B backlog; Benzinga: Oracle Project Jupiter
  force-majeure relief (Px has the Oracle story, not linked to DELL). **META −3%** — Px:
  profit-taking after +13% week; Benzinga: Goldman $300B AI-revenue warning + Muse privacy.
- Thin nuance: ZS "falls despite beat + FY27 guide above consensus" framing; Iran FM Araghchi
  "fully prepared" quote; AKAM secondary terms ($5.5B capex, Jabil memory, Lenovo); MU BMO
  supercycle / 3× demand; NVDA agent platform partner list + Anthropic integration; QCOM
  Snapdragon launch; AMD BofA thesis.
- Pattern: Px under-links an event to the other stocks it moved (Oracle → DELL/BE), and when a
  stock falls it defaults to a generic reason (gold, profit-taking) instead of the company event.

**META attribution fix (9/28 14:25, it6 research + new META gap fill, resynth):**
- Cause: META ranked below the 15-name gap-fill cutoff (sorted by |move|), and broad probes only
  gave "profit-taking after +13% week". A "why is META down" query finds Goldman's $300B warning
  naming Meta, a Muse data-exposure vulnerability, Amazon blocking Muse, NM Cambridge Analytica
  verdict.
- Changes: gap fill sorts Mega first and also triggers on generic reasons
  (`GENERIC_REASON_RE`: profit-taking / reversal / no new catalyst / broad selloff); gap prompt
  asks "why is X down/up", sector analyst warnings, product controversies; megacap probe asks for
  the reason reporters give; synth rule: no profit-taking-only attribution for mega-caps when a
  warning/controversy names the company.
- Result: brief now "Profit-taking after +13% Muse rally; GS AI capex breakeven warning" → I03
  partial (Goldman linked; Muse privacy still dropped by synth; profit-taking still listed first).
  Major recall 60.5% · coverage 94.7% (it6 was 53.6% · 85.7%; one run, within noise).
- Open: synth drops the privacy/controversy line; if it persists, add "name each controversy"
  to the mega-cap rule.

## Results 2026-09-29 (cutoff 08:28 ET, 58 items, eval v3)

Legacy $2.46. Tape: session 9/28, 58/58 priced (DB current).

| Run | Change | All recall · coverage | Major recall · coverage | Top Movers overlap |
|---|---|---|---|---|
| it1 (API, live 8:28) | 9/28 code, compaction ON (API default) | 38.8% · 56.9% | 63.6% · 100% (11) | 19/22 |
| it2 | it1 research; `--no-compact`; Quick Hits section (≤30) | 43.1% · 69.0% | 61.5% · 100% (13) | 19/22 |
| it3 | full rerun; + `press_releases`, `macro_secondary` probes; no-compact default | 40.5% · 63.8% | 63.2% · 100% (19) | 18/22 |
| it4 | it3 research; Quick Hits ≤50 (brief wrote 59) | 43.1% · 62.1% | 65.8% · 94.7% (19) | 18/22 |

Findings:
- Big stories: all captured every run (MDB CEO → Meta and META Enterprise Platform / Desai hire,
  BA 737 MAX glitch + FAA delay, OpenAI GPT-6.1 Astra cancellation, White House AI lunch, Anthropic
  IPO prospectus, yields 5.24%, Iran/Hormuz, MU preview, KOD, NVTS, AMC). Gaps are nuance
  details inside them (PANW BTIG thesis, MDB interim CEO + guide reaffirm, MOVE index, Bessent
  15M bbl, Axios "HOAX", JPM/BofA MU notes). CAFE rule (TSLA credits) missed in it4.
- Long tail is the gap (18–25 missing, all minor): overnight earnings (JEF, MTN, UEC, CNXC),
  energy/industrial press releases (FLR LNG Canada $7.5B, TRP Coastal GasLink FID, SLB Rovuma,
  CAT/Fabick, SSTI take-private), CBOE–S&P license, Netlist ITC, GS Solomon succession, Dallas
  Fed, CRFB, Zervos, Roadster delay, STX.
- Compaction drops long-tail facts (Northern Star, Dallas Fed, SSTI, DevDay in research, lost in
  ledger) → no-compact is now the default (`--compact` to opt in).
- Quick Hits helps breadth (+12 pts coverage it1→it2) but raising the cap to 50 did not: synth
  fills it with Px-only items, not the Benzinga long tail. Several found-in-research items
  (UEC, CNXC, Starship, Northern Star, Dallas Fed) still get dropped.
- Each Perplexity probe returns ~9–12 companies regardless of how broad the ask is; the single
  `press_releases` probe found none of the target deals. Long-tail breadth needs more, narrower
  queries (e.g. press releases × sector), not broader prompts.
- Verify: 9/29 it2 proposed 9 fixes copied from notes (0 applied) — prompt now requires `find`
  from the brief and ignores <1-pt price diffs.
- INTC "misattributed" in it2: Px gave company-specific causes (Apple dropping Intel Mac, OpenAI
  incident) vs Benzinga "yields/profit-taking" — Px arguably better.
- Grader tiers still vary per run (11–19 major) → Major numbers are not comparable across runs
  until tier is fixed in `eval_items.md`.

### 9/29 controlled check (eval v4: tier fixed in `eval_items.md`, 17 major / 57)

Re-grade of saved runs — Major recall · All coverage: it1 61.8% · 57.9%; it2 58.8% · 64.9%;
it3 70.6% · 61.4%; it4 (committed code) 67.6% · 59.6%. Major coverage 100% in all four.

A/B on identical it3 research (`--skip-research --outdir ab_0929_<slot>`), 2 runs each:
Quick Hits ≤30 → Major 64.7/67.6 (avg 66.2), All coverage 59.6/59.6; ≤50 → Major 70.6/67.6
(avg 69.1), All coverage 61.4/61.4. Keep 50. Synthesis noise on identical input: ~3–6 pts Major.

**Current best = committed code; its 9/29 brief = `user_data/market_brief_perplexity/2026-09-29/`
(it4, `/market-brief-px`).** Process from now on: every change is A/B'd on identical research
(≥2 runs each) before commit; only search-side changes need live days. 9/28 is not used for
re-runs (post-cutoff leakage).

## Results 2026-10-02 (final day; cutoff 08:20 ET, 57 items, 11 major) — committed code 5a9159e

Live run (API, committed code): Major 90.9% coverage (10/11) · 68.2% recall; All 68.4% coverage ·
46.5% recall; Top Movers 25/30. Px $4.92, legacy $2.44.
- Only major miss: optical rally — **misattributed** (user-confirmed): the catalyst was US
  restrictions on Chinese 3.2T optical transceivers (FCC rule per Morgan Stanley; bipartisan bill);
  Px led with Bernstein's initiation. Perplexity's default answer is Bernstein; the policy angle
  surfaces only when the query asks for China/policy. Fix (px-brief, general, untested live): gap
  prompt also searches why the whole group moved (government action on the industry, competitor/
  customer events) even when an analyst action explains the stock; trade probe covers
  restrictions on foreign-made products/components; synth leads a group move with the catalyst
  that explains the whole group over a note on one or a few names. Validate on the next live day
  (10/02 rerun now would leak).
- Missing minor (18): ~9 never found (IMF / El-Erian, Samsung HBM price triple, STX −10% PM,
  Dell/JERA Japan DC, Canada Pacific Link, Polymarket diesel odds, MU buyback window, FAI optical
  report, ACN MS PT), ~7 found but dropped by synth (INFY deals, NKE PACE + analyst actions, MU
  Taiwan strike vote, H100 smuggling case, APP–Unity suit, LW preview).

A/B rejected (identical live research, 2 runs each): coverage step also listing ≤25 "Quick Hits"
candidates + revise integrating them → All coverage 65.8% vs 66.7% control, Major recall 63.6% vs
68.2%. Reverted.

## Final assessment (stop point)

Px with the committed code, over 9/29 and 10/2 live days:
- Big stories: 91–100% present; ~68% carry full Benzinga nuance (attribution + key details).
  Attribution mostly matches; where it differs Px usually gives a sourced, company-specific cause.
- All stories: ~60–68% present. The gap is Benzinga's long tail (small earnings, press
  releases, niche policy/analyst items), which Perplexity's ~10 companies/query search does not
  reach without many more queries.
- Synthesis levers are exhausted (Quick Hits cap, coverage candidates, compaction off). Further
  gains need search breadth (sector-split press releases/earnings), judged over several live
  days — not worth more same-day iteration.
- Cost ~$5/run vs legacy ~$2.5; runtime ~18 min.

## Gaps (it4, best run)

Story present in some form: 46/59 (78%); at full detail: 10/59 (17%).

**Wrong numbers/catalysts (root causes):**
- Oil: Perplexity mislabelled a 09:59 GMT (= 05:59 ET) quote as after the cutoff; synth used an
  older 07:49 ET print (+1.5% vs +3.8%). → ET-conversion rule in FACT_RULES + verify pass.
- NVDA PM −1% (05:36 Yahoo, pre-buyback) vs +1.7% (Reuters 07:05, post-buyback). → latest-reading rule.
- FedWatch: five readings 64–77.5% from different days; synth picked an old one. → latest-reading rule.
- COST EPS: $6.60 adjusted (Zacks) vs $6.75 GAAP (legacy). → label adj vs GAAP.
- META catalyst (Goldman $300B warning / Muse privacy) and DELL catalyst (Oracle force majeure
  easing) — Px chose a different explanation. Not fixed yet.

**Missing entirely (search breadth; all 6 tested were findable with a targeted query):**
MDB CEO → Meta; Jana/FISV; MS cuts AMAT/CAMT PTs; Corteva $35M FTC; Bessent Iran 15M bbl;
Trump–tech CEOs 9/29; retail ETF/SOXX outflows; Huang AI-factory economics; GLND/Greenland; MTN
earnings tonight; APLD Oct 8; Tesla Roadster Oct 1.

**Partial (biggest bucket, 31–34/59):** story present, figures dropped — COST capex/openings/comps,
ZS beat + FY27 guide, GFI/Northern Star $27.1B, RBLX bookings 5% vs 13%, AMD BofA $720,
Goldman $300B breakeven, Burry AGI, Amodei dinner/Oval Office. Mix of synthesis compression and
research variance (COST capex, ABBV tavapadon, Solidigm found in it1 research, absent in it2).

**Px-only material (fine):** AKAM +21% PM, DC Circuit/Anthropic Pentagon ruling, Apple $5.7B
verdict, Netlist ITC vs MU/SMCI/HPE, Anthropic GPU deals (Nscale/TeraWulf/Lambda), Berkshire LEN.

## Next steps

**After 9/29 (do first):**
- A. Fix tier in `eval_items.md` extraction (major/minor per item, cached) so Major is comparable.
- B. Long tail: split `press_releases` by sector (5 queries) and add an "earnings since last
  close, every company" probe split by cap/sector; keep Quick Hits ≤30.
- C. Synth drops of found long-tail items: coverage step should flag Quick-Hits-worthy omissions
  (currently only "material" ones).
- D. Nuance inside big stories: thread follow-ups should ask for the analyst thesis details and
  company statements (interim CEO, guidance reaffirmed) — planner prompt.

0. Priority (qualitative): (a) attribution — per big mover, a gap-fill probe that asks "what
   company-specific event explains X's move" even when research already has a generic reason
   (gold, sector, profit-taking); synth rule: prefer company event > sector/macro reason and
   state both; (b) cross-links — thread planner/synth must connect an event to every stock it
   moved (Oracle force majeure → DELL, BE); (c) weekend 8-K / exec-change probe (MDB);
   (d) fixed major tier in `eval_items.md`.
1. Keep verify + timing rules (it6 fixed oil/FedWatch/weekday errors).
2. Research variance: run key probes twice (or two phrasings) and union — cheap (~$0.03/call).
3. Breadth: dedicated probes for what was missed — SEC 8-K item 5.02 exec changes, activist
   letters (13D), FTC/DOJ settlements (Law360), CEO interviews (Huang/Altman on podcasts),
   retail-flow data (Vanda, JPM retail desk), White House schedule for the week, EV/auto events.
4. Partial → found: synthesis currently compresses; let the brief be longer (Px extra material is
   allowed) or add a per-story "carry all figures" check in the coverage step (coverage currently
   only flags omissions, not dropped figures).
5. Catalyst choice (META/DELL): when research gives several explanations, state all with sources.
6. Average ≥2 runs per config before concluding; do ≥3 live days before deciding on retirement.
7. Decide defaults: `--no-compact` looked neutral-to-better; make default if it holds.

## Artifacts

- `user_data/market_brief_perplexity/2026-09-28_it{1..5}/` — per-iteration snapshots (brief,
  research, eval). `compare_nuance_v2*.md` in it1/it2 = re-grades with the fixed eval.
- `user_data/market_brief/2026-09-28/02_brief.md` — Benzinga baseline.
- `user_data/market_brief_perplexity_v1|_v2/` — 9/24–25 backdated experiments (inflated scores).
