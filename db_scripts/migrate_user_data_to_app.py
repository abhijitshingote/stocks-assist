"""One-time load of ``user_data/`` files into the Postgres ``app`` schema.

Idempotent: user stores are replaced wholesale, briefs/runs are upserted.
Experiment runs (non-date dirs: ``ab_*``, ``*_itN``, ``*-v2``) are skipped.

    docker compose exec backend python db_scripts/migrate_user_data_to_app.py --dry-run
    docker compose exec backend python db_scripts/migrate_user_data_to_app.py
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_store import artifacts, user  # noqa: E402

DATE_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}$")

DICT_STORES = {
    "abi_watchlist.json": user.WATCHLIST,
    "abi_dislikes.json": user.DISLIKES,
    "abi_passes.json": user.PASSES,
    "abi_trades.json": user.TRADES,
    "abi_ticker_notes.json": user.TICKER_NOTES,
    "abi_chart_notes.json": user.CHART_NOTES,
    "abi_chart_levels.json": user.CHART_LEVELS,
    "price_alerts_state.json": user.ALERT_FIRES,
}
LIST_STORES = {
    "abi_general_notes.json": user.GENERAL_NOTES,
    "themes.json": user.THEMES,
    "etfs.json": user.ETF_GROUPS,
}

# (user_data subdir, kind, source)
BRIEF_DIRS = (
    ("market_brief", "daily", "benzinga"),
    ("market_brief_perplexity", "daily", "perplexity"),
    ("market_brief_losers", "losers", "benzinga"),
    ("market_brief_perplexity_losers", "losers", "perplexity"),
)
# (user_data subdir, pipeline, files the UI reads; None = all top-level *.json)
RUN_DIRS = (
    ("daily_screener", "daily_screener", None),
    ("theme_discovery", "theme_discovery", ("themes.json", "run_summary.json", "run_costs.json")),
    ("market_narratives", "narratives", ("events.json", "run_costs.json")),
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _norm(store: Any, entry: dict[str, Any]) -> dict[str, Any]:
    """Comparable form of a file entry: known fields, no Nones, timestamps as instants."""
    kinds = dict(store.cols)
    out = {}
    for f, v in entry.items():
        if f not in kinds or v is None:
            continue
        if kinds[f] == "ts":
            v = datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()
        out[f] = v
    return out


def migrate_user_stores(ud: Path, dry: bool) -> bool:
    ok = True
    for name, store in {**DICT_STORES, **LIST_STORES}.items():
        path = ud / name
        if not path.exists():
            print(f"  - {name}: missing, skipped")
            continue
        src = _read_json(path)
        is_dict = name in DICT_STORES
        n = len(src)
        if dry:
            print(f"  · {name} → app.{store.table}: {n} rows")
            continue
        if is_dict:
            user.save_dict(store, src)
            back = user.load_dict(store)
            same = list(back) == list(src) and all(
                _norm(store, back[k]) == _norm(store, src[k]) for k in src
            )
        else:
            user.save_list(store, src)
            back = user.load_list(store)
            same = len(back) == n and all(_norm(store, a) == _norm(store, b) for a, b in zip(back, src))
        ok &= same
        print(f"  {'✓' if same else '✗ MISMATCH'} {name} → app.{store.table}: {n} rows")

    fb = ud / "daily_screener_feedback.json"
    if fb.exists():
        src = _read_json(fb)
        if dry:
            print(f"  · daily_screener_feedback.json → app.screener_feedback: {sum(map(len, src.values()))} rows")
        else:
            user.save_feedback(src)
            same = user.load_feedback() == {d: v for d, v in src.items() if v}
            ok &= same
            print(f"  {'✓' if same else '✗ MISMATCH'} daily_screener_feedback.json → app.screener_feedback")
    return ok


def migrate_briefs(ud: Path, dry: bool) -> None:
    for sub, kind, source in BRIEF_DIRS:
        base = ud / sub
        if not base.is_dir():
            continue
        done, skipped = [], []
        for d in sorted(base.iterdir()):
            if not d.is_dir():
                continue
            md = d / "02_brief.md"
            if not DATE_DIR.match(d.name) or not md.is_file():
                skipped.append(d.name)
                continue
            opt = lambda n: _read_json(d / n) if (d / n).is_file() else None  # noqa: E731
            if not dry:
                artifacts.upsert_brief(
                    d.name, kind, source,
                    brief_md=md.read_text(encoding="utf-8"),
                    brief_json=opt("02_brief.json"),
                    run_costs=opt("run_costs.json"),
                    metadata=opt("metadata.json"),
                )
            done.append(d.name)
        print(f"  {'·' if dry else '✓'} {sub} → briefs[{kind}/{source}]: {len(done)} "
              f"({done[0] if done else '-'} … {done[-1] if done else '-'}); skipped {skipped or 'none'}")


def migrate_runs(ud: Path, dry: bool) -> None:
    for sub, pipeline, names in RUN_DIRS:
        base = ud / sub
        if not base.is_dir():
            continue
        done, skipped = [], []
        for d in sorted(base.iterdir()):
            if not d.is_dir():
                continue
            paths = sorted(d.glob("*.json")) if names is None else [d / n for n in names]
            files = {p.name: _read_json(p) for p in paths if p.is_file() and p.name != "status.json"}
            if not DATE_DIR.match(d.name) or not files:
                skipped.append(d.name)
                continue
            if not dry:
                artifacts.put_run(pipeline, d.name, files)
            done.append(f"{d.name}({len(files)})")
        print(f"  {'·' if dry else '✓'} {sub} → pipeline_runs[{pipeline}]: {done or 'none'}; skipped {skipped or 'none'}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--user-data", default=str(ROOT / "user_data"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    ud = Path(args.user_data)

    print(f"user_data: {ud}{'  (dry run)' if args.dry_run else ''}")
    print("User stores:")
    ok = migrate_user_stores(ud, args.dry_run)
    print("Briefs:")
    migrate_briefs(ud, args.dry_run)
    print("Pipeline runs:")
    migrate_runs(ud, args.dry_run)
    if not ok:
        print("\n✗ Round-trip mismatch — inspect before switching over.")
        return 1
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
