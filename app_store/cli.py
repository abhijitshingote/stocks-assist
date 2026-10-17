"""Hand-edit a user store as JSON (same shape the old ``user_data/*.json`` files had).

    docker compose exec backend python -m app_store.cli export etf_groups > /tmp/etfs.json
    docker compose exec -T backend python -m app_store.cli import etf_groups < /tmp/etfs.json

``import`` replaces the whole table.
"""

from __future__ import annotations

import argparse
import json
import sys

from app_store import user

STORES = {
    s.table: s for s in (
        user.WATCHLIST, user.DISLIKES, user.PASSES, user.TRADES, user.TICKER_NOTES,
        user.CHART_NOTES, user.CHART_LEVELS, user.ALERT_FIRES, user.GENERAL_NOTES,
        user.THEMES, user.ETF_GROUPS,
    )
}
NAMES = sorted(STORES) + ["screener_feedback"]


def _load(name: str):
    if name == "screener_feedback":
        return user.load_feedback()
    store = STORES[name]
    return user.load_dict(store) if store.key else user.load_list(store)


def _save(name: str, data) -> None:
    if name == "screener_feedback":
        return user.save_feedback(data)
    store = STORES[name]
    if store.key:
        if not isinstance(data, dict):
            raise SystemExit(f"{name}: expected a JSON object")
        user.save_dict(store, data)
    else:
        if not isinstance(data, list):
            raise SystemExit(f"{name}: expected a JSON array")
        user.save_list(store, data)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=("export", "import"))
    p.add_argument("store", choices=NAMES)
    args = p.parse_args()
    if args.action == "export":
        json.dump(_load(args.store), sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
    else:
        _save(args.store, json.load(sys.stdin))
        print(f"app.{args.store}: replaced", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
