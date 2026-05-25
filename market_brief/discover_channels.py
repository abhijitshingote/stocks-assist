"""One-off Benzinga channel discovery via Polygon (run in backend container).

    docker compose exec backend python -m market_brief.discover_channels
    docker compose exec backend python -m market_brief.discover_channels --apply-config

Probes each channel slug seen in a general feed sample plus configured slugs,
reports article counts, and prints a suggested GENERAL_CHANNEL_FETCHES list.
Use --apply-config to rewrite the tuple block in config.py (review diff after).
"""

from __future__ import annotations

import argparse
import re
from collections import Counter
from market_brief import config
from market_brief.ingest import news_window_for_run
from market_brief.ingest_window import filter_published_window
from market_brief.trading_calendar import NewsWindow

import benzinga_news as bz


def _channels_on_article(raw: dict) -> list[str]:
    ch = raw.get("channels") or []
    out: list[str] = []
    for item in ch:
        if isinstance(item, str):
            out.append(item.strip().lower())
        elif isinstance(item, dict) and item.get("name"):
            out.append(str(item["name"]).strip().lower())
    return out


def _probe_channel(
    channel: str,
    window: NewsWindow,
    limit: int = 50,
) -> tuple[int, int, list[str]]:
    """Return (api_rows, after_filter, sample_ids)."""
    try:
        raw = bz.fetch_benzinga_general(
            limit=limit,
            published_gte=window.start_utc,
            channels=channel,
        )
    except Exception as e:  # noqa: BLE001
        return -1, -1, [f"ERROR: {e}"]
    filtered = filter_published_window(raw, window)
    ids = [str(a.get("benzinga_id")) for a in filtered[:5] if a.get("benzinga_id")]
    return len(raw), len(filtered), ids


def main() -> int:
    p = argparse.ArgumentParser(description="Discover Benzinga channel slugs for ingest")
    p.add_argument(
        "--apply-config",
        action="store_true",
        help="Rewrite GENERAL_CHANNEL_FETCHES in market_brief/config.py",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Per-channel probe limit (default 50)",
    )
    args = p.parse_args()

    window = news_window_for_run()
    print(f"News window: {window.label}\n")

    print("=== General feed (no channel filter) ===")
    try:
        general_raw = bz.fetch_benzinga_general(
            limit=config.GENERAL_NEWS_LIMIT,
            published_gte=window.start_utc,
        )
    except Exception as e:  # noqa: BLE001
        print(f"FAILED: {e}")
        return 1
    general = filter_published_window(general_raw, window)
    print(f"API rows: {len(general_raw)}  after filter_since: {len(general)}")

    tag_counter: Counter[str] = Counter()
    for art in general:
        for ch in _channels_on_article(art):
            tag_counter[ch] += 1

    print(f"\nChannel tags on general sample ({len(tag_counter)} distinct):")
    for ch, n in tag_counter.most_common(40):
        print(f"  {ch}: {n}")

    configured = [c for c, _ in config.GENERAL_CHANNEL_FETCHES]
    candidates = sorted(
        set(tag_counter.keys()) | set(configured) | {"news", "movers", "wiim"},
        key=lambda x: (-tag_counter.get(x, 0), x),
    )

    print("\n=== Per-slug API probe (channels= query param) ===")
    print(f"{'channel':<28} {'api':>5} {'filtered':>9}  note")
    print("-" * 60)

    probe_results: list[tuple[str, int, int]] = []
    for slug in candidates:
        api_n, filt_n, sample = _probe_channel(slug, window, limit=args.limit)
        note = ""
        if api_n < 0:
            note = sample[0] if sample else "error"
        elif filt_n == 0:
            note = "empty (slug invalid or no stories in window)"
        else:
            note = f"ids e.g. {', '.join(sample[:3])}"
        print(f"{slug:<28} {api_n:>5} {filt_n:>9}  {note}")
        if filt_n > 0:
            probe_results.append((slug, filt_n, api_n))

    probe_results.sort(key=lambda x: (-x[1], x[0]))
    suggested = [(slug, min(args.limit, max(25, count))) for slug, count, _ in probe_results]

    print("\n=== Suggested GENERAL_CHANNEL_FETCHES (slug, limit) ===")
    print("# Comment out lines you do not want in config.py")
    for slug, lim in suggested:
        on_sample = tag_counter.get(slug, 0)
        print(f'    ("{slug}", {lim}),  # probe={lim} tags_on_general={on_sample}')

    print("\n=== Currently configured ===")
    for slug, lim in config.GENERAL_CHANNEL_FETCHES:
        api_n, filt_n, _ = _probe_channel(slug, window, limit=lim)
        status = "OK" if filt_n > 0 else ("EMPTY" if api_n == 0 else "ZERO after filter")
        print(f"  {slug} limit={lim} -> api={api_n} filtered={filt_n} [{status}]")

    if args.apply_config:
        config_path = config.PACKAGE_DIR / "config.py"
        text = config_path.read_text(encoding="utf-8")
        block = "GENERAL_CHANNEL_FETCHES: list[tuple[str, int]] = [\n"
        lines = [
            "# Empirical channel pulls (market_brief.discover_channels). Comment out as needed.",
        ]
        for slug, lim in suggested:
            lines.append(f'    ("{slug}", {lim}),')
        lines.append("]")
        new_block = block + "\n".join(lines) + "\n"
        pattern = r"GENERAL_CHANNEL_FETCHES: list\[tuple\[str, int\]\] = \[.*?\]\n"
        if not re.search(pattern, text, flags=re.DOTALL):
            print("Could not find GENERAL_CHANNEL_FETCHES block in config.py")
            return 1
        text = re.sub(pattern, new_block, text, count=1)
        config_path.write_text(text, encoding="utf-8")
        print(f"\nUpdated {config_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
