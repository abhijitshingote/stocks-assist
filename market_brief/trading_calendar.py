"""US equity session calendar for Benzinga ingest windows.

News ingest is anchored at **5:00 AM America/New_York** on the relevant
NYSE session day (pre-market through run time), not a rolling 24h UTC window.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
PREMARKET_CUTOFF = time(9, 30)  # regular session open
ANCHOR_TIME = time(5, 0)  # window always starts here on session day

# Good Friday (NYSE closed) through 2030
_GOOD_FRIDAY: dict[int, date] = {
    2024: date(2024, 3, 29),
    2025: date(2025, 4, 18),
    2026: date(2026, 4, 3),
    2027: date(2027, 3, 26),
    2028: date(2028, 4, 14),
    2029: date(2029, 3, 30),
    2030: date(2030, 4, 19),
    2031: date(2031, 4, 4),
}


def _observed(d: date) -> date:
    """Sat/Sun → adjacent Friday/Monday per NYSE observance."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """``weekday`` 0=Mon; ``n`` 1=first occurrence, -1=last in month."""
    if n > 0:
        d = date(year, month, 1)
        while d.weekday() != weekday:
            d += timedelta(days=1)
        return d + timedelta(weeks=n - 1)
    d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


def nyse_holidays(year: int) -> set[date]:
    """Full-day NYSE closures for ``year`` (observed dates)."""
    closed: set[date] = set()
    closed.add(_observed(date(year, 1, 1)))
    closed.add(_nth_weekday(year, 1, 0, 3))  # MLK
    closed.add(_nth_weekday(year, 2, 0, 3))  # Presidents
    if year in _GOOD_FRIDAY:
        closed.add(_GOOD_FRIDAY[year])
    closed.add(_nth_weekday(year, 5, 0, -1))  # Memorial
    closed.add(_observed(date(year, 6, 19)))  # Juneteenth
    closed.add(_observed(date(year, 7, 4)))
    closed.add(_nth_weekday(year, 9, 0, 1))  # Labor
    closed.add(_nth_weekday(year, 11, 3, 4))  # Thanksgiving (4th Thu)
    closed.add(_observed(date(year, 12, 25)))
    return closed


_NYSE_CLOSED: frozenset[date] = frozenset(
    d for y in range(2024, 2032) for d in nyse_holidays(y)
)


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in _NYSE_CLOSED


def previous_trading_day(d: date) -> date:
    """Last NYSE session strictly before calendar day ``d``."""
    cur = d - timedelta(days=1)
    for _ in range(366):
        if is_trading_day(cur):
            return cur
        cur -= timedelta(days=1)
    raise ValueError(f"no trading day found before {d}")


def anchor_session_date(run_at_et: datetime) -> date:
    """Trading day whose 5:00 AM ET anchor starts the news window.

    Rules (all times America/New_York):
    - Weekday before 9:30 → previous trading day
    - Monday before 9:30 → prior Friday (via previous_trading_day)
    - Saturday/Sunday → most recent Friday
    - Weekday at/after 9:30 → current day if trading day, else roll back
    """
    cal = run_at_et.date()
    if cal.weekday() >= 5:
        return previous_trading_day(cal)
    if run_at_et.time() < PREMARKET_CUTOFF:
        return previous_trading_day(cal)
    if is_trading_day(cal):
        return cal
    return previous_trading_day(cal)


@dataclass(frozen=True)
class NewsWindow:
    """Inclusive published-time window for Benzinga pulls."""

    start_utc: datetime
    end_utc: datetime
    anchor_session: date  # trading day whose 5 AM ET opens the window
    run_at_et: datetime
    label: str

    @property
    def start_et(self) -> datetime:
        return self.start_utc.astimezone(ET)

    @property
    def end_et(self) -> datetime:
        return self.end_utc.astimezone(ET)


def resolve_run_instant_et(asof: str | None = None) -> datetime:
    """Wall clock for window rules. ``asof`` backfills use that date at 6:00 AM ET."""
    if asof:
        d = datetime.strptime(asof, "%Y-%m-%d").date()
        return datetime.combine(d, time(6, 0), tzinfo=ET)
    return datetime.now(ET)


def compute_news_window(asof: str | None = None) -> NewsWindow:
    run_et = resolve_run_instant_et(asof)
    session = anchor_session_date(run_et)
    start_et = datetime.combine(session, ANCHOR_TIME, tzinfo=ET)
    end_et = run_et
    if end_et <= start_et:
        end_et = start_et + timedelta(minutes=1)

    start_utc = start_et.astimezone(timezone.utc)
    end_utc = end_et.astimezone(timezone.utc)
    rule = _rule_name(run_et)
    label = (
        f"{start_et.strftime('%Y-%m-%d %H:%M %Z')} → "
        f"{end_et.strftime('%Y-%m-%d %H:%M %Z')} "
        f"(session {session.isoformat()}, rule: {rule})"
    )
    return NewsWindow(
        start_utc=start_utc,
        end_utc=end_utc,
        anchor_session=session,
        run_at_et=run_et,
        label=label,
    )


def _rule_name(run_et: datetime) -> str:
    cal = run_et.date()
    if cal.weekday() >= 5:
        return "weekend → prior Friday 5:00 AM ET"
    if run_et.time() < PREMARKET_CUTOFF:
        if cal.weekday() == 0:
            return "Mon before 9:30 AM ET → prior Friday 5:00 AM ET"
        return "weekday before 9:30 AM ET → prior session 5:00 AM ET"
    if is_trading_day(cal):
        return "weekday after 9:30 AM ET → current session 5:00 AM ET"
    return "holiday → prior session 5:00 AM ET"


def prior_session_for_brief(asof: str | None = None) -> date:
    """Trading session date the brief copy should reference (tape / prompts)."""
    run_et = resolve_run_instant_et(asof)
    return anchor_session_date(run_et)
