"""Tests for NYSE-anchored Benzinga ingest windows."""

from __future__ import annotations

import unittest
from datetime import date, datetime, time

from market_brief.trading_calendar import (
    ET,
    ANCHOR_TIME,
    anchor_session_date,
    compute_news_window,
    is_trading_day,
    previous_trading_day,
)


class TradingCalendarTests(unittest.TestCase):
    def test_wednesday_before_open_uses_prior_session(self) -> None:
        # Tue 2026-05-26 is Memorial Day (closed); use Wed after a normal Tue.
        run = datetime(2026, 5, 27, 8, 0, tzinfo=ET)
        self.assertEqual(anchor_session_date(run), date(2026, 5, 26))

    def test_wednesday_after_open_uses_current_session(self) -> None:
        run = datetime(2026, 5, 27, 10, 0, tzinfo=ET)
        self.assertEqual(anchor_session_date(run), date(2026, 5, 27))

    def test_monday_before_open_uses_prior_friday(self) -> None:
        run = datetime(2026, 5, 25, 8, 0, tzinfo=ET)  # Mon 8 AM
        self.assertEqual(anchor_session_date(run), date(2026, 5, 22))

    def test_sunday_uses_prior_friday(self) -> None:
        run = datetime(2026, 5, 24, 12, 0, tzinfo=ET)
        self.assertEqual(anchor_session_date(run), date(2026, 5, 22))

    def test_saturday_uses_prior_friday(self) -> None:
        run = datetime(2026, 5, 23, 9, 0, tzinfo=ET)
        self.assertEqual(anchor_session_date(run), date(2026, 5, 22))

    def test_good_friday_is_not_trading_day(self) -> None:
        self.assertFalse(is_trading_day(date(2026, 4, 3)))

    def test_monday_after_july4_weekend_rolls_to_thursday(self) -> None:
        # July 4 2026 Sat (observed Fri Jul 3 closed); Mon Jul 6 8 AM → Thu Jul 2
        run = datetime(2026, 7, 6, 8, 0, tzinfo=ET)
        self.assertEqual(anchor_session_date(run), date(2026, 7, 2))

    def test_window_starts_at_five_am_et(self) -> None:
        win = compute_news_window("2026-05-27")
        self.assertEqual(win.anchor_session, date(2026, 5, 26))
        self.assertEqual(win.start_et.time(), ANCHOR_TIME)
        self.assertEqual(win.start_et.date(), date(2026, 5, 26))

    def test_previous_trading_day_skips_weekend(self) -> None:
        self.assertEqual(previous_trading_day(date(2026, 5, 27)), date(2026, 5, 26))

    def test_tuesday_before_open_skips_memorial_day(self) -> None:
        run = datetime(2026, 5, 26, 8, 0, tzinfo=ET)
        self.assertEqual(anchor_session_date(run), date(2026, 5, 22))


if __name__ == "__main__":
    unittest.main()
