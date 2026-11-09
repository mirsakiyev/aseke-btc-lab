import pandas as pd

from lab.phase2 import store
from lab.phase6.coverage import active_candles, historical_candles


def test_active_coverage_counts_elapsed_closes_and_preserves_unknowns(tmp_path):
    path = tmp_path / "receipts.sqlite3"
    activation = pd.Timestamp("2026-01-01T00:01Z")
    now = pd.Timestamp("2026-01-01T00:31Z")
    with store.connect(path) as db:
        for close_minute, receipt_minute, lookback in ((10, 11, 15), (5, 17, 20), (15, 18, 15),
                                                        (20, 24, 20), (10, 28, 15), (25, 32, 15)):
            close = activation.floor("1h") + pd.Timedelta(minutes=close_minute)
            received = activation.floor("1h") + pd.Timedelta(minutes=receipt_minute)
            start = close - pd.Timedelta(minutes=lookback)
            row = (close - pd.Timedelta("5min"), 1., 2., 1., 2., float(receipt_minute))
            normalized = pd.DataFrame([row], columns=["open_at", "open", "high", "low", "close", "volume"])
            store.commit_response(db, 300, start, close, received, "fixture",
                                  str(receipt_minute).encode(), normalized)
        # The first durable timing evidence starts at 00:11. Closes at 00:05 and
        # 00:10 retain unknown timing, including one later recovered in catch-up.
        report = active_candles(db, 300, activation, now)
        assert report["denominator"] == 5
        assert {key: report[key] for key in ("timely", "late", "catch_up", "missing", "unknown")} == {
            "timely": 0, "late": 1, "catch_up": 1, "missing": 1, "unknown": 2,
        }
        assert report["closed_pending_deadline"] == 1
        assert report["receipt_delay_seconds"]["count"] == 2
        day = historical_candles(db, 300, activation, now)
        assert day["expected"] == 288
        assert day["present"] == 4
        assert day["within_120_seconds"] == 1
