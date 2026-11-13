"""Group correlated operational findings before they reach the outbox."""

import json

from lab.health.state import get_meta, set_meta
from lab.storage import utc


FUTURES = "KuCoin shared futures and Phase 15 pipeline"
SPOT = "KuCoin native Spot hourly pipeline"
COINBASE = "Coinbase research collector"
FUTURES_BOOKS = ("day/fixed", "swing/fixed", "swing/trail")
POSITION_BOOK = "position/fixed"
GROUPS = {
    COINBASE: ("Coinbase collector task", "Coinbase worker", "Coinbase heartbeat", "Coinbase evidence",
               "Coinbase 5m progression", "Coinbase 1h progression", "Coinbase 5m API failures", "Coinbase 1h API failures"),
    FUTURES: ("KuCoin collector task", "KuCoin worker", "KuCoin heartbeat", "KuCoin persistent API backoff",
              "KuCoin evidence", "KuCoin shared observer checkpoint", "KuCoin shared observer API errors",
              "KuCoin public collection disabled",
              *(f"KuCoin futures {basis} {metric}" for basis in ("trade", "mark")
                for metric in ("progression", "coverage", "receipt delay"))),
    SPOT: ("KuCoin Spot hourly progression", "KuCoin Spot hourly coverage"),
}
RANK = {None: 0, "WARNING": 1, "CRITICAL": 2}


def _severity(rows):
    return max((row[0] for row in rows), key=RANK.get, default=None)


def _fresh_managed(now, kucoin, phase15):
    managed = phase15.get("managed") or {}
    return bool(kucoin.get("lock") and kucoin.get("launch_id") and managed.get("at")
                and managed.get("registration") == phase15.get("id")
                and managed.get("launcher") == f"scheduled:{kucoin['launch_id']}"
                and 0 <= (now - utc(managed["at"])).total_seconds() <= 30 * 60)


def consolidate(findings, evidence, now):
    """Return stable notification groups, keeping protection and funding independent."""
    result = dict(findings)
    coinbase = evidence.get("coinbase", {})
    kucoin = evidence.get("kucoin", {})
    phase15 = evidence.get("phase15", {})
    futures = kucoin.get("futures") or {}
    spot = kucoin.get("spot") or {}
    managed = phase15.get("managed") or {}
    books = phase15.get("books", {})
    fresh_managed = _fresh_managed(now, kucoin, phase15)
    decision_at = managed.get("last_qualified_decision_at")
    qualified_managed = bool(fresh_managed and decision_at and 0 <= (now - utc(decision_at)).total_seconds() <= 30 * 60)
    stale_books = [name.removeprefix("breakout/") for name, status in evidence.get("evaluation_freshness", {}).items()
                   if name.startswith("breakout/") and status.get("state") == "STALE"]
    labels = ", ".join(f"Phase 15 {name}" for name in (*FUTURES_BOOKS, POSITION_BOOK))
    protection = [name for name, book in books.items() if book.get("requires_protection")]
    grouped = {}
    for key, children in GROUPS.items():
        rows = [(name, result.pop(name)) for name in children if name in result]
        if (key == FUTURES and kucoin.get("state") != "PAUSED" and phase15.get("publication_open")
                and phase15.get("controls", {}).get("analysis") and not qualified_managed):
            rows.append(("Managed Phase 15", ("WARNING", "Current managed decision evidence is unverified", False)))
        if key == FUTURES and stale_books:
            rows.append(("Stale decisions " + ", ".join(stale_books), ("WARNING", "Missed qualified decisions", False)))
        if (key == FUTURES and kucoin.get("state") != "PAUSED" and kucoin.get("futures_due")
                and futures.get("expected", 0) >= 15 and not futures.get("recent_qualified")):
            rows.append(("Recent qualified trade and mark", ("WARNING", "The latest completed quarter is unqualified", False)))
        if (key == SPOT and kucoin.get("state") != "PAUSED" and kucoin.get("spot_due")
                and spot.get("expected") and spot.get("last_qualified_close") != spot.get("end_exclusive")):
            rows.append(("Recent qualified Spot hour", ("WARNING", "The latest completed hour is unqualified", False)))
        if not rows:
            continue
        bad = [name for name, row in rows if row[0]]
        severity = _severity(row for _, row in rows)
        immediate = any(row[2] and row[0] == severity for _, row in rows)
        if key == COINBASE:
            feeds = coinbase.get("feeds") or {}
            qualified = ", ".join(f"{seconds}s {item.get('last_observed_close') or 'unverified'}" for seconds, item in feeds.items())
            detail = (f"Last collected candles: {qualified or 'unverified'}. Path: experimental research and shadow collection. "
                      "Affected current paper books: none. Paper entry and protection impact: none.")
        elif key == FUTURES:
            prices = futures.get("prices", {})
            qualified = ", ".join(f"{basis} {prices.get(basis, {}).get('last_qualified_close') or 'unverified'}"
                                  for basis in ("trade", "mark"))
            metrics = ", ".join(f"{basis} missing {view.get('missing', '?')}, max receipt lag {view.get('max_receipt_delay_seconds') or 'unverified'} s"
                                for basis, view in prices.items())
            detail = (f"Last on-cadence futures closes: {qualified}. Last managed receipt: {managed.get('at') or 'unverified'}. "
                      f"Recent metrics: {metrics or 'unverified'}. "
                      f"Books: {labels}. New entries: {'qualification impaired' if severity else 'usual eligibility gates'}. "
                      f"Protection: {'degraded for ' + ', '.join(protection) if severity and protection else 'no active exposure reported' if books and not protection else 'verify per book'}.")
        else:
            detail = (f"Last timely native Spot hour: {spot.get('last_qualified_close') or 'unverified'}. "
                      f"Missing hours: {spot.get('missing', 'unverified')}. Affected paper books: none. "
                      "Phase 17.1 Spot research has its own hourly capture. Paper entry and protection impact: none.")
        if bad:
            detail += "\nChecks: " + ", ".join(bad) + "."
        detail += "\nState: " + ("degraded." if severity else "checks qualified.")
        grouped[key] = (severity, detail, immediate)
    for name, book in books.items():
        exposure = book.get("requires_protection")
        key = f"Phase 15 {name} protective monitoring"
        price_path = book.get("last_source_close")
        price_unverified = not price_path or (now - utc(price_path)).total_seconds() > 30 * 60
        market_unverified = name in FUTURES_BOOKS and not futures.get("recent_qualified")
        degraded = bool(exposure and (not fresh_managed or price_unverified or market_unverified
                                      or book.get("problem") or book.get("permanent_block")))
        if exposure or book.get("state") == "FLAT":
            result[key] = (
                "CRITICAL" if degraded else None,
                f"Last managed receipt: {managed.get('at') or 'unverified'}. "
                f"Book: {name}. New entries: {'qualification impaired' if degraded else 'usual eligibility gates'}. "
                f"Protection: {'degraded with exposure or pending work' if degraded else 'management verified or book flat'}.",
                degraded,
            )
    result.update(grouped)
    return result


def adopt_legacy(db, findings, now):
    """Carry active alerts into a group without replaying or falsely recovering them."""
    for key, children in GROUPS.items():
        if key not in findings:
            continue
        marker = "incident_group_v1:" + key
        slots = ",".join("?" for _ in children)
        if not get_meta(db, marker):
            old = db.execute(f"SELECT * FROM incident WHERE key IN ({slots}) AND recovered IS NULL", children).fetchall()
            if old and not db.execute("SELECT 1 FROM incident WHERE key=?", (key,)).fetchone():
                notified = [row for row in old if row["notified"] is not None]
                opened = max((row["opened"] for row in notified), key=RANK.get, default=None)
                db.execute("INSERT INTO incident VALUES(?,?,?,?,?,0,?,?,?,NULL,?)",
                           (key, 1, min(row["first"] for row in old), now, max(row["failures"] for row in old),
                            _severity((row["severity"],) for row in old), opened,
                            max((row["notified"] for row in notified), default=None), findings[key][1]))
            set_meta(db, marker, "true")
        db.execute(f"UPDATE outbox SET status='SUPERSEDED',detail=? WHERE incident IN ({slots}) "
                   "AND kind IN ('incident','recovery') AND status IN ('PENDING','RETRY')",
                   ("Consolidated under " + key, *children))
        db.execute(f"UPDATE incident SET recovered=?,detail=? WHERE key IN ({slots}) AND recovered IS NULL",
                   (now, "Consolidated under " + key, *children))
