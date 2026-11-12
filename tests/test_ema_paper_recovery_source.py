from contextlib import closing
from dataclasses import replace

import httpx
import pandas as pd
import pytest

from lab.ema_paper import kraken_reference as reference, recovery_source
from lab.kraken_paper import source
from lab.strategy_lab.market_data import Purpose
from test_ema_paper_kraken_reference import environment  # noqa: F401
from test_kraken_paper_source import MINUTE, SEAM, _handler


def test_repair_preserves_live_checkpoint_and_actual_late_receipt(environment, monkeypatch):
    root, clock, calls, scope = environment
    monkeypatch.setattr(source, "_check_budget", lambda root: None)
    reference.qualify(root=root)
    monkeypatch.setattr(recovery_source, "utc_now", clock)
    step = 15 * MINUTE
    clock.at = SEAM + step + 5 * pd.Timedelta(seconds=1)
    monkeypatch.setattr(reference, "_client",
                        lambda: httpx.Client(transport=httpx.MockTransport(_handler(clock, calls, gap=SEAM))))
    assert reference.collect_once(root=root)["15"]["state"] == "GAP"
    clock.at = SEAM + 2 * step + 5 * pd.Timedelta(seconds=1)
    assert reference.collect_once(root=root)["15"]["state"] == "GAP"
    initial = reference.status(root=root)["evidence"]["checkpoints"]
    monkeypatch.setattr(reference, "_client", lambda: httpx.Client(transport=httpx.MockTransport(_handler(clock, calls))))
    result = recovery_source.repair(SEAM + step, step, now=clock.at, root=root)
    assert result["state"] == "CAPTURED" and result["mode"] == "HISTORICAL_CHART_REPAIR"
    assert result["new_rows"] == 1 and result["complete_rows"] == 1
    assert reference.status(root=root)["evidence"]["checkpoints"] == initial
    before_repeat = len(calls)
    assert recovery_source.repair(SEAM + step, step, root=root)["state"] == "ALREADY_PRESENT"
    assert len(calls) == before_repeat
    assert len(calls) >= 2 and calls[-1][0] - calls[-2][0] >= source.MIN_REQUEST_SPACING
    grant = scope.grant(Purpose.FORWARD_DECISION, SEAM, SEAM + 2 * step)
    data = reference.market_data([grant], root=root)
    prior = data.read(grant.id, grant.book_id, grant.purpose, source.TRADE, SEAM, SEAM + step,
                      SEAM + step + 2 * MINUTE, step)
    assert not prior.complete
    recovered = data.read(grant.id, grant.book_id, grant.purpose, source.TRADE, SEAM, SEAM + step, clock.at, step)
    assert recovered.complete
    assert recovered.observations[0].available_at >= SEAM + 2 * step
    with closing(source._read_connection(root / "source")) as db:
        assert db.execute("SELECT COUNT(*) FROM receipt WHERE id=?", (result["receipt_id"],)).fetchone()[0] == 1


def test_repair_declines_unapproved_future_and_out_of_window_requests(environment, monkeypatch):
    root, clock, calls, _ = environment
    monkeypatch.setattr(source, "_check_budget", lambda root: None)
    reference.qualify(root=root)
    monkeypatch.setattr(recovery_source, "utc_now", clock)
    with pytest.raises(ValueError, match="aligned, already closed"):
        recovery_source.repair(SEAM + 15 * MINUTE, "15min", root=root)
    with pytest.raises(ValueError, match="registered Kraken chart intervals"):
        recovery_source.repair(SEAM, "1min", root=root)
    with pytest.raises(PermissionError, match="precedes"):
        recovery_source.repair(SEAM - pd.Timedelta(days=4), "15min", root=root)
    clock.at = SEAM + pd.Timedelta(days=30)
    assert recovery_source.repair(SEAM + 15 * MINUTE, "15min", root=root)["state"] == "OUTSIDE_KRAKEN_HISTORY"
    assert len(calls) == 5


def test_pending_attempts_bind_consumer_grants_and_fetch_one_with_durable_cooldown(environment, monkeypatch):
    root, clock, calls, scope = environment
    monkeypatch.setattr(source, "_check_budget", lambda root: None)
    monkeypatch.setattr(recovery_source, "utc_now", clock)
    reference.qualify(root=root)
    step = 15 * MINUTE
    first, second = SEAM + step, SEAM + 2 * step
    clock.at = second + MINUTE
    grant = scope.grant(Purpose.FORWARD_DECISION, SEAM, second + pd.Timedelta(days=1))
    result = {"day": {"source_protocol": {"book_id": grant.book_id,
              "chart_recovery": {"missing_closes": [second.isoformat(), first.isoformat()]}}}}
    assert recovery_source.repair_pending(result, [replace(grant, book_id="wrong")], root=root, now=clock.at) == {
        "state": "NO_ELIGIBLE_REPAIR"}
    assert len(calls) == 5

    monkeypatch.setattr(reference, "_client", lambda: httpx.Client(transport=httpx.MockTransport(
        _handler(clock, calls, gap=SEAM))))
    pending = recovery_source.repair_pending(result, [grant], root=root, now=clock.at)
    assert pending["state"] == "GAP" and pending["close_at"] == first.isoformat()
    assert len(calls) == 6
    next_pending = recovery_source.repair_pending(result, [grant], root=root, now=clock.at)
    assert next_pending["state"] == "CAPTURED" and next_pending["close_at"] == second.isoformat()
    assert len(calls) == 7
    assert recovery_source.repair_pending(result, [grant], root=root, now=clock.at)["state"] == "NO_ELIGIBLE_REPAIR"
    assert len(calls) == 7
    clock.at += recovery_source.REPAIR_COOLDOWN
    monkeypatch.setattr(reference, "_client", lambda: httpx.Client(transport=httpx.MockTransport(_handler(clock, calls))))
    assert recovery_source.repair_pending(result, [grant], root=root, now=clock.at)["state"] == "CAPTURED"
    assert len(calls) == 8
