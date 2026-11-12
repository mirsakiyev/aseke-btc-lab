from pathlib import Path

import pytest

from lab.strategy_lab import ema_registration
from lab.strategy_lab.evaluator import compile_spec
from lab.strategy_lab.registry import FLAGS, connect, get, inventory, register, source_content


def baseline_spec(profile):
    spot = profile == "position"
    return {
        "strategy_id": f"phase15/{profile}/fixed", "family_id": "breakout_v1",
        "origin": "PHASE15_REGISTERED_PAPER",
        "risk": {"starting_equity": 10000, "risk_fraction": .0025, "notional_cap": .25 if spot else 1,
                 "leverage": 1 if spot else 3, "ordinary_cap": 1 if spot else 5,
                 "liquidation_buffer_atr": 0 if spot else .5},
        "cost": {"taker": .001 if spot else .0006, "adverse_proxy": .0005},
        "execution": {"version": "closed-input-ioc-onebar-v1", "processing_seconds": 30,
                      "entry_limit_atr": .25, "funding_reserve_bps": 10},
        "session": {"timezone": "UTC", "max_hold_minutes": 43200 if spot else 10080},
    }


def test_import_preserves_source_and_frozen_policy_snapshots(tmp_path):
    with connect(tmp_path, write=True) as db:
        baselines = [register(db, baseline_spec(profile)) for profile in ("position", "swing")]
        records = ema_registration.register_ema_pullback(db)
        assert records == ema_registration.register_ema_pullback(db)
        source, position, swing = [record["spec"] for record in records]
        for spec in (source, position, swing):
            compile_spec(spec)
        assert [record["version"] for record in records] == [1, 1, 1]
        assert {record["state"] for record in records} == {"SPECIFIED"}
        assert {row["family_id"] for row in inventory(db)} == {"breakout_v1", ema_registration.FAMILY}
        assert source["recipe"] == position["recipe"] == swing["recipe"]
        assert source["session"]["max_hold_minutes"] is None
        assert source["variant"]["sizing"]["allocation_fraction"] == .10
        assert source["cost"]["taker"] == .0005
        assert position["sides"] == ["long"] and swing["sides"] == ["long", "short"]
        assert position["risk"] == baselines[0]["spec"]["risk"]
        assert swing["risk"] == baselines[1]["spec"]["risk"]
        assert baselines == [get(db, row["hash"]) for row in baselines]
        assert all(record["flags"]["prospective_permission"] == FLAGS["prospective_permission"] for record in records)
        assert db.execute("SELECT COUNT(*) FROM source_blob").fetchone()[0] == 2
        for filename, key in ema_registration.ARTIFACTS:
            assert source_content(db, key) == (ema_registration.SOURCE_DIR / filename).read_bytes()
        assert all(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
                   for table in ("dataset", "job", "proposal"))


def test_import_rejects_changed_source_bytes(tmp_path, monkeypatch):
    replacement = Path(tmp_path) / "artifacts"
    replacement.mkdir()
    filename, _ = ema_registration.ARTIFACTS[0]
    (replacement / filename).write_bytes(b"modified source")
    monkeypatch.setattr(ema_registration, "SOURCE_DIR", replacement)
    with pytest.raises(ValueError, match="source checksum mismatch"), connect(tmp_path, write=True) as db:
        ema_registration.register_ema_pullback(db)
    with connect(tmp_path) as db:
        assert db.execute("SELECT COUNT(*) FROM source_blob").fetchone()[0] == 0
        assert not inventory(db)
