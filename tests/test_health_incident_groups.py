from lab.health import delivery, incidents, state
from lab.storage import utc


NOW = utc("2026-11-13T16:00:00Z")


def _evidence(*, qualified=False, exposure=False):
    return {
        "kucoin": {"lock": True, "launch_id": "current", "futures_due": True,
                   "futures": {"expected": 60, "recent_qualified": qualified,
                               "prices": {"trade": {"last_qualified_close": NOW.isoformat()}}}},
        "phase15": {"id": "phase15", "publication_open": True, "controls": {"analysis": True},
                    "managed": {"at": NOW.isoformat(), "launcher": "scheduled:current", "registration": "phase15",
                                "last_qualified_decision_at": NOW.isoformat() if qualified else None},
                    "books": {"day/fixed": {"state": "PAPER_POSITION_OPEN" if exposure else "FLAT",
                                           "requires_protection": exposure, "last_source_close": NOW.isoformat()}}},
    }


def test_correlated_metrics_share_one_episode_and_protection_alert_remains_independent(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path)
    cfg = {**state.DEFAULTS, "warning_checks": 1, "critical_checks": 1}
    captured = []

    def send(message):
        captured.append(message)
        return delivery.Delivery("DELIVERED", "fixture")

    source_checks = {
        "KuCoin shared observer checkpoint": ("WARNING", "Stale checkpoint", False),
        "KuCoin futures trade coverage": ("WARNING", "Missing minutes", False),
        "KuCoin futures mark receipt delay": ("WARNING", "Late receipts", False),
    }
    epoch = int(NOW.timestamp())
    with state.connect() as db:
        warning = incidents.consolidate(source_checks, _evidence(), NOW)
        state.observe(db, warning, epoch, cfg, episode_only=incidents.GROUPS)
        db.commit()
        state.flush(db, epoch, cfg, send)
        assert len(captured) == 1 and incidents.FUTURES in captured[0]
        assert "Last on-cadence futures closes" in captured[0]
        critical = {**source_checks, "KuCoin futures trade coverage": ("CRITICAL", "Large gap", False)}
        groups = incidents.consolidate(critical, _evidence(exposure=True), NOW)
        assert groups["Phase 15 day/fixed protective monitoring"][0] == "CRITICAL"
        for at in (epoch + 1, epoch + 60, epoch + cfg["reminder_minutes"] * 60 + 2):
            state.observe(db, groups, at, cfg, episode_only=incidents.GROUPS)
            db.commit()
            state.flush(db, at, cfg, send)
        assert sum(incidents.FUTURES in text for text in captured) == 2
        assert any("protective monitoring" in text for text in captured)

        clear = dict.fromkeys(source_checks, (None, "Recovered", False))
        catchup = incidents.consolidate(clear, _evidence(), NOW)
        assert catchup[incidents.FUTURES][0] == "WARNING"
        healthy = incidents.consolidate(clear, _evidence(qualified=True), NOW)
        for at in (epoch + 20_001, epoch + 20_002, epoch + 20_003):
            state.observe(db, healthy, at, cfg, episode_only=incidents.GROUPS)
            db.commit()
            state.flush(db, at, cfg, send)
        assert sum("RECOVERY" in text and incidents.FUTURES in text for text in captured) == 1


def test_legacy_alert_migration_keeps_delivery_history_and_supersedes_unsent_duplicates(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path)
    cfg = {**state.DEFAULTS, "critical_checks": 1}
    epoch = int(NOW.timestamp())
    metrics = {key: ("CRITICAL", "Fixture", False) for key in
               ("KuCoin futures trade coverage", "KuCoin futures mark coverage")}
    with state.connect() as db:
        state.observe(db, metrics, epoch, cfg)
        db.commit()
        state.flush(db, epoch, cfg, lambda _: delivery.Delivery("DELIVERED", "fixture", message_id=42), limit=1)
        groups = incidents.consolidate(metrics, _evidence(), NOW)
        incidents.adopt_legacy(db, groups, epoch + 1)
        incidents.adopt_legacy(db, groups, epoch + 2)
        state.observe(db, groups, epoch + 2, cfg, episode_only=incidents.GROUPS)
        assert db.execute("SELECT status,message_id FROM outbox ORDER BY rowid").fetchall()[0][:] == ("DELIVERED", 42)
        assert dict(db.execute("SELECT status,COUNT(*) FROM outbox GROUP BY status")) == {"DELIVERED": 1, "SUPERSEDED": 1}
        assert [row[0] for row in db.execute("SELECT key FROM incident WHERE recovered IS NULL")] == [incidents.FUTURES]
        assert db.execute("SELECT first FROM incident WHERE key=?", (incidents.FUTURES,)).fetchone()[0] == epoch


def test_native_spot_and_funding_are_separate_from_phase15_futures():
    checks = {"KuCoin Spot hourly coverage": ("CRITICAL", "Hours missing", False),
              "KuCoin funding settlements": ("CRITICAL", "Missing settlement", False)}
    grouped = incidents.consolidate(checks, {}, NOW)
    assert set(grouped) == {incidents.SPOT, "KuCoin funding settlements"}
    assert "Affected paper books: none" in grouped[incidents.SPOT][1]
