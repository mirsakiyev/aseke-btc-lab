from pathlib import Path

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.phase11 import archive, campaign, qualify
from lab.phase11.plan import sources
from lab.storage import file_hash, utc, write_json


def test_archive_windows_and_cutoff_requests():
    right = utc("2025-11-08T00:00Z")
    specs = sources(right)
    assert len(specs) == 237 and len({item["key"] for item in specs}) == 237
    assert all(utc(row["end"]) <= right for row in specs)
    assert sum(row["kind"] == "fundingRate" for row in specs) == 74
    assert len([row for row in specs if row["kind"] == "fundingREST"]) == 1
    assert specs[-1]["params"]["endTime"] == right.value // 10**6 - 1
    assert (right - utc("2024-07-01")) // pd.Timedelta("1min") == 712800
    assert (right - utc(specs[0]["start"])) // pd.Timedelta("1min") == 3254400


def test_archive_and_trial_clocks_persist_with_request_count(tmp_path, monkeypatch):
    start = utc("2026-11-09T17:00Z")
    original = {"body": {"acquisition": {"max_get_requests": 38, "requests_per_batch": 19,
                                         "batch_wall_seconds": 140, "batch_bytes": 1000, "min_spacing_seconds": 0}}}
    monkeypatch.setattr(archive, "utc_now", lambda: start)
    clock = archive.acquisition_clock(original, tmp_path / "archive")
    trial = campaign.start_clock(tmp_path / "trial", 7200, start)
    later = start + pd.Timedelta(seconds=90)
    monkeypatch.setattr(archive, "utc_now", lambda: later)
    assert archive.acquisition_clock(original, tmp_path / "archive") == clock
    assert campaign.start_clock(tmp_path / "trial", 7200, later) == trial
    assert utc(trial["deadline"]) - utc(trial["started_at"]) == pd.Timedelta("2h")
    path = tmp_path / "archive" / "requests"
    for i in range(38):
        write_json(path / f"{i:04}.json", {"sent": start.isoformat()})
    with pytest.raises(archive.Restricted, match="HTTP request allowance"):
        archive.Download(original, tmp_path / "archive", 2).get(None, "never called")
    with pytest.raises(archive.Restricted, match="batch ceiling"):
        archive.Download(original, tmp_path / "archive", 5).get(None, "never called")


def test_replay_budget_reservations_are_durable_and_bounded(tmp_path):
    campaign.reserve(tmp_path, "kernel_visits", ["first", 1], 30, 50)
    with pytest.raises(campaign.BudgetExhausted, match="already consumed"):
        campaign.reserve(tmp_path, "kernel_visits", ["first", 1], 30, 50)
    with pytest.raises(campaign.BudgetExhausted, match="ceiling"):
        campaign.reserve(tmp_path, "kernel_visits", ["second", 1], 21, 50)
    campaign.reserve(tmp_path, "kernel_visits", ["second", 1], 20, 50)
    campaign.reserve(tmp_path, "estimator_fits", ["trial", "y2025", "inner"], 1, 144)
    assert campaign.ledger(tmp_path)["reservations"] == {"kernel_visits": 50, "estimator_fits": 1}
    assert campaign.ledger(tmp_path)["actual_estimator_fits"] == 0


def test_completed_campaign_reuses_result_without_live_admission(tmp_path, monkeypatch):
    monkeypatch.setattr(campaign, "ROOT", tmp_path)
    monkeypatch.setattr(campaign, "admission", lambda _: pytest.fail("A completed campaign reached live admission"))
    body = {"amendment": "synthetic"}
    identifier = fingerprint(body)
    folder = campaign.campaign_folder(identifier)
    write_json(folder / "manifest.json", {"id": identifier, "body": body})
    write_json(folder / "result.json", {"campaign": identifier, "state": "COMPLETE", "actual_estimator_fits": 2})
    assert campaign.resume(identifier) == {"campaign": identifier, "state": "COMPLETE", "actual_estimator_fits": 2,
                                           "reused": True}
    assert not (folder / "deadline.json").exists()


def test_funding_preserves_source_milliseconds_and_zero_rate():
    start, end = utc("2022-06-01"), utc("2022-06-02")
    stamps = pd.to_datetime([start.value // 10**6, (start + pd.Timedelta(hours=8, milliseconds=25)).value // 10**6,
                             (start + pd.Timedelta(hours=16)).value // 10**6], unit="ms", utc=True)
    frame = pd.DataFrame({"settlement_at": stamps, "rate": [.001, 0., -.001], "interval_minutes": [480, 480, 0]})
    qualified, audit = qualify.matching_funding(frame, start, end)
    assert qualified.settlement_at.iloc[1] == stamps[1] and audit["off_grid_events"] == 1
    assert audit["zero_rates"] == 1 and audit["maximum_nominal_offset_ms"] == 25
    with pytest.raises(ValueError, match="spacing gaps"):
        qualify.matching_funding(frame.drop(index=1), start, end)


def test_complete_evaluation_does_not_hide_training_mark_gap(tmp_path, monkeypatch):
    from lab.phase11 import qualify as qc

    monkeypatch.setattr(qc, "ROOT", tmp_path)
    left = utc("2022-07-01")
    boundaries = [left + pd.Timedelta(minutes=i) for i in (0, 2, 3)]
    sources = []
    found = {}
    minutes = pd.date_range(boundaries[0], boundaries[-1], freq="1min", inclusive="left")

    def observation(key, kind, first, end, frame):
        path = tmp_path / f"{key}.parquet"
        frame.to_parquet(path, index=False)
        sources.append({"key": key, "kind": kind, "start": first.isoformat(), "end": end.isoformat()})
        found[key] = {"spec": sources[-1], "file": str(path), "normalized_sha256": file_hash(path), "raw_sha256": key}

    for i, (first, end) in enumerate(zip(boundaries, boundaries[1:])):
        interval = minutes[(minutes >= first) & (minutes < end)]
        observation(f"trade-{i}", "klines", first, end, pd.DataFrame({"open_at": interval}))
        observation(f"mark-{i}", "markPriceKlines", first, end, pd.DataFrame({"open_at": interval[:-1] if i == 0 else interval}))
    observation("2022-07-fundingRate", "fundingRate", boundaries[0], boundaries[-1],
                pd.DataFrame({"settlement_at": minutes, "rate": 0., "interval_minutes": 1}))
    monkeypatch.setattr(qc, "records", lambda _: found)
    body = {"sources": sources, "source": {"venue": "synthetic"},
            "windows": [{"id": "synthetic", "inner_start": boundaries[1].isoformat(), "outer_end": boundaries[2].isoformat()}]}
    guard = {"work_plan": "synthetic", "unique_observation_allowance": {"trade": 3, "mark": 3, "funding": 3}}
    report = qc.qualify({"id": "synthetic", "body": body}, {"id": fingerprint(guard), "body": guard})["body"]
    row = report["folds"]["synthetic"]
    assert row["evaluation_missing_minutes"] == {"trade": 0, "mark": 0}
    assert row["funding"]["rows"] == 3 and not row["eligible"]
    assert "trade observations lack paired mark" in row["issues"][0]
