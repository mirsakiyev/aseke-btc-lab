from datetime import timedelta
import json
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.phase9.reference import APPROVAL, MODE, parent_design, reference
from lab.phase13.protocol import active
from lab.phase13.registry import sealed
from lab.phase14.context import pointer as phase14_pointer
from lab.storage import RUNS, read_json, utc, utc_now, write_json, writer_lock


ROOT = RUNS / "phase15"
STEP = pd.Timedelta(minutes=1)
QUARTER = pd.Timedelta(minutes=15)
POLICY = "prospective-native-minute-paper-v1"
TRAILING = "kucoin-swing-breakout-atr-trail-v1"
CONTROLS = {"analysis": True, "publication": True, "paper": True, "spot_minutes": True}


def encode(value):
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"), allow_nan=False)


def rules(config, parent):
    policy = config["ideas"]
    settings = parent["design"]["settings"]
    result = {}
    for name, mode in config["profiles"].items():
        profile = settings["profiles"][mode["parent"]]
        for suffix, exit_id, version in [("fixed", policy["exit"], policy["version"])] + (
                [("trail", "atr_trail_v1", TRAILING)] if name == "swing" else []):
            result[f"{name}/{suffix}"] = {"profile": name, "mode": mode, "profile_rules": profile,
                "strategy": policy["family"], "strategy_version": version, "exit": exit_id,
                "entry_parameters": settings["strategies"][policy["family"]], "exit_parameters": settings["exits"][exit_id],
                "risk": settings["risk"][profile["risk"]], "execution": settings["execution"],
                "cost": {"id": mode["fee"], **config["fees"][mode["fee"]], **config["fee_evidence"]},
                "model_version": None, "evidence_basis": "RULES_ONLY", "approval": APPROVAL}
    return result


def registered():
    path = ROOT / "registration.json"
    if path.exists():
        identifier = read_json(path)["id"]
        return sealed(ROOT / "registrations" / (identifier + ".json"), identifier)
    return None


def register():
    with writer_lock(ROOT / ".register.lock"):
        if record := registered():
            return record
        original, amendment = active(), phase14_pointer("amendment")
        if not original or not amendment or amendment["body"]["original_protocol"] != original["id"]:
            raise ValueError("Commissioning requires the active KuCoin minute feed and Phase 14 source binding")
        config, parent = reference(), parent_design()
        now = utc_now()
        start = now.floor(QUARTER) + QUARTER
        issue_end = start + timedelta(days=30)
        follow_end = min(issue_end + timedelta(days=31), utc(original["body"]["end_exclusive"]))
        if follow_end < issue_end + timedelta(days=30, hours=2):
            raise ValueError("The shared public feed does not cover the bounded publication and follow-through windows")
        versions = rules(config, parent)
        for key, item in versions.items():
            item["book_id"] = fingerprint([POLICY, key, item, start.isoformat()])
        body = {"policy": POLICY, "registered_at": now.isoformat(), "start": start.isoformat(),
                "publication_end_exclusive": issue_end.isoformat(), "follow_end_exclusive": follow_end.isoformat(),
                "source_protocol": original["id"], "phase14_amendment": amendment["id"], "parent_manifest": parent["id"],
                "holdout_end_exclusive": parent["design"]["holdout"]["end_exclusive"], "config": config,
                "lanes": versions, "approval": APPROVAL, "mode": MODE, "max_live_delay_seconds": 300,
                "max_batch_minutes": 180, "gap_grace_seconds": 2700, "funding_grace_seconds": 7200,
                "spot_budget": {"requests": 6000, "timeout_seconds": 5, "spacing_seconds": .35,
                                "response_bytes": 2*1024**2, "retained_bytes": 96*1024**2, "min_free_bytes": 2*1024**3}}
        identity = fingerprint(body)
        record = {"id": identity, "body": body}
        write_json(ROOT / "registrations" / (identity + ".json"), record)
        write_json(ROOT / "registration.json", {"id": identity})
        return record


def controls():
    path = ROOT / "controls.json"
    return read_json(path) if path.exists() else dict(CONTROLS)


def set_control(name, enabled):
    if name not in CONTROLS or type(enabled) is not bool:
        raise ValueError("Unknown Phase 15 control")
    with writer_lock(ROOT / ".controls.lock"):
        flags = {**controls(), name: enabled}
        write_json(ROOT / "controls.json", flags)
    return flags


def db_open(readonly=False):
    path = ROOT / "paper.sqlite3"
    if readonly:
        return sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript("""
    CREATE TABLE IF NOT EXISTS book (id TEXT PRIMARY KEY, state TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS event (key TEXT PRIMARY KEY, book TEXT NOT NULL, kind TEXT NOT NULL,
        idea TEXT NOT NULL, source_at TEXT NOT NULL, recorded_at TEXT NOT NULL, body TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS book_events ON event(book,kind,recorded_at);
    CREATE TABLE IF NOT EXISTS spot_request (id INTEGER PRIMARY KEY, start TEXT, end TEXT, reserved_at TEXT, receipt TEXT);
    CREATE TABLE IF NOT EXISTS spot_minute (open_at TEXT PRIMARY KEY, body TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS spot_correction (open_at TEXT NOT NULL, sha TEXT NOT NULL, PRIMARY KEY(open_at,sha));
    """)
    return db
