"""Bind reviewed feature-only history to the original paper account identities."""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3

from lab.config import ROOT as PROJECT, fingerprint
from lab.ema_paper import catalog, source as legacy_source
from lab.kraken_paper import authorization, catalog as kraken_catalog, source as kraken_source
from lab.storage import file_hash, utc, utc_now, writer_lock
from lab.strategy_lab.cross_venue_warmup import POLICY_ID, freeze_proxy_seed, load_proxy_seed
from lab.strategy_lab.market_data import ObservationKind, Purpose, SourceGrant, SourceIdentity


PROTOCOL = "CROSS_VENUE_FEATURE_WARMUP_V1"
ROOT = catalog.ROOT / "proxy"
MANIFEST = PROJECT / "KRAKEN_SPOT_PAPER_MANIFEST.json"
ORIGINAL_HASHES = {
    "day": "81277c8ef5fd6d6b113a7cdd2a272b961f986f94a76a9d432cbd4079fd559ee6",
    "swing": "f41153a53e627ee799940bdf007f84c5d0d142aaa68a929a074555004e3db4d4",
    "position": "50a8b7c23ab5f428568c1d38bb5c8b37d23336b7f369096163429ed48dc3ada1",
}
SCHEMA = """
CREATE TABLE IF NOT EXISTS amendment (
    id TEXT PRIMARY KEY, book TEXT NOT NULL, body TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS amendment_update BEFORE UPDATE ON amendment
    BEGIN SELECT RAISE(ABORT,'immutable EMA source protocol amendment'); END;
CREATE TRIGGER IF NOT EXISTS amendment_delete BEFORE DELETE ON amendment
    BEGIN SELECT RAISE(ABORT,'immutable EMA source protocol amendment'); END;
"""
SOURCE_BLOCKER = "Independent KuCoin forward and execution rights covering Ontario and this book are unverified."


def _pins():
    paths = (
        "lab/ema_paper/proxy_protocol.py",
        "lab/ema_paper/proxy_native.py",
        "lab/ema_paper/proxy_signal.py",
        "lab/strategy_lab/cross_venue_warmup.py",
        "lab/strategy_lab/market_data.py",
        "lab/strategy_lab/technical_features.py",
    )
    return {path: file_hash(PROJECT / path) for path in paths}


def _original(registrations):
    if set(registrations) != set(ORIGINAL_HASHES):
        raise ValueError("All three original EMA registrations are required")
    for profile, row in registrations.items():
        lane = row["body"]
        expected = ("BTC-USDT", "SPOT", 86400) if profile == "position" else (
            "XBTUSDTM", "FUTURES", 900 if profile == "day" else 14400)
        actual = lane["instrument"], lane["market"], lane["execution_clock"]["decision_seconds"]
        if (lane["strategy_hash"] != ORIGINAL_HASHES[profile] or actual != expected
                or lane["risk"]["starting_equity"] != 10000 or not lane["permissions"]["paper_only"]
                or lane["permissions"]["live_orders"] or lane["permissions"]["protected_sources"]):
            raise ValueError(f"The {profile} registration differs from the original EMA paper protocol")
    return registrations


def _load(root, registrations):
    path = root / "protocol.sqlite3"
    if not path.is_file():
        return {}
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        rows = db.execute("SELECT id,book,body FROM amendment ORDER BY rowid").fetchall()
    original = {record["body"]["book_id"]: record for record in registrations.values()}
    result = {}
    for identifier, book, raw in rows:
        body = json.loads(raw)
        record = original.get(book)
        if (fingerprint(body) != identifier or record is None or body["book_id"] != book
                or body["registration_id"] != record["id"] or body["protocol"] != PROTOCOL
                or body["strategy_hash"] != record["body"]["strategy_hash"]):
            raise ValueError("EMA proxy protocol record failed its identity or checksum verification")
        profile = record["body"]["profile"]
        previous = result.get(profile)
        if (body["predecessor_amendment_id"] != (previous["id"] if previous else None)
                or previous and utc(body["recorded_at"]) <= utc(previous["body"]["recorded_at"])):
            raise ValueError("EMA protocol history must form a chronological append-only chain per account")
        seed = load_proxy_seed(body["seed"], body["seed_id"])
        grant = dict(body["warmup_grant"])
        source = SourceIdentity(**grant.pop("source"))
        scope = SourceGrant(source=source, **grant)
        if (seed.book_id != book or scope.book_id != book or scope.purpose != Purpose.WARMUP
                or scope.id != body["warmup_grant_id"] or seed.grant_id != scope.id
                or seed.source_id != source.id or seed.content_id != body["archive_content_id"]):
            raise ValueError("EMA proxy seed differs from its account or feature-only grant")
        result[profile] = {"id": identifier, "body": body}
    if result and set(result) != set(registrations):
        raise ValueError("EMA proxy amendments must cover the three original accounts atomically")
    return result


def _archive(profile, entry, archive_root, observed):
    frozen = entry["frozen_seed"]
    if frozen:
        return frozen, utc(observed)
    candidate = entry["pre_seam_feature_check"]
    digest = candidate["artifact_sha256"]
    artifact = json.loads((archive_root / "preflight" / f"{digest}.json").read_text(encoding="utf-8"))
    if (fingerprint(artifact) != digest or artifact["profile"] != profile
            or artifact["kind"] != "FEATURE_ONLY_PRE_SEAM_CHECK" or artifact["book_id"] != entry["book_id"]
            or artifact["snapshot_id"] != candidate["source_snapshot_id"]
            or artifact["content_id"] != candidate["content_sha256"]):
        raise ValueError("Archived Kraken feature-only verification failed")
    return candidate, utc(artifact["as_of"])


def _amendments(registrations, archive_root, review, now, previous):
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    consent = authorization.read_review(review, at=now)
    consent_id = authorization.evidence_id(consent)
    if consent_id != manifest["authorization_id"]:
        raise PermissionError("Reviewed Kraken correspondence differs from the archived source authorization")
    with closing(kraken_catalog.db_open(True, root=archive_root)) as db:
        archived = kraken_catalog.registrations(db)
        grants = kraken_catalog.grants(db)
    pins = _pins()
    archive_store = archive_root / "source"
    old_market = kraken_source.market_data(grants, root=archive_store)
    records = []
    for profile, registration in registrations.items():
        lane, old_entry = registration["body"], manifest["books"][profile]
        prior_lane = archived[profile]
        if prior_lane["id"] != old_entry["registration_id"] or prior_lane["body"]["book_id"] != old_entry["book_id"]:
            raise ValueError("Kraken archival manifest does not match the archived registration")
        archive, as_of = _archive(profile, old_entry, archive_root, manifest["observed_at"])
        old_grant = old_market.grant(prior_lane["body"]["source"]["warmup_grant_id"])
        book = lane["book_id"]
        kind, purpose = ObservationKind.TRADE_OHLCV, Purpose.WARMUP
        start, end = utc(archive["earliest_open_utc"]), utc(archive["latest_close_utc"])
        interval = f"{lane['execution_clock']['decision_seconds']}s"
        historic = old_market.read(old_grant.id, old_entry["book_id"], purpose, kind, start, end, as_of, interval)
        if (historic.id != archive["source_snapshot_id"] or historic.content_id != archive["content_sha256"]
                or old_grant.source.id != manifest["source_id"] or old_grant.source.rights_evidence_sha256 != consent_id):
            raise ValueError("Archived Kraken snapshot or original receipt provenance failed verification")
        grant = SourceGrant(old_grant.source, book, purpose, frozenset({kind}), start, end, now,
                            utc(lane["registered_at"]), f"operator-approved-cross-venue-feature-only:{consent_id}")
        market = kraken_source.market_data((grant,), root=archive_store)
        snapshot = market.read(grant.id, book, purpose, kind, start, end, now, interval)
        if (not snapshot.complete or snapshot.content_id != historic.content_id
                or len(snapshot.observations) != archive["bars"]):
            raise ValueError("Original-account Kraken feature read differs from the frozen archive")
        seed = freeze_proxy_seed(snapshot, timeframe=lane["spec"]["timeframe"])
        predecessor = previous.get(profile)
        if predecessor and seed.state_id != predecessor["body"]["seed"]["state_id"]:
            raise ValueError("A runtime revision cannot change the frozen feature state")
        body = {
            "protocol": PROTOCOL, "policy_id": POLICY_ID, "book_id": book, "registration_id": registration["id"],
            "strategy_id": f"ema_pullback_trend_continuation/{profile}", "strategy_hash": lane["strategy_hash"],
            "predecessor_policy": lane["policy"], "predecessor_amendment_id": predecessor["id"] if predecessor else None,
            "recorded_at": now.isoformat(), "code_pins": pins,
            "archive_registration_id": old_entry["registration_id"], "archive_snapshot_id": historic.id,
            "archive_content_id": historic.content_id, "archive_receipt_at": archive["latest_receipt_utc"],
            "warmup_provider": "Kraken", "warmup_instrument": "BTC-USD", "warmup_market": "SPOT",
            "warmup_grant": kraken_catalog._grant_body(grant), "warmup_grant_id": grant.id,
            "forward_provider": "KuCoin", "forward_market": lane["market"], "forward_instrument": lane["instrument"],
            "seed_id": seed.id, "seed": seed.body(), "warmup_count": seed.count,
            "paper_dispatch_authorized": False,
        }
        records.append((fingerprint(body), book, json.dumps(body, sort_keys=True, allow_nan=False)))
    return records


def prepare(review, *, root=ROOT, paper_root=None, archive_root=kraken_catalog.ROOT, append_runtime_revision=False):
    """Freeze the archived Kraken feature seeds without opening an execution ledger for writing."""
    root, archive_root = Path(root), Path(archive_root)
    paper_root = catalog.ROOT if paper_root is None else Path(paper_root)
    registrations = _original(catalog.registrations(root=paper_root))
    with writer_lock(root / ".protocol.lock"):
        previous = _load(root, registrations)
        if append_runtime_revision and not previous:
            raise ValueError("Prepare an initial EMA proxy amendment before appending a runtime revision")
        changed = previous and any(item["body"]["code_pins"] != _pins() for item in previous.values())
        if not previous or (append_runtime_revision and changed):
            with writer_lock(paper_root / ".cycle.lock"):
                with closing(catalog.db_open(True, root=paper_root)) as ledger:
                    if any(ledger.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                           for table in ("event", "eligibility")):
                        raise RuntimeError("A started EMA protocol requires a reviewed successor migration")
                with legacy_source.database(paper_root / "source") as receipts:
                    if receipts and receipts.execute("SELECT 1 FROM grant_scope LIMIT 1").fetchone():
                        raise RuntimeError("A legacy EMA forward grant requires a reviewed successor migration")
                records = _amendments(registrations, archive_root, review, utc_now(), previous)
                with closing(sqlite3.connect(root / "protocol.sqlite3", timeout=3)) as db:
                    db.executescript(SCHEMA)
                    with db:
                        db.executemany("INSERT INTO amendment VALUES (?,?,?)", records)
        return status(root=root, registrations=registrations)


def amended(*, root=ROOT):
    """Guard legacy intake even if the amendment store needs investigation."""
    return (Path(root) / "protocol.sqlite3").is_file()


def get_amendment(profile, *, root=ROOT):
    registrations = catalog.registrations()
    record = _load(Path(root), registrations)[profile]
    if record["body"]["code_pins"] != _pins():
        raise PermissionError("EMA proxy runtime code differs from the prepared amendment")
    return record


def status(*, root=ROOT, registrations=None):
    """Expose feature preparation and execution blockers as separate facts."""
    registrations = catalog.registrations() if registrations is None else registrations
    amendments = _load(Path(root), registrations)
    pins = _pins() if amendments else {}
    result = {}
    for profile, registration in registrations.items():
        lane = registration["body"]
        item = amendments.get(profile, {})
        body = item.get("body", {})
        valid = bool(body and body["code_pins"] == pins and body["policy_id"] == POLICY_ID)
        seed = body.get("seed")
        if seed:
            gain, loss = seed["average_gain"], seed["average_loss"]
            rsi = 50.0 if gain == loss == 0 else 100.0 if loss == 0 else 100.0 - 100.0 / (1.0 + gain / loss)
            indicators = {"ema50": seed["ema_fast"], "ema200": seed["ema_slow"], "rsi14": rsi, "atr14": seed["atr"]}
        else:
            indicators = None
        result[profile] = {
            "book_id": lane["book_id"], "registration_id": registration["id"],
            "strategy_id": f"ema_pullback_trend_continuation/{profile}", "strategy_hash": lane["strategy_hash"],
            "protocol": body.get("protocol"), "amendment_id": item.get("id"), "protocol_pins_match": valid,
            "warmup_provider": "Kraken", "warmup_instrument": "BTC-USD",
            "warmup_count": body.get("warmup_count", 0), "warmup_seed": seed, "seed_indicators": indicators,
            "warmup_seed_id": body.get("seed_id"), "archive_snapshot_id": body.get("archive_snapshot_id"),
            "last_warmup_receipt_at": body.get("archive_receipt_at"), "last_native_receipt_at": None,
            "forward_provider": "KuCoin", "forward_instrument": lane["instrument"],
            "native_volume": {"complete": 0, "required": 20}, "native_swing": {"complete": 0, "required": 5},
            "feature_mode": "KRAKEN_PROXY_SEEDED" if valid else "DATA_INCOMPLETE", "source_mode": "SOURCE_BLOCKED",
            "price_basis": None, "transition_at": None, "first_eligible_at": None,
            "transition_warning": "An authorized, gap-free UTC seam and acceptable price basis must be verified before evaluation.",
            "source_permission": SOURCE_BLOCKER, "state": "BLOCKED_PERMISSION",
            "readiness": {"proxy_feature_ready": valid, "native_signal_ready": False, "economic_execution_ready": False},
            "execution_permission": "No approved execution grant or commissioned proxy dispatcher.",
        }
    return result


def health_status():
    return {"books": {profile: {key: item[key] for key in ("book_id", "state", "feature_mode", "source_mode", "readiness")}
                      for profile, item in status().items()}, "paper_dispatch": "BLOCKED_PERMISSION"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("status", help="read original-account proxy preparation and blockers")
    initialize = subcommands.add_parser("prepare", help="verify local archive and append feature-only amendments")
    initialize.add_argument("--kraken-review", required=True, type=Path, help="private reviewed Kraken consent descriptor")
    initialize.add_argument("--append-runtime-revision", action="store_true")
    args = parser.parse_args()
    result = prepare(args.kraken_review, append_runtime_revision=args.append_runtime_revision) if args.command == "prepare" else status()
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
