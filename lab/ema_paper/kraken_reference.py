"""One authorized Kraken BTC/USD reference source for the original EMA books."""

import argparse
from contextlib import closing
from dataclasses import dataclass
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from time import sleep

import httpx
import pandas as pd
import psutil

from lab.config import ROOT as PROJECT, fingerprint
from lab.kraken_paper import authorization, catalog, managed, source
from lab.learning.control import process_identity
from lab.storage import RUNS, read_json, utc_now, write_json, writer_lock
from lab.strategy_lab.market_data import Purpose, SourceGrant, SourceIdentity
from lab.strategy_lab.spot_sources import KRAKEN_MAX_ROWS, SpotCandleRequest, Venue, inspect_receipt


ROOT = RUNS / "ema_kraken_reference"
INTERVALS = (1, 15, 240, 1440)
POLL_SECONDS = 20
QUALIFICATION = "qualification.json"
PROCESS = "reference-process.json"
HEARTBEAT = "reference-heartbeat.json"
AUTHORIZATION_ID = "b6352954bc1250f7514df23d5ab812bb1e1c908f9f658a80e9c9bac25264c2e5"
BOOK_SCOPE = "kraken-btc-usd-ema-reference-collection"
DEFAULT_REVIEW = (Path.home() / ".codex" / "private" / "kraken-market-data" / "scope-review-2026-11-12.json")


@dataclass(frozen=True, slots=True)
class _Collection:
    source: SourceIdentity
    approved_at: pd.Timestamp
    expires_at: pd.Timestamp | None

    def grant(self, purpose, start, end):
        return SourceGrant(self.source, BOOK_SCOPE, purpose, frozenset({source.TRADE}), start, end,
                           self.approved_at, self.approved_at, self.source.rights_reference)


def _root(root):
    root = Path(root).resolve()
    if root.is_relative_to(catalog.ROOT.resolve()) or catalog.ROOT.resolve().is_relative_to(root):
        raise ValueError("EMA reference source needs a separate store from the retired Kraken paper archive")
    return root


def _permission(review, now):
    record = authorization.read_review(review, at=now)
    if authorization.evidence_id(record) != AUTHORIZATION_ID:
        raise PermissionError("Reference collection requires the verified original Kraken BTC/USD consent")
    expiry = authorization.utc(record["expires_at"]) if record.get("expires_at") else None
    return _Collection(catalog._source(record), authorization.utc(record["operator_approved_at"]), expiry)


def market_data(grants, *, root=ROOT):
    """Open the same private source store through the caller's reviewed book grants."""
    return source.market_data(grants, root=_root(root) / "source")


def instrument_evidence(grants, *, as_of, root=ROOT):
    return source.instrument_evidence(grants, as_of=as_of, root=_root(root) / "source")


def _client():
    return httpx.Client(timeout=6, follow_redirects=False,
                        headers={"User-Agent": "ASEKE-BTC-Lab-EMA-reference/1.0"})


def _instrument_terms():
    # This is the last independently verified identity baseline, not a source of
    # current economic terms. A new public receipt is checked against it.
    archived = catalog.registrations()
    terms = {tuple(item["body"]["instrument_terms"][key] for key in source.TERMS_FIELDS)
             for item in archived.values()}
    if len(terms) != 1:
        raise ValueError("Kraken archival registrations do not share one exact Spot instrument")
    return dict(zip(source.TERMS_FIELDS, terms.pop(), strict=True))


def _fetch(db, scope, grant, request, client, clock, pause, *, audit=False):
    started = source._reserve_request(db, scope.source.id, clock, pause)
    if started is None:
        return source._deferred(db, scope.source.id)
    try:
        if scope.expires_at is not None and started >= scope.expires_at:
            raise PermissionError("Kraken authorization expired before the request")
        receipt = source._response(client, request, started, clock)
        result = source._record(db, receipt, scope, (grant,), clock)
        if not audit:
            return result
        stats = inspect_receipt(receipt)
        tail_start = stats.missing_opens[-1] + request.interval if stats.missing_opens else request.start
        return {**result, "raw_sha256": stats.raw_sha256, "raw_rows": stats.raw_rows,
                "excluded_unfinished": stats.excluded_unfinished, "longest_contiguous": stats.longest_contiguous_run,
                "tail_contiguous": (request.end - tail_start) // request.interval,
                "first_open_at": stats.coverage_start.isoformat() if stats.coverage_start else None,
                "latest_close_at": stats.coverage_end.isoformat() if stats.coverage_end else None,
                "request_started_at": started.isoformat(), "latency_seconds": stats.latency_seconds}
    except (httpx.HTTPError, OverflowError, TypeError, ValueError) as error:
        return source._backoff(db, scope.source.id, clock(), error)


def qualification(*, root=ROOT):
    """Read the immutable summary of the first bounded qualification."""
    path = _root(root) / QUALIFICATION
    if not path.is_file():
        return {}
    report = read_json(path)
    if fingerprint(report["body"]) != report["id"] or report["body"]["authorization_id"] != AUTHORIZATION_ID:
        raise ValueError("Kraken EMA reference qualification checksum or consent differs")
    return report


def scope_identity(*, root=ROOT, as_of=None):
    """Revalidate consent and return the qualified identity for consumer grants."""
    report = qualification(root=root)
    if not report:
        raise PermissionError("Kraken EMA reference history has not been qualified")
    return _qualified_permission(report, utc_now() if as_of is None else as_of).source


def _qualified_permission(report, now):
    evidence = report["body"]
    if authorization.utc(evidence["recorded_at"]) > authorization.utc(now):
        raise PermissionError("Reference history was unavailable at the supplied clock")
    scope = _permission(evidence["review_path"], now)
    if scope.source.id != evidence["source_id"]:
        raise PermissionError("Kraken EMA reference source identity changed")
    return scope


def qualify(review_path=DEFAULT_REVIEW, *, root=ROOT):
    """Capture four bounded public OHLC receipts and verify the latest closed histories."""
    root = _root(root)
    now = utc_now()
    scope = _permission(review_path, now)
    if existing := qualification(root=root):
        return existing
    terms = _instrument_terms()
    with (writer_lock(root / ".qualify.lock"), _client() as client,
          source._writer(root / "source") as db):
        if existing := qualification(root=root):
            return existing
        instrument, _ = source._refresh_instrument(db, scope, terms, client, utc_now, sleep)
        if instrument["state"] != "READY":
            return {"state": instrument["state"], "instrument": instrument}
        windows = {}
        for minutes in INTERVALS:
            step = pd.Timedelta(minutes=minutes)
            end = utc_now().floor(step)
            start = end - KRAKEN_MAX_ROWS * step
            request = SpotCandleRequest(Venue.KRAKEN, minutes, start, end)
            grant = scope.grant(Purpose.WARMUP, start, end)
            windows[str(minutes)] = _fetch(db, scope, grant, request, client, utc_now, sleep, audit=True)
            if windows[str(minutes)]["state"] in {"BACKOFF", "WAITING_RATE_LIMIT"}:
                break
        ready = (set(windows) == {str(i) for i in INTERVALS}
                 and all(window.get("tail_contiguous", 0) >= (1 if minutes == "1" else 200)
                         for minutes, window in windows.items()))
        if not ready:
            return {"state": "WAITING_FOR_DATA", "instrument": instrument, "intervals": windows}
        body = {"schema": "kraken-ema-reference-qualification-v1", "state": "QUALIFIED_HISTORY",
                "source_id": scope.source.id, "authorization_id": AUTHORIZATION_ID, "instrument": instrument,
                "intervals": windows, "recorded_at": utc_now().isoformat(),
                "review_path": str(Path(review_path).resolve())}
        report = {"id": fingerprint(body), "body": body}
        write_json(root / QUALIFICATION, report)
        return report


def _forward_request(db, scope, minutes, at):
    step = pd.Timedelta(minutes=minutes)
    end = at.floor(step)
    prior = db.execute("SELECT boundary,state,recorded FROM checkpoint WHERE stream=? AND minutes=?",
                       (scope.source.id, minutes)).fetchone()
    if prior is not None:
        retry = (end.value == prior[0] and prior[1] == "GAP"
                 and prior[2] + source.GAP_RECHECK.value <= at.value <= end.value + source.TIMELY_CHART.value)
        if end.value <= prior[0] and not retry:
            return None
    last_open = db.execute("SELECT MAX(opened) FROM observation WHERE stream=? AND minutes=?",
                           (scope.source.id, minutes)).fetchone()[0]
    start = min(end - 2 * step, source._stamp(last_open)) if last_open is not None else end - 2 * step
    start = max(start, end - KRAKEN_MAX_ROWS * step, scope.approved_at.ceil(step))
    if start >= end:
        return None
    return SpotCandleRequest(Venue.KRAKEN, minutes, start, end)


def _collect(scope, report, root, client, clock, pause):
    results = {}
    with source._writer(root / "source") as db:
        expected = report["body"]["instrument"]["terms"]
        instrument, fetched = source._refresh_instrument(db, scope, expected, client, clock, pause)
        if fetched or instrument["state"] != "READY":
            results["instrument"] = instrument
        if instrument["state"] != "READY":
            return results
        for minutes in INTERVALS:
            at = authorization.utc(clock())
            if scope.expires_at is not None and at >= scope.expires_at:
                break
            if request := _forward_request(db, scope, minutes, at):
                purpose = Purpose.EXECUTION_EVIDENCE if minutes == 1 else Purpose.FORWARD_DECISION
                grant = scope.grant(purpose, request.start, request.end)
                source._check_budget(root / "source")
                result = _fetch(db, scope, grant, request, client, clock, pause)
                results[str(minutes)] = result
                if result["state"] in {"BACKOFF", "WAITING_RATE_LIMIT"}:
                    break
    return results


def collect_once(*, root=ROOT):
    """Capture due reference observations once, without operating a paper ledger."""
    root = _root(root)
    report = qualification(root=root)
    if not report:
        raise PermissionError("Kraken EMA reference history has not been qualified")
    scope = _qualified_permission(report, utc_now())
    with _client() as client:
        return _collect(scope, report, root, client, utc_now, sleep)


def _arguments(root, qualification_id):
    return ["-m", "lab.ema_paper.kraken_reference", "_produce", "--root", str(root),
            "--qualification", qualification_id]


def status(*, root=ROOT):
    """Read process identity, heartbeat, and committed candle checkpoints."""
    root = _root(root)
    marker = read_json(root / PROCESS) if (root / PROCESS).is_file() else {}
    heartbeat = read_json(root / HEARTBEAT) if (root / HEARTBEAT).is_file() else {}
    process = process_identity(marker.get("pid"), marker.get("created"))
    state = "STOPPED" if marker else "NOT_STARTED"
    if process is not None:
        expected = _arguments(root, marker["qualification_id"])
        try:
            actual = process.cmdline()
            state = "RUNNING" if actual[-len(expected):] == expected else "PROCESS_IDENTITY_MISMATCH"
        except psutil.AccessDenied:
            state = "RUNNING_IDENTITY_UNVERIFIED"
        except psutil.NoSuchProcess:
            process, state = None, "STOPPED"
    return {"state": state, "pid": process.pid if process is not None else None,
            "heartbeat": heartbeat, "evidence": source.status(root=root / "source")}


def ensure(*, root=ROOT):
    """Let the existing watchdog supervise a single hidden collector for all profiles."""
    root = _root(root)
    report = qualification(root=root)
    if not report:
        return {"state": "NOT_QUALIFIED"}
    with writer_lock(root / ".reference-dispatch.lock"):
        now = utc_now()
        _qualified_permission(report, now)
        prior = status(root=root)
        if prior["pid"]:
            return prior
        if reason := managed._resources(root):
            return {"state": "BLOCKED_RESOURCES", "reason": reason}
        child = subprocess.Popen(
            [sys.executable, "-B", "-X", "utf8", *_arguments(root, report["id"])],
            cwd=PROJECT, env={**os.environ, **managed.THREADS}, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS,
        )
        try:
            created = psutil.Process(child.pid).create_time()
        except psutil.NoSuchProcess:
            return {"state": "WAITING_PRODUCER", "reason": "Reference producer exited during launch"}
        write_json(root / PROCESS, {"pid": child.pid, "created": created, "started_at": now.isoformat(),
                                   "qualification_id": report["id"]})
        return {"state": "STARTED", "pid": child.pid}


def _run(root, qualification_id):
    root = _root(root)
    with writer_lock(root / ".reference-process.lock"), _client() as client:
        while True:
            try:
                report = qualification(root=root)
                if not report or report["id"] != qualification_id:
                    raise PermissionError("Reference producer qualification changed")
                scope = _qualified_permission(report, utc_now())
                if reason := managed._resources(root, current=True):
                    outcome = {"state": "BLOCKED_RESOURCES", "reason": reason}
                else:
                    outcome = {"state": "RUNNING", "collection": _collect(scope, report, root, client, utc_now, sleep)}
            except (FileNotFoundError, PermissionError) as exc:
                write_json(root / HEARTBEAT, {"state": "BLOCKED_PERMISSION", "reason": type(exc).__name__,
                                              "at": utc_now().isoformat()})
                return
            except (OSError, RuntimeError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                outcome = {"state": "WAITING_SOURCE", "reason": type(exc).__name__}
            write_json(root / HEARTBEAT, {**outcome, "at": utc_now().isoformat()})
            if outcome["state"] == "BLOCKED_RESOURCES" and "RSS" in outcome["reason"]:
                return
            finished = utc_now()
            next_tick = finished.floor(f"{POLL_SECONDS}s") + pd.Timedelta(seconds=POLL_SECONDS)
            sleep(max(.1, (next_tick - finished).total_seconds()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "qualify", "collect-once", "_produce"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--review", type=Path, default=DEFAULT_REVIEW)
    parser.add_argument("--qualification")
    args = parser.parse_args()
    if args.action == "_produce":
        _run(args.root, args.qualification)
    else:
        if args.action == "qualify":
            report = qualify(args.review, root=args.root)
            if "body" in report:
                report = {**report, "body": {key: value for key, value in report["body"].items() if key != "review_path"}}
        elif args.action == "collect-once":
            report = collect_once(root=args.root)
        else:
            report = status(root=args.root)
        print(json.dumps(report, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
