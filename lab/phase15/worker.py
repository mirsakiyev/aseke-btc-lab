from contextlib import closing
from dataclasses import asdict

import pandas as pd

from lab.phase9.ideas import current_rules, timedelta_age
from lab.phase9.reference import APPROVAL, MODE, NAMES, parent_design, reference
from lab.phase10.worker import controls as monitoring_controls, process_state
from lab.phase10.research import work_state
from lab.phase13 import observation, protocol
from lab.phase13.diagnostics import gates_off
from lab.phase14.context import pointer as phase14_pointer
from lab.phase15 import paper
from lab.phase15.catalog import QUARTER, ROOT, STEP, controls, db_open, registered, rules
from lab.phase15.feeds import capture_spot, futures_window, spot_bars, swing_trails
from lab.phase15.opportunities import analyze, current_inputs
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


def opportunity_rows(record=None, states=None, now=None):
    now = now if now is not None else utc_now()
    if record:
        body = record["body"]
    else:
        cfg, parent = reference(), parent_design()
        body = {"lanes": rules(cfg, parent), "config": cfg, "holdout_end_exclusive": parent["design"]["holdout"]["end_exclusive"]}
    metadata = protocol.current_metadata()
    by_profile = {}
    result = {}
    for key, lane in body["lanes"].items():
        name = lane["profile"]
        if name not in by_profile:
            by_profile[name] = current_inputs(lane, body["config"], now)
        cutoff, frame, quote, mark = by_profile[name]
        equity = states[key]["cash"] if states else lane["risk"]["starting_equity"]
        result[key] = analyze(lane, body["config"], frame, cutoff, now, quote, mark, metadata, equity,
                              holdout_end=body["holdout_end_exclusive"])
    return result


def advance(db, book, lane, body, end, now, metadata):
    if not book["cursor"] or book["permanent_block"]:
        return
    start = utc(book["cursor"])
    pending = book["pending"]
    if pending and start >= utc(pending["plan"]["entry_expiry"]) and end >= start:
        paper.expire(db, book, start, now, "Entry window elapsed")
        return
    right = min(end, start+pd.Timedelta(minutes=body["max_batch_minutes"]))
    if right <= start:
        return
    market = lane["mode"]["trade_type"]
    if market == "SPOT":
        trade, corrections = spot_bars(db, start, right)
        mark, funding, flags, schedule = trade, {}, {}, True
    else:
        path_start = utc(book["position"]["entry_at"]) if book["position"] else utc(pending["plan"]["eligible_from"])
        bars, funding, flags, schedule = futures_window(body, start, right, now, path_start)
        trade, mark, corrections = bars["trade"], bars["mark"], set()
    active = book["position"] or pending
    spec = active["plan"]["contract"]
    if metadata.get("as_of") and timedelta_age(metadata["as_of"], now, body["config"]["ideas"]["maximum_metadata_age_hours"]):
        try:
            current = asdict(current_rules(market, metadata, body["config"]))
        except (KeyError, StopIteration, ValueError):
            current = None
        if current and spec != current:
            paper.problem(db, book, "Observed public instrument rules changed while exposure was possible", start, now, permanent=True)
            return
    if not schedule:
        paper.problem(db, book, "Dated public funding schedule is unavailable", start, now)
        return
    trails = swing_trails(lane, start, right, now) if lane["exit"] == "atr_trail_v1" else {}
    for at in pd.date_range(start, right, freq=STEP, inclusive="left"):
        if not book["cursor"]:
            break
        if book["pending"] and at >= utc(book["pending"]["plan"]["entry_expiry"]):
            paper.expire(db, book, at, now, "Entry window elapsed")
            break
        if at in corrections:
            paper.problem(db, book, "Conflicting public Spot minute during an active path", at, now, permanent=True)
            break
        if at in flags:
            reason, permanent = flags[at]
            paper.problem(db, book, reason, at, now, permanent=permanent)
            break
        if not paper.minute(db, book, at, now, trade.get(at), mark.get(at), funding.get(at, ()), trails.get(at)):
            if not book["permanent_block"]:
                late = now >= at+STEP+pd.Timedelta(seconds=body["gap_grace_seconds"])
                paper.problem(db, book, "Native price path has a missing or invalid minute" if late else
                              "Waiting for paired completed native minutes", at, now)
            break
    if book["pending"] and book["cursor"] and not book["problem"] and utc(book["cursor"]) >= utc(book["pending"]["plan"]["entry_expiry"]):
        paper.expire(db, book, utc(book["cursor"]), now, "Entry window elapsed")
    if book["cursor"] and end >= utc(body["follow_end_exclusive"]) and utc(book["cursor"]) >= end:
        paper.problem(db, book, "The fixed follow-through horizon ended with exposure unresolved", end, now, permanent=True)


def cycle(launcher="operator", transport=None, *, allow_spot=True, source_cycle=None):
    if not (record := registered()):
        return {"state": "UNREGISTERED"}
    body = record["body"]
    with writer_lock(ROOT / ".writer.lock"), closing(db_open()) as db:
        now = utc_now()
        flags, monitor = controls(), monitoring_controls()
        start = utc(body["start"])
        follow_end = utc(body["follow_end_exclusive"])
        end = min((now-pd.Timedelta(seconds=45)).floor(QUARTER), follow_end)
        books = {key: paper.load(db, lane) for key,lane in body["lanes"].items()}
        spot = {"status": "IDLE"}
        if allow_spot and flags["paper"] and flags["spot_minutes"] and monitor["monitor"] and start < end and now <= follow_end+QUARTER:
            position = books["position/fixed"]
            if (position["cursor"] and not position["permanent_block"]) or not db.execute("SELECT 1 FROM spot_minute LIMIT 1").fetchone():
                left = utc(position["cursor"]) if position["cursor"] else end-QUARTER
                left = max(start, left, end-pd.Timedelta(minutes=body["max_batch_minutes"]))
                if left < end:
                    spot = capture_spot(db, body, left, end, now, transport)
        if flags["paper"] and end > start:
            metadata = protocol.current_metadata()
            for key, lane in body["lanes"].items():
                advance(db, books[key], lane, body, end, utc_now(), metadata)
                paper.save(db, books[key])
        now = utc_now()
        analyses = opportunity_rows(record, books, now) if flags["analysis"] else {}
        published = {}
        if flags["publication"] and flags["paper"] and monitor["monitor"] and start <= now < utc(body["publication_end_exclusive"]):
            for key, analysis in analyses.items():
                cutoff = utc(analysis["source_cutoff"])
                lane = body["lanes"][key]
                if not monitor["ideas"][lane["profile"]] or not start <= cutoff <= now or (
                        now-cutoff).total_seconds() > body["max_live_delay_seconds"]:
                    continue
                if lane["profile"] == "position" and not flags["spot_minutes"]:
                    continue
                if analysis["category"] in {"RISK_REJECTED", "EXECUTION_INFEASIBLE", "ENTRY_EXPIRED", "STOP_INVALIDATED"}:
                    paper.event(db, books[key], "SETUP_REJECTED", "", cutoff, now, {"reason": analysis["reason"]})
                if idea := paper.publish(db, books[key], analysis, now):
                    published[key] = idea["id"]
                paper.save(db, books[key])
        db.commit()
        result = {"state": "RUNNING" if start <= now else "WAITING_FOR_PROSPECTIVE_START", "at": now.isoformat(),
                  "registration": record["id"], "launcher": launcher, "requested_boundary": end.isoformat(), "spot": spot,
                  "shared_capture": {key: source_cycle[key] for key in ("state", "left", "end_exclusive", "received", "recorded_at")
                                     if key in source_cycle} if source_cycle else None,
                  "spot_requests": db.execute("SELECT count(*) FROM spot_request").fetchone()[0], "published": published,
                  "opportunities": {key: {k:v for k,v in value.items() if k != "chart"} for key,value in analyses.items()}}
        write_json(ROOT / "last-cycle.json", result)
        if launcher.startswith("scheduled:"):
            write_json(ROOT / "last-managed.json", result)
        return result


def status(*, with_opportunities=True):
    registration = registered()
    runtime, owned = process_state()
    managed_path = ROOT / "last-managed.json"
    managed = read_json(managed_path) if managed_path.exists() else None
    error_path = ROOT / "managed-error.json"
    error = read_json(error_path) if error_path.exists() else None
    phase14 = phase14_pointer("amendment")
    public = protocol.active()
    pulse = read_json(observation.PULSE) if observation.PULSE.exists() else {}
    flags, monitor = controls(), monitoring_controls()
    queue = work_state()
    now = utc_now()
    heartbeat = (now-utc(runtime["heartbeat"])).total_seconds() if runtime.get("heartbeat") else None
    books, request_count = {}, 0
    if registration and (ROOT / "paper.sqlite3").exists():
        with closing(db_open(readonly=True)) as db:
            books = {key: paper.book_report(db, lane) for key,lane in registration["body"]["lanes"].items()}
            request_count = db.execute("SELECT count(*) FROM spot_request").fetchone()[0]
    elif registration:
        books = {key: paper.initial(lane) for key,lane in registration["body"]["lanes"].items()}
    return {"approval": APPROVAL, "execution_mode": MODE, "historical_estimator_fits": 0,
            "continuous_training_off": gates_off(), "registration": registration, "controls": flags, "worker_controls": monitor,
            "collection": {"shared_enabled": protocol.control()["enabled"], "monitor_enabled": monitor["monitor"],
                           "source_protocol": public["id"] if public else None, "next_due": pulse.get("wake"), "spot_requests": request_count},
            "worker": {"live_and_locked": owned, "launch_id": runtime.get("launch_id"), "pid": runtime.get("pid"),
                       "heartbeat_age_seconds": heartbeat, "managed_phase15_verified": bool(owned and managed and
                           managed["launcher"] == f"scheduled:{runtime.get('launch_id')}" and heartbeat is not None and -5 <= heartbeat < 120
                           and -5 <= (now-utc(managed["at"])).total_seconds() <= 1800
                           and (not error or utc(error["at"]) < utc(managed["at"])))},
            "last_managed": managed, "last_managed_error": error, "books": books,
            "opportunities": opportunity_rows(registration, books, now) if with_opportunities and flags["analysis"] else {},
            "phase14": {"amendment": phase14["id"], "start": phase14["body"]["start"], "review": phase14["body"]["technical_review_due"],
                         "end_exclusive": phase14["body"]["end_exclusive"], "evaluation_manual": True,
                         "evaluation_state": "RESULT_SAVED" if phase14_pointer("evaluation") else
                         "OBSERVATION_HORIZON_OPEN" if now < utc(phase14["body"]["end_exclusive"]) else
                         "AWAITING_MANUAL_QUALIFICATION_AND_EVALUATION"} if phase14 else None,
            "training_queues": {name: {"gate_enabled": monitor["training"][name],
                                "source": "Pinned Spot research result" if name == "position" else "Phase 10 qualified futures bundle queue",
                                "running": queue["current_job"] if (queue["current_job"] or {}).get("profile") == name else None,
                                "waiting": [item for item in queue["waiting_jobs"] if item["profile"] == name]} for name in NAMES}}
