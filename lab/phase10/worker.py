from datetime import timedelta
import logging
from logging.handlers import RotatingFileHandler
import os
import subprocess
import sys
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import psutil

from lab.config import ROOT, fingerprint
from lab.phase10.observer import Observer
from lab.phase9.data import candles, inspect_batch, market_quote, mark_quote, metadata
from lab.phase9.ideas import FINAL, append_event, compute, events, fresh, observation_state, publish
from lab.phase9.public import INTERVALS, kline, latest, load_payload, plan_requests, send
from lab.phase9.reference import (APPROVAL, MODE, NAMES, controls as phase9_controls, freeze_reference,
                                  parent_design, read_frozen_reference, reference)
from lab.storage import DATA, RUNS, read_json, utc, utc_now, write_json, writer_lock


STATE, OUTPUT = DATA / "phase10", RUNS / "phase10"
CONTROL = STATE / "control.json"
RUNTIME = STATE / "runtime.json"
LOCK = STATE / ".worker.lock"
POLL_SECONDS = 15
TIMELY_SECONDS = 120
DELAYED_SECONDS = 300
MAX_REQUESTS_PER_CYCLE = 20
MAX_RAW_BYTES = 96 * 1024**2
WAITING = "WAITING_FOR_QUALIFIED_FUTURES_BUNDLE"
LOG = logging.getLogger("lab.phase10")


def optional(path, fallback):
    return read_json(path) if path.exists() else fallback


def controls():
    old = phase9_controls()
    default = {"paused": True, "monitor": old["monitor"], "ideas": old["ideas"],
               "training": dict.fromkeys(NAMES, False), "timezone": "America/Toronto"}
    return optional(CONTROL, default)


def set_control(key, value, profile=None):
    if key == "timezone":
        ZoneInfo(value)
    elif type(value) is not bool or key not in ("paused", "monitor", "ideas", "training"):
        raise ValueError("Unknown control or value")
    if key == "training" and profile == "position" and value:
        raise ValueError("Position uses the saved Phase 9 Spot campaign. Phase 10 queues futures only.")
    with writer_lock(STATE / ".control.lock"):
        flags = controls()
        if key in ("ideas", "training"):
            if profile not in NAMES:
                raise ValueError("Select a profile")
            flags[key][profile] = value
        else:
            flags[key] = value
        write_json(CONTROL, flags)
    return flags


def process_state():
    row = optional(RUNTIME, {})
    if not row.get("pid"):
        return row, False
    try:
        proc = psutil.Process(row["pid"])
        live = proc.create_time() == row["created"]
    except (OSError, psutil.Error):
        live = False
    try:
        with writer_lock(LOCK):
            owned = False
    except RuntimeError:
        owned = True
    return row, live and owned


def cadence(config, parent):
    profiles = parent["design"]["settings"]["profiles"]
    return {name: pd.Timedelta(profiles[item["parent"]]["decision"]) for name, item in config["profiles"].items()}


def boundary(now, step, grace):
    return (now - pd.Timedelta(seconds=grace)).floor(step)


def timing(now, close):
    delay = (now - close).total_seconds()
    return "TIMELY" if delay <= TIMELY_SECONDS else "DELAYED" if delay <= DELAYED_SECONDS else "CATCH_UP"


def decision_key(name, close, strategy):
    return fingerprint([name, close.isoformat(), strategy])


def cache_read(name):
    path = STATE / "candles" / f"{name}.parquet"
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def cache_save(name, frame):
    path = STATE / "candles" / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{uuid4().hex}.tmp")
    try:
        frame.to_parquet(temporary, index=False)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def missing_requests(name, mode, frame, close, budget):
    step = pd.Timedelta(INTERVALS[mode["warmup_interval"]])
    left = close - mode["warmup_bars"] * step
    needed = pd.date_range(left, close, freq=step, inclusive="left")
    have = pd.DatetimeIndex(frame.open_at) if not frame.empty else pd.DatetimeIndex([], tz="UTC")
    missing = needed.difference(have)
    # Fetch the newest gaps first, then warmup gaps, without pulling already cached bars.
    spans = []
    for at in missing:
        if spans and spans[-1][1] == at:
            spans[-1] = (spans[-1][0], at + step)
        else:
            spans.append((at, at + step))
    max_bars = budget["spot_page_bars" if mode["trade_type"] == "SPOT" else "futures_page_bars"]
    width = step * max_bars
    specs = []
    for start, end in reversed(spans):
        while end > start:
            page = max(start, end - width)
            specs.append(kline(name, mode["symbol"], mode["trade_type"], "TRADE", mode["warmup_interval"], page, end))
            end = page
    return specs


def merged_cache(name, old, new, spec, received, sha, close, mode):
    if new.empty:
        return old
    new = new.assign(received_at=received, raw_sha256=sha, receipt_class=spec)
    # First receipt wins. Later evidence remains in the raw log.
    frame = pd.concat([old, new], ignore_index=True).drop_duplicates("open_at", keep="first").sort_values("open_at")
    step = pd.Timedelta(INTERVALS[mode["warmup_interval"]])
    frame = frame.loc[frame.open_at >= close - (mode["warmup_bars"] + 8) * step].reset_index(drop=True)
    cache_save(name, frame)
    return frame


class Worker:
    def __init__(self, runtime, config, parent):
        self.rt, self.cfg, self.parent = runtime, config, parent
        self.phase13_observer = Observer(ROOT)
        self.policy = config["ideas"]
        self.budget = config["acquisition"]
        self.steps = cadence(config, parent)
        self.ref_id = freeze_reference(config)
        self.raw = OUTPUT / "raw"
        self.used = sum(item.stat().st_size for item in self.raw.glob("*") if item.is_file())
        self.frames = {name: cache_read(name) for name in NAMES}
        self.rules = metadata()
        prior_rules = optional(STATE / "metadata.json", {})
        if prior_rules.get("as_of") and utc(prior_rules["as_of"]) > utc(self.rules.get("as_of") or "2000-01-01"):
            self.rules = prior_rules

    def save(self):
        self.rt["heartbeat"] = utc_now().isoformat()
        self.rt["raw_bytes"] = self.used
        write_json(RUNTIME, self.rt)

    def seed(self, now):
        folder = latest("current")
        if not folder or all(not frame.empty for frame in self.frames.values()):
            return
        plan = read_json(folder / "plan.json")["design"]
        if plan["scope"] != "PROSPECTIVE_FEATURE_WARMUP_ONLY":
            return
        pinned = read_frozen_reference(plan["reference_id"])["reference"]["profiles"]
        history, frames, _ = inspect_batch(folder, self.rules)
        for row in history:
            name = row["key"].split("-", 1)[0]
            if name not in NAMES or row["key"] not in frames or not self.frames[name].empty:
                continue
            mode = self.cfg["profiles"][name]
            fields = ("venue", "symbol", "trade_type", "quote", "warmup_interval")
            if any(pinned[name][field] != mode[field] for field in fields):
                continue
            # The first page is only one of several for Day and Swing. Add all pages below.
            for item in history:
                key = item["key"]
                if key.startswith(name + "-") and key in frames:
                    cutoff = boundary(now, self.steps[name], self.policy["completed_candle_grace_seconds"])
                    self.frames[name] = merged_cache(name, self.frames[name], frames[key], "BOOTSTRAP_PHASE9",
                                                     utc(item["received_at"]), item["raw_sha256"], cutoff, mode)

    def fetch(self, client, spec):
        if (self.remaining <= 0 or controls()["paused"] or not controls()["monitor"]
                or (self.rt.get("retry_at") and utc_now() < utc(self.rt["retry_at"]))):
            return None
        if self.used + self.budget["maximum_response_bytes"] > MAX_RAW_BYTES:
            self.rt["last_error"] = {"at": utc_now().isoformat(), "reason": "KuCoin raw evidence disk ceiling reached"}
            self.rt["retry_at"] = (utc_now() + pd.Timedelta("1h")).isoformat()
            return None
        time.sleep(self.budget["minimum_spacing_seconds"])
        with writer_lock(STATE / ".control.lock"):
            if controls()["paused"]:
                return None
            self.remaining -= 1
            self.rt["active_request"] = {"key": spec["key"], "started_at": utc_now().isoformat()}
            self.save()
        try:
            evidence = send(client, spec, self.budget["maximum_response_bytes"], self.raw)
        finally:
            self.rt["active_request"] = None
        if evidence.get("raw_sha256"):
            path = self.raw / f'{evidence["raw_sha256"]}.json'
            if evidence["raw_sha256"] not in self.seen:
                self.seen.add(evidence["raw_sha256"])
                self.used += path.stat().st_size
        row = {"request": spec, **evidence}
        self.rt["requests"] = self.rt.get("requests", 0) + 1
        if evidence["status"] != "RECEIVED":
            failures = self.rt.get("consecutive_errors", 0) + 1
            self.rt["consecutive_errors"] = failures
            self.rt["last_error"] = row
            wait = 3600 if evidence["status"] == "ACCESS_RESTRICTED" else min(900, 15 * 2**min(failures, 6))
            earliest = utc_now() + pd.Timedelta(seconds=wait)
            self.rt["retry_at"] = max(earliest, utc(row.get("resume_after", earliest))).isoformat()
            self.save()
            return None
        self.rt.update(retry_at=None, consecutive_errors=0)
        return row

    def payload(self, evidence):
        return load_payload(OUTPUT, evidence)

    def refresh_metadata(self, client, now):
        if self.rules.get("as_of") and now - utc(self.rules["as_of"]) < pd.Timedelta("23h"):
            return
        # The Phase 9 parser accepts the same versioned attempt format.
        from lab.storage import immutable_bytes
        folder = OUTPUT / "metadata" / now.strftime("%Y%m%dT%H%M%S")
        for number, spec in enumerate(plan_requests("metadata", self.cfg, now, self.parent["design"]["holdout"])):
            row = self.fetch(client, spec)
            if row is None:
                return
            raw_path = self.raw / f'{row["raw_sha256"]}.json'
            immutable_bytes(folder / "raw", raw_path.read_bytes(), "json")
            write_json(folder / "attempts" / f"{number:02}.json", row)
        rules = metadata(folder)
        if rules["status"] == "OBSERVED":
            self.rules = rules
            write_json(STATE / "metadata.json", rules)

    def prices(self, client, name, close):
        mode = self.cfg["profiles"][name]
        frame = self.frames[name]
        fetched = []
        for spec in missing_requests(name, mode, frame, close, self.budget):
            row = self.fetch(client, spec)
            if row is None:
                break
            received = utc(row["received_at"])
            multiplier = self.rules.get("contract", {}).get("multiplier")
            data = candles(self.payload(row), spec, received, float(multiplier) if multiplier else None)
            if data.empty:
                continue
            receipt = "LIVE_INCREMENT" if len(frame) and data.close_at.max() == close else "CATCH_UP"
            frame = merged_cache(name, frame, data, receipt, received, row["raw_sha256"], close, mode)
            fetched.append({**row, "rows": len(data), "first_open": data.open_at.min().isoformat(),
                            "last_close": data.close_at.max().isoformat(), "receipt_class": receipt})
        self.frames[name] = frame
        if fetched:
            source = fetched[-1]
            cycle = {"profile": name, "at": utc_now().isoformat(), "due_boundary": close.isoformat(),
                     "last_close": frame.close_at.max().isoformat(), "source": fetched,
                     "launch_id": self.rt["launch_id"], "launcher": self.rt["launcher"]}
            write_json(OUTPUT / "cycles" / f'{fingerprint(cycle)}.json', cycle)
            self.rt["profiles"][name].update(checkpoint=cycle["last_close"], last_success=cycle["at"],
                                             last_receipt=source["received_at"], last_cycle=fingerprint(cycle))
            self.save()

    def quotes(self, client, requested):
        quotes = {}
        for spec in plan_requests("quotes", self.cfg, utc_now(), self.parent["design"]["holdout"]):
            key = spec["key"]
            if key not in requested:
                continue
            row = self.fetch(client, spec)
            if row is None:
                break
            value, received = self.payload(row), utc(row["received_at"])
            parsed = (mark_quote(value, received) if key == "perp-mark" else
                      market_quote(value, spec["params"]["symbol"], spec["params"]["tradeType"], received))
            quotes[key] = {**parsed, "raw_sha256": row["raw_sha256"]}
        return quotes

    def existing_decision(self, name, close):
        key = decision_key(name, close, self.policy["version"])
        return OUTPUT / "decisions" / name / f"{key}.json"

    def mirror(self, row, path):
        idea = publish(row["idea"], row["reference_id"]) if row["idea"] is not None else None
        if idea and idea["status"] == "CONDITIONAL":
            self.rt.setdefault("active", {})[idea["idea_id"]] = idea
        name = row["profile"]
        item = self.rt["profiles"][name]
        if last := item.get("boundary"):
            missed = (utc(row["boundary"]) - utc(last)) // self.steps[name] - 1
            if missed > 0:
                item["older_boundaries_missed"] = item.get("older_boundaries_missed", 0) + missed
        item.update(boundary=row["boundary"], decision=row["status"],
            timing=row["timing"], waiting_reason=row["reason"], last_decision=str(path),
            last_idea=idea["idea_id"] if idea else None)

    def recover_publications(self):
        for name in NAMES:
            item = self.rt["profiles"][name]
            last = utc(item.get("boundary", "2000-01-01"))
            paths = (OUTPUT / "decisions" / name).glob("*.json")
            pending = [(read_json(path), path) for path in paths if path.stat().st_mtime >= last.timestamp()]
            for row, path in sorted(pending, key=lambda item: item[0]["boundary"]):
                if utc(row["boundary"]) > last:
                    self.mirror(row, path)

    def record(self, name, close, quotes, flags):
        path = self.existing_decision(name, close)
        now = utc_now()
        if path.exists():
            row = read_json(path)
        else:
            arrival = timing(now, close)
            frame = self.frames[name]
            mode = self.cfg["profiles"][name]
            step = pd.Timedelta(INTERVALS[mode["warmup_interval"]])
            left = close - mode["warmup_bars"] * step
            if not frame.empty:
                frame = frame.loc[frame.open_at.ge(left) & frame.close_at.le(close)]
            spot = name == "position"
            quote = quotes.get("spot-quote" if spot else "perp-quote")
            mark = None if spot else quotes.get("perp-mark")
            ready_at = None if frame.empty else frame.received_at.max().isoformat()
            if arrival == "CATCH_UP" or not flags["ideas"][name]:
                row = {"status": "CATCH_UP_ONLY" if flags["ideas"][name] else "IDEAS_OFF", "idea": None,
                       "timing": "CATCH_UP" if flags["ideas"][name] else "UNKNOWN",
                       "reason": "Decision boundary passed its live window" if flags["ideas"][name] else "Profile ideas disabled"}
            else:
                idea = compute(name, frame, quote, mark, self.rules, self.cfg, self.parent, now,
                               evidence={"phase10_boundary": close.isoformat(), "latest_input_receipt": ready_at,
                                         "candle_raw_sha256": sorted(set(frame.raw_sha256)) if len(frame) else []})
                if idea["source_cutoff"] != close.isoformat():
                    return
                if idea["status"] == "DATA_NOT_READY" and arrival == "TIMELY":
                    self.rt["profiles"][name]["waiting_reason"] = idea["reason"]
                    return
                row = {"status": idea["status"], "reason": idea["reason"], "idea": idea,
                       "timing": "UNKNOWN" if idea["status"] == "DATA_NOT_READY" else arrival}
            row.update(boundary=close.isoformat(), recorded_at=now.isoformat(), profile=name,
                       strategy_version=self.policy["version"], source_receipt=ready_at,
                       model_readiness="MODEL_UNAVAILABLE_RULES_PINNED", launch_id=self.rt["launch_id"],
                       launcher=self.rt["launcher"], reference_id=self.ref_id)
            write_json(path, row)
        self.mirror(row, path)
        self.save()

    def cycle(self, client):
        now = utc_now()
        flags = controls()
        if not flags["monitor"]:
            for entry in self.rt["profiles"].values():
                entry["waiting_reason"] = "MONITORING_OFF"
            self.manage_active({}, now)
            return
        self.remaining = MAX_REQUESTS_PER_CYCLE
        grace = self.policy["completed_candle_grace_seconds"]
        closes = {name: boundary(now, self.steps[name], grace) for name in NAMES}
        todo = []
        for name in ("day", "swing", "position"):
            close = closes[name]
            item = self.rt["profiles"][name]
            item["next_due"] = (close + self.steps[name] + pd.Timedelta(seconds=grace)).isoformat()
            last = item.get("boundary")
            if not last or utc(last) < close:
                todo.append(name)
        for name in todo:
            self.prices(client, name, closes[name])
        active = self.rt.get("active", {})
        wanted = {name for name in todo if flags["ideas"][name] and timing(utc_now(), closes[name]) != "CATCH_UP"}
        wanted |= {idea["profile"] for idea in active.values()}
        keys = {"spot-quote"} if "position" in wanted else set()
        if wanted & {"swing", "day"}:
            keys |= {"perp-quote", "perp-mark"}
        quotes = self.quotes(client, keys)
        for name in todo:
            self.record(name, closes[name], quotes, flags)
        self.manage_active(quotes, utc_now())
        self.refresh_metadata(client, utc_now())

    def admit_research(self):
        from lab.phase10.research import should_run
        flags = controls()
        if flags["paused"] or not any(flags["training"].values()):
            return
        if getattr(self, "supervisor", None) and self.supervisor.poll() is None:
            return
        if job := should_run():
            self.supervisor = subprocess.Popen(
                [sys.executable, "-X", "utf8", "-m", "lab.phase10", "_supervise", job], cwd=ROOT,
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS,
            )

    def observe_registered_research(self):
        from lab.phase13.observation import scheduled_due

        observer = self.phase13_observer
        previous = observer.status["state"]
        with writer_lock(STATE / ".control.lock"):
            if controls()["paused"]:
                return
            observer.tick(self.rt["launch_id"], scheduled_due)
        self.rt["phase13_observer"] = observer.status
        if observer.status["state"] != previous:
            LOG.info("Phase 13 observer %s", observer.status)

    def manage_active(self, quotes, now):
        for key, idea in list(self.rt.get("active", {}).items()):
            market = idea["market"] == "FUTURES"
            quote = quotes.get("perp-quote" if market else "spot-quote")
            mark = quotes.get("perp-mark") if market else None
            if observe_idea(idea, quote, mark, now, self.policy):
                del self.rt["active"][key]


def observe_idea(idea, quote, mark, now, policy):
    history = events(idea["idea_id"])
    by_type = {row["type"]: row for row in history}
    limit, skew = policy["maximum_quote_age_seconds"], policy["maximum_quote_clock_skew_seconds"]
    if not by_type.keys() & FINAL:
        kind = observation_state(idea, quote, mark, now, limit, skew, "ENTRY_CONDITION_OBSERVED" in by_type)
        if kind and kind not in by_type:
            by_type[kind] = append_event(idea, kind, {"quote": quote, "mark": mark, "source": "PHASE10_PUBLIC_QUOTES"}, now)
    if "SIMULATED_OUTCOME" in by_type:
        return True
    fill = by_type.get("SIMULATED_FILL")
    long = idea["direction"] == "LONG"
    direction = 1 if long else -1
    if not fill and "ENTRY_CONDITION_OBSERVED" in by_type:
        observation = by_type["ENTRY_CONDITION_OBSERVED"]
        entry_quote = observation["evidence"]["quote"]
        # Only the worker's own timestamped receipt supports the quote-fill proxy.
        if observation["evidence"].get("source") == "PHASE10_PUBLIC_QUOTES":
            fill = append_event(idea, "SIMULATED_FILL", {"observed_at": observation["recorded_at"],
                                "price": entry_quote["ask" if long else "bid"], "source": "HYPOTHETICAL_QUOTE_PROXY",
                                "quantity_base": idea["paper"]["base_quantity"], "market_event": observation["id"]}, now)
    if not fill:
        return bool(by_type.keys() & FINAL)
    entry = fill["evidence"]
    opened = utc(entry["observed_at"])
    deadline = opened + pd.Timedelta(minutes=idea["time_exit"]["maximum_hold_minutes"])
    if (flat := idea["time_exit"].get("session_flat_minute_utc")) is not None:
        deadline = min(deadline, opened.normalize() + pd.Timedelta(minutes=flat))
    valid = fresh(quote, now, limit, skew) and min(utc(quote["at"]), utc(quote["received_at"])) > opened
    if idea["stop"]["basis"] == "MARK":
        valid = valid and fresh(mark, now, limit, skew) and min(utc(mark["at"]), utc(mark["received_at"])) > opened
    if valid:
        trigger = mark["mark"] if mark else quote["last"]
        reason = ("STOP" if direction * (trigger - idea["stop"]["price"]) <= 0 else
                  "TARGET" if direction * (trigger - idea["take_profit"][0]["price"]) >= 0 else
                  "TIME" if now >= deadline else None)
        if reason:
            price, size = quote["bid" if long else "ask"], entry["quantity_base"]
            gross = direction * (price - entry["price"]) * size
            fees = (price + entry["price"]) * size * idea["cost"]["taker"]
            futures = idea["market"] == "FUTURES"
            append_event(idea, "SIMULATED_OUTCOME", {"reason": reason, "exit_price": price, "gross": gross,
                         "fee_reference": fees, "net": None if futures else gross-fees,
                         "funding": "UNOBSERVED" if futures else "NOT_APPLICABLE", "fill_event": fill["id"],
                         "quote": quote, "mark": mark, "source": "HYPOTHETICAL_QUOTE_PROXY"}, now)
            return True
    return False


def status():
    from lab.learning.state import status as learner_status
    from lab.phase10.research import work_state
    rt, running = process_state()
    flags = controls()
    stamp = utc_now()
    heartbeat = rt.get("heartbeat")
    runtime = "RUNNING" if running and heartbeat and stamp - utc(heartbeat) < pd.Timedelta("2min") else "STALE" if running else "STOPPED"
    legacy = learner_status()
    probe = optional(OUTPUT / "binance-probe" / "result.json", {})
    cfg, parent = reference(), parent_design()
    view = {name: dict(rt.get("profiles", {}).get(name, {})) for name in NAMES}
    for name, step in cadence(cfg, parent).items():
        grace = cfg["ideas"]["completed_candle_grace_seconds"]
        due = boundary(stamp, step, grace) + step + pd.Timedelta(seconds=grace)
        result = view[name].get("decision")
        reason = view[name].get("waiting_reason")
        if not flags["monitor"]:
            reason = "MONITORING_OFF"
        elif not flags["ideas"][name]:
            reason = "IDEAS_OFF"
        elif runtime == "STOPPED":
            reason = "WORKER_STOPPED"
        elif reason == "MONITORING_OFF":
            reason = "Waiting for next boundary"
        view[name].update(next_due_utc=due.isoformat(), next_due_local=due.tz_convert(flags["timezone"]).isoformat(),
                          waiting_reason=reason,
                          input_readiness="READY_AT_LAST_DECISION" if result in ("NO_TRADE", "CONDITIONAL") else
                          "DATA_NOT_READY" if result == "DATA_NOT_READY" else "WAITING_FOR_LIVE_INPUTS",
                          model_readiness="MODEL_UNAVAILABLE_RULES_PINNED",
                          training_readiness=WAITING if name != "position" else "PHASE9_SPOT_RESULT_PINNED")
    counts = dict.fromkeys(("TIMELY", "DELAYED", "CATCH_UP", "UNKNOWN"), 0)
    for path in (OUTPUT / "decisions").glob("*/*.json"):
        value = read_json(path)["timing"]
        counts[value] += 1
    return {"execution_mode": MODE, "validation": APPROVAL, "runtime": runtime, "controls": flags,
            "identity": {key: rt.get(key) for key in ("pid", "created", "launcher", "launch_id", "started_at", "heartbeat")},
            "last_error": rt.get("last_error"), "retry_at": rt.get("retry_at"), "raw_bytes": rt.get("raw_bytes", 0),
            "raw_ceiling_bytes": MAX_RAW_BYTES, "profiles": view, "decision_counts": counts,
            "research": {**work_state(), "reason": probe.get("reason", "Finite source probe pending"),
                         "legacy_mode": legacy["mode"], "phase9_manual_gate": phase9_controls()["training"]}}


def run(managed=False):
    OUTPUT.mkdir(parents=True, exist_ok=True)
    LOG.setLevel(logging.INFO)
    handler = RotatingFileHandler(OUTPUT / "worker.log", maxBytes=1024**2, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(handler)
    try:
        with writer_lock(LOCK):
            activity = STATE / ".activity.lock"
            with writer_lock(activity):
                if controls()["paused"]:
                    return
                now = utc_now()
                rt = optional(RUNTIME, {})
                previous = rt.get("launch_id")
                rt.update(pid=os.getpid(), created=psutil.Process().create_time(), launcher="scheduled" if managed else "local",
                          launch_id=uuid4().hex, started_at=now.isoformat(), previous_launch=previous, stopped_at=None,
                          drain_guard_version=1, active_request=None, worker_stage="STARTING", phase13_observer=None)
                rt.setdefault("profiles", {name: {} for name in NAMES})
                worker = Worker(rt, reference(), parent_design())
                worker.seen = {item.stem for item in worker.raw.glob("*.json")}
                worker.recover_publications()
                worker.save()
            try:
                with httpx.Client(timeout=5, follow_redirects=False) as client:
                    while True:
                        with writer_lock(activity):
                            if controls()["paused"]:
                                break
                            try:
                                rt["worker_stage"] = "COLLECTING"
                                worker.seed(utc_now())
                                worker.cycle(client)
                                worker.admit_research()
                            except (ValueError, KeyError, OSError, RuntimeError) as exc:
                                rt["last_error"] = {"at": utc_now().isoformat(), "reason": str(exc)}
                                rt["retry_at"] = (utc_now() + pd.Timedelta(seconds=30)).isoformat()
                                LOG.exception("KuCoin cycle failed")
                            try:
                                if not controls()["paused"]:
                                    worker.observe_registered_research()
                            except (ValueError, KeyError, OSError, RuntimeError):
                                LOG.exception("Phase 13 observation launch failed")
                            rt["worker_stage"] = "IDLE"
                            worker.save()
                        for _ in range(POLL_SECONDS):
                            if controls()["paused"]:
                                break
                            time.sleep(1)
            finally:
                if not worker.phase13_observer.drain():
                    with writer_lock(activity):
                        rt["worker_stage"] = "DRAINING_OBSERVER"
                        rt["phase13_observer"] = worker.phase13_observer.status
                        worker.save()
                    while not worker.phase13_observer.drain():
                        time.sleep(1)
                with writer_lock(activity):
                    rt["worker_stage"] = "STOPPED"
                    rt["phase13_observer"] = worker.phase13_observer.status
                    rt["stopped_at"] = utc_now().isoformat()
                    worker.save()
                LOG.info("KuCoin worker stopped")
    finally:
        handler.close()
        LOG.removeHandler(handler)


def start_local():
    if process_state()[1]:
        return status()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    set_control("paused", False)
    subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase10", "worker"], cwd=ROOT,
                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                     creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP)
    for _ in range(30):
        if process_state()[1]:
            return status()
        time.sleep(.3)
    raise RuntimeError("KuCoin worker did not acquire its lock")


def _drain(seconds):
    set_control("paused", True)
    deadline = time.monotonic() + seconds
    while True:
        rt = optional(RUNTIME, {})
        worker_owned = True
        observer_owned = None
        try:
            with writer_lock(LOCK):
                worker_owned = False
                with writer_lock(OUTPUT / ".observer-cycle.lock"):
                    return {"state": "STOPPED", "writer_lock_held": False, "observer_lock_held": False, "paused": True}
        except RuntimeError:
            if not worker_owned:
                observer_owned = True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                owner = "Worker" if worker_owned else "Observer"
                return {"state": "BLOCKED", "writer_lock_held": worker_owned, "observer_lock_held": observer_owned,
                        "paused": True, "reason": f"{owner} lock still held at the graceful shutdown deadline",
                        **{field: rt.get(field) for field in ("pid", "launch_id", "heartbeat", "active_request",
                                                            "phase13_observer", "worker_stage")}}
            time.sleep(min(.5, remaining))


def stop_local(*, drain_seconds=None):
    result = _drain(12.5 if drain_seconds is None else drain_seconds)
    if drain_seconds is not None:
        return result
    if result["state"] == "BLOCKED":
        raise RuntimeError("Worker is still draining its current request")
    return status()
