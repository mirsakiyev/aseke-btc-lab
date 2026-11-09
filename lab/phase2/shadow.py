from collections.abc import Callable
from math import isfinite
from pathlib import Path

import pandas as pd
from threadpoolctl import threadpool_limits

from lab.backtest import size_order
from lab.config import ORIGIN, Profile, fingerprint, load_profiles
from lab.data import OHLCV
from lab.execution import Setup, execution_open, known_exit
from lab.features import decisions, feature_columns
from lab.model import load_model
from lab.storage import file_hash, utc, utc_now

from lab.phase2 import store
from lab.phase2.checkpoint import frozen_prices, frozen_runs
from lab.phase2.shadow_ledger import account_view, advance

SHADOW_VERSION = "closed-bar-shadow-1"
OBSERVABILITY_VERSION = "input-provenance-v1"
WAIT_AFTER_CLOSE = pd.Timedelta("40s")


def decision_grid(at: pd.Timestamp, frame: str) -> pd.Timestamp:
    step = pd.Timedelta(frame)
    return ORIGIN + ((at - ORIGIN) // step) * step


def new_account(profile: Profile, at: pd.Timestamp) -> dict:
    following = decision_grid(at, profile.decision)
    if following < at:
        following += pd.Timedelta(profile.decision)
    return {
        "next_decision": following.isoformat(), "starting_cash": profile.starting_cash, "cash": profile.starting_cash,
        "position": None, "pending_entry": None, "unresolved": None, "realized_usd": 0.0, "fees_usd": 0.0,
        "entry_fills": 0, "closed_trades": 0, "reconstructed_or_uncertain_trades": 0,
        "decision_count": 0, "prospective_decision_count": 0, "late_catchup_count": 0, "last_decision": None,
    }


def nominal_open(profile: Profile, close: pd.Timestamp) -> pd.Timestamp:
    return execution_open(close + pd.Timedelta(seconds=profile.costs.processing_seconds),
                          profile.step, profile.costs.delay_bars)


def commit_decision(db, profile, state, close, clock, record, intent=None, action=None) -> None:
    """Persist an intent, then obtain post-commit proof that it was durable before its modeled open."""
    now = clock()
    fill = utc(intent["fill_at"]) if intent is not None else None
    if fill is not None and now >= fill:
        record["outcome"], intent = "missed_execution_open", None
    if intent is not None:
        if action == "entry":
            state["pending_entry"] = intent
        else:
            state["position"]["signal_exit"] = intent
    record["decision_recorded_at"] = now
    record["decision_outcome"] = record.pop("outcome")
    before_open = now < nominal_open(profile, close)
    record["observation_quality"] = "prospective_before_modeled_open" if before_open else "late_catch_up"
    record["intent"] = intent
    state["last_decision"] = {key: record[key] for key in ("decision_close", "decision_recorded_at", "decision_outcome")}
    state["next_decision"] = (close + pd.Timedelta(profile.decision)).isoformat()
    state["decision_count"] += 1
    count_field = "prospective_decision_count" if before_open else "late_catchup_count"
    state[count_field] = state.get(count_field, 0) + 1
    with db:
        store.event(db, profile.id, f"decision:{close.isoformat()}", "decision", now, record)
        store.set_account(db, profile.id, state)
    if intent is None:
        return
    acknowledged = clock()
    with db:
        if acknowledged < fill:
            intent["confirmed_at"] = acknowledged.isoformat()
            kind = "durable_intent_confirmed"
        else:
            if action == "entry":
                state["pending_entry"] = None
            else:
                state["position"]["signal_exit"] = None
                state["position"]["missed_signal_exit"] = True
            kind = "intent_too_late_no_fill"
        store.event(db, profile.id, f"intent-proof:{close.isoformat()}", kind, acknowledged,
                    {"action": action, "decision_close": close, "modeled_fill_at": fill, "intent": intent})
        store.set_account(db, profile.id, state)


class Shadow:
    """Experimental lane only. The artifact, execution policy, and starting accounts are pinned."""

    def __init__(self, boundary: pd.Timestamp, clock: Callable = utc_now):
        self.boundary, self.clock = boundary, clock
        self.observability = {"version": OBSERVABILITY_VERSION, "shadow_code_sha256": file_hash(Path(__file__)),
                              "effective_at_utc": self.clock(), "execution_policy_version": SHADOW_VERSION}
        self.runs = frozen_runs()
        self.profiles = {key: Profile(**run[1]["profile"]) for key, run in self.runs.items()}
        self.predictors, self.base, self.model_issues = {}, {}, {}
        self.base_loaded_at = {}
        self.problem = None
        for key, profile in self.profiles.items():
            manifest = self.runs[key][1]
            metadata = manifest["model"]
            try:
                if manifest["holdout_used"] or utc(metadata["training_cutoff"]) >= utc(manifest["windows"]["holdout_start"]):
                    raise ValueError("Training metadata crosses the sealed holdout")
                self.predictors[key] = load_model(metadata, profile, profile.costs)
            except (OSError, ValueError, TypeError, KeyError) as exc:
                self.model_issues[key] = str(exc)

    def start_if_ready(self, db, public):
        existing = store.get(db, "experiment")
        if existing is not None:
            if (existing["version"] != SHADOW_VERSION or utc(existing["registered_holdout_end"]) != self.boundary
                    or existing["profile_hashes"] != {key: p.identity for key, p in self.profiles.items()}
                    or existing["model_versions"] != self.model_versions()):
                raise ValueError("Existing forward experiment is incompatible with this shadow runner")
            return existing
        if not self.predictors or not public["healthy"]:
            self.problem = "Waiting for a compatible frozen artifact and a fresh public heartbeat"
            return None
        now = self.clock()
        feeds = store.feed_status(db)
        if now < self.boundary or not any(
            key in self.predictors
            and (latest := feeds[str(p.seconds)]["last_observed_close"]) is not None
            and latest >= (now - WAIT_AFTER_CLOSE).floor(p.step)
            for key, p in self.profiles.items()
        ):
            self.problem = "Waiting for post-holdout closed-bar collection"
            return None
        experiment = {
            "observation_start_utc": now.isoformat(), "registered_holdout_end": self.boundary.isoformat(),
            "version": SHADOW_VERSION, "purpose": "experimental_shadow_only", "paper_approved": False,
            "profile_hashes": {key: p.identity for key, p in self.profiles.items()},
            "model_versions": self.model_versions(), "holdout_history_used_for_forward_features": False,
            "holdout_performance_revealed": False, "trained_on_holdout": False,
        }
        with db:
            store.set_meta(db, "experiment", experiment)
            for key, p in self.profiles.items():
                if store.get_account(db, key):
                    raise ValueError("Cannot initialize over an existing shadow account")
                store.set_account(db, key, new_account(p, now))
                store.event(db, key, "activation", "observation_activated", now, experiment)
        return experiment

    def model_versions(self):
        return {key: manifest["model"]["model_version"] for key, (_, manifest) in self.runs.items()}

    def tick(self, db, public) -> None:
        experiment = self.start_if_ready(db, public)
        if experiment is None:
            return
        self.problem = None
        now = self.clock()
        for key, profile in self.profiles.items():
            state = store.get_account(db, key)
            if "prospective_decision_count" not in state:
                previous = store.events(db, key, limit=state["decision_count"], kind="decision")
                on_time = sum(utc(event["recorded_at"]) < utc(event["modeled_fill_at"]) for event in previous)
                state["prospective_decision_count"] = on_time
                state["late_catchup_count"] = state["decision_count"] - on_time
                with db:
                    store.set_account(db, key, state)
            for _ in range(32):
                close = utc(state["next_decision"])
                advance(db, profile, state, self.boundary, min(close, now), now)
                allowance = pd.Timedelta(seconds=profile.costs.processing_seconds)
                if now < close + max(WAIT_AFTER_CLOSE, allowance):
                    break
                if store.checkpoint(db, profile.seconds, self.boundary) < close and now < nominal_open(profile, close):
                    break
                self.observe_decision(db, profile, state, close, experiment, public)

    def inputs(self, db, profile: Profile, close: pd.Timestamp):
        record = self.runs[profile.id][1]["source_snapshot"]
        if profile.seconds not in self.base:
            self.base[profile.seconds] = frozen_prices(record)[[*OHLCV, "close_at"]]
            self.base_loaded_at[profile.seconds] = self.clock()
        base = self.base[profile.seconds]
        forward = store.bars(db, profile.seconds, self.boundary, close)
        candles = pd.concat([base.loc[base.close_at <= self.boundary], forward[[*OHLCV, "close_at"]]])
        # The stored revision watermark plus the immutable base digest reproduce the input view.
        identity = {"historical_snapshot_sha256": record["sha256"],
                    "latest_forward_revision_id": int(forward.revision_id.max()) if len(forward) else None,
                    "forward_revisions_sha256": fingerprint([int(i) for i in forward.revision_id]),
                    "last_observed_close": candles.close_at.max(), "source": record["source"]}
        return candles, identity

    def observe_decision(self, db, profile, state, close, experiment, public):
        if close < max(self.boundary, utc(experiment["observation_start_utc"])):
            raise ValueError("Refusing a shadow decision before its permitted observation window")
        metadata = self.runs[profile.id][1]["model"]
        now, fill = self.clock(), nominal_open(profile, close)
        record = {
            "profile": profile.id, "profile_hash": profile.identity, "profile_version": profile.version,
            "strategy_version": profile.strategy_version, "feature_version": profile.feature_version,
            "target_version": profile.target_version, "model_version": metadata["model_version"],
            "model_sha256": metadata["artifact_sha256"], "model_training_cutoff": metadata["training_cutoff"],
            "decision_close": close, "model_threshold_decimal": metadata["entry_threshold"],
            "modeled_fill_at": fill, "data": {
                "historical_snapshot_sha256": metadata["data_sha256"],
                "scanned_through": store.checkpoint(db, profile.seconds, self.boundary),
            }, "feature_ready": None, "baseline_signal": None,
            "score_decimal": None, "costs": profile.costs.model_dump(),
            "observation_start_utc": experiment["observation_start_utc"],
            "holdout_history_used_for_forward_features": False,
            "holdout_performance_revealed": False, "trained_on_holdout": False,
            "lane": "experimental_shadow_only", "feature_input_delay_seconds": (now - close).total_seconds(),
            "observability": self.observability,
        }
        position = state.get("position")
        if now >= fill and not position:
            record["outcome"] = "late_catch_up_no_intent"
            commit_decision(db, profile, state, close, self.clock, record)
            return
        load_started = self.clock()
        candles, identity = self.inputs(db, profile, close)
        load_finished = self.clock()
        record["data"] = identity
        record["input_load"] = {"started_at": load_started, "finished_at": load_finished,
                                "cached_base_loaded_at": self.base_loaded_at.get(profile.seconds),
                                "first_source_open": candles.index.min(), "last_source_close": identity["last_observed_close"]}
        complete = candles.close_at.max() == close
        if not complete:
            record["outcome"] = "missing_latest_closed_execution_bar"
            if position:
                position["missed_signal_exit"] = True
        else:
            feature = decisions(candles, profile)
            record["holdout_history_used_for_forward_features"] = True
            if not experiment["holdout_history_used_for_forward_features"]:
                with db:
                    experiment["holdout_history_used_for_forward_features"] = True
                    store.set_meta(db, "experiment", experiment)
            if close not in feature.index:
                record["outcome"] = "incomplete_decision_bucket"
                if position:
                    position["missed_signal_exit"] = True
            else:
                row = feature.loc[close]
                record.update(feature_ready=bool(row.ready), baseline_signal=bool(row.enter),
                              decision_exit_signal=bool(row.exit))
                record["feature_gates"] = {
                    "baseline_ready": bool(row.baseline_ready), "ridge_inputs_ready": bool(row.ready),
                    "finite_context_by_frame": {item.frame: isfinite(row[f"context_{item.frame}_spread"])
                                                for item in profile.context},
                }
                self.evaluate_row(db, profile, state, close, fill, metadata, row, record, public)
                return
        commit_decision(db, profile, state, close, self.clock, record)

    def evaluate_row(self, db, profile, state, close, fill, metadata, row, record, public):
        position = state.get("position")
        intent, action = None, None
        now = self.clock()
        if state["unresolved"]:
            reason = "account_has_unresolved_exposure"
        elif position and row.exit:
            if now >= fill:
                position["missed_signal_exit"] = True
                reason = "late_signal_exit_no_intent"
            elif fill >= utc(position["intent"]["time_exit"]):
                reason = "precommitted_time_exit_has_priority"
            elif position.get("signal_exit"):
                reason = "signal_exit_already_pending"
            else:
                intent = {"fill_at": fill.isoformat(), "decision_close": close, "original_entry": position["entry_at"]}
                action, reason = "exit", "signal_exit_intent"
        elif now >= fill:
            reason = "late_catch_up_no_intent"
        else:
            reason, intent = self.entry_gate(profile, state, close, fill, metadata, row, record, public)
            if intent is not None:
                action = "entry"
        record["outcome"] = reason
        commit_decision(db, profile, state, close, self.clock, record, intent, action)

    def entry_gate(self, profile, state, close, fill, metadata, row, record, public) -> tuple:
        if not public["healthy"] or self.clock() - utc(public["received_at"]) >= pd.Timedelta("15s"):
            return "public_heartbeat_stale", None
        if (profile.id not in self.predictors or load_profiles()[profile.id].identity != profile.identity):
            record["model_block"] = self.model_issues.get(profile.id, "Profile changed after the frozen fit")
            return "incompatible_or_missing_frozen_model", None
        if not row.baseline_ready:
            return "baseline_warmup_or_context_gap", None
        if not row.enter:
            return "no_baseline_breakout_or_context_trend", None
        if not row.ready:
            return "missing_model_features", None
        features = row[feature_columns(profile)].astype(float).to_frame().T
        with threadpool_limits(limits=1):
            score = float(self.predictors[profile.id].predict(features)[0])
        record["score_decimal"] = score
        if state["position"] or state["pending_entry"]:
            return "position_occupied_or_order_pending", None
        if profile.session:
            cutoff = fill.normalize() + pd.Timedelta(minutes=profile.session.cutoff_minute_utc)
            if fill >= cutoff or fill.normalize() != close.normalize():
                return "entry_cutoff", None
        if not score > metadata["entry_threshold"]:
            return "nonpositive_net_score", None
        deadline, time_exit, kind = known_exit(fill, profile, profile.costs)
        if time_exit > deadline:
            return "deadline_infeasible", None
        setup = Setup(close, close + pd.Timedelta(seconds=profile.costs.processing_seconds),
                      fill, float(row.close), float(row.stop), deadline)
        cash = state["cash"]
        units, risk = size_order(setup, profile, profile.costs, cash)
        return "entry_intent", {
            "fill_at": fill.isoformat(), "decision_close": close.isoformat(),
            "decision_price": float(row.close), "stop": float(row.stop), "units_btc": units,
            "deadline": deadline.isoformat(), "time_exit": time_exit.isoformat(), "time_reason": kind,
            "risk_budget_usd": cash * profile.risk, "planned_risk_usd": risk,
            "allocation_cap_usd": cash * profile.allocation, "costs": profile.costs.model_dump(),
            "profile_hash": profile.identity, "profile_version": profile.version,
            "exit_range": profile.exit_range, "max_minutes": profile.max_minutes,
            "session": profile.session.model_dump() if profile.session else None,
            "strategy_version": profile.strategy_version, "model_version": metadata["model_version"],
            "model_sha256": metadata["artifact_sha256"],
        }

    def status(self, db):
        experiment = store.get(db, "experiment")
        return {
            "state": "observing_experimentally" if experiment and not self.problem else "blocked_or_waiting",
            "experiment": experiment, "problem": self.problem, "model_issues": self.model_issues,
            "observability": self.observability,
            "accounts": {key: account_view(state) for key in self.profiles
                         if (state := store.get_account(db, key))},
            "approval": "No validated model",
        }
