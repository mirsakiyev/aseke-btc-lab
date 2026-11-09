from pathlib import Path

import pandas as pd

from lab.backtest import Simulation, simulate
from lab.config import Execution, Profile
from lab.execution import execution_open, plan_setups
from lab.phase5.design import LABEL, checked_completion
from lab.storage import file_hash, read_json, utc, utc_now, write_json


def read_account(equity: Path, trades: Path, metadata: dict) -> Simulation:
    return Simulation(metadata, pd.read_parquet(equity), pd.read_parquet(trades), pd.DataFrame())


def originals(phase3: Path, phase4: Path, prior3: dict, prior4: dict) -> dict[str, Simulation]:
    directory = phase3 / "reference"
    reference = prior3["scenarios"]["reference"]["methods"]["baseline"]
    accounts = {"reference": read_account(directory / "baseline-equity.parquet",
                                           directory / "baseline-trades.parquet",
                                           {"status": reference["observed_engine_status"],
                                            "open_position": reference["account_open_or_unresolved_position"]})}
    for key, name in (("fixed_ledger", "fixed_ledger"), ("lower_fee", "execution_replay")):
        result = prior4["comparison"]["aggregate"][name]
        accounts[key] = read_account(phase4 / f"{name}-equity.parquet", phase4 / f"{name}-trades.parquet",
                                     {"status": result["account_status"],
                                      "open_position": prior4["replay_open_or_unresolved_position"]
                                      if key == "lower_fee" else reference["account_open_or_unresolved_position"]})
    return accounts


def validate_timing(sim: Simulation, profile: Profile, costs: Execution, end: pd.Timestamp) -> None:
    for row in sim.trades.itertuples():
        available = row.signal_at + pd.Timedelta(seconds=costs.processing_seconds)
        expected = execution_open(available, profile.step, costs.delay_bars)
        if row.available_at != available or row.entry_at != expected or row.entry_at >= end:
            raise ValueError("Executed entry violates the preserved signal and timing boundary")
        if row.deadline > row.entry_at + pd.Timedelta(minutes=profile.max_minutes):
            raise ValueError("Holding deadline exceeds the fixed horizon")
        if pd.notna(row.exit_at) and (row.exit_at > min(row.deadline, end) or row.exit_bar_open >= end):
            raise ValueError("Closed trade crosses a holding or sealed price boundary")


def stress_case(directory, name, manifest, candles, signals, eligible, profile) -> Simulation:
    if name not in manifest["execution_stress_budget"]["cases"]:
        raise ValueError("Unregistered Phase 5 stress")
    path = directory / name
    if (path / "complete.json").exists():
        checked_completion(path)
        return read_account(path / "equity.parquet", path / "trades.parquet", read_json(path / "account.json")["account"])
    if (path / "started.json").exists():
        raise RuntimeError(f"Execution budget already used for {name}; inspect its retained partial evidence")
    costs = Execution(**manifest["costs"][name])
    start, end = (utc(manifest[key]) for key in ("requested_start_utc", "requested_end_utc"))
    setups = [item for item in plan_setups(candles, signals, profile, costs, end) if item.signal_at in eligible.index]
    if [item.signal_at for item in setups] != list(eligible.index):
        raise ValueError("Frozen eligible signal population changed")
    write_json(path / "started.json", {"label": LABEL, "case": name, "costs": costs.model_dump(),
                                       "design_sha256": manifest["design_sha256"], "at_utc": utc_now()})
    try:
        sim = simulate(candles, setups, profile, costs, start, end)
        validate_timing(sim, profile, costs, end)
        if len(sim.trades):
            sim.trades["signal_fold"] = eligible.loc[sim.trades.signal_at, "fold"].to_numpy()
        for filename, values in (("equity.parquet", sim.equity), ("trades.parquet", sim.trades)):
            values.assign(research_scope=LABEL, account_currency="USD", stress=name).to_parquet(path / filename)
        write_json(path / "account.json", {"label": LABEL, "case": name, "account": sim.metrics})
        write_json(path / "complete.json", {"label": LABEL, "files": {
            p.name: file_hash(p) for p in path.iterdir() if p.is_file()
        }})
    except Exception as exc:
        write_json(path / "failure.json", {"label": LABEL, "error": str(exc), "at_utc": utc_now()})
        raise
    return sim


def identity_delta(left: Simulation, right: Simulation, end: pd.Timestamp) -> dict:
    def ids(sim):
        return set(sim.trades.loc[sim.trades.entry_at < end, "signal_at"]) if len(sim.trades) else set()

    prior, alternate = ids(left), ids(right)
    return {"shared_signal_ids": len(prior & alternate),
            "lower_fee_only": sorted(prior - alternate), "stress_only": sorted(alternate - prior)}
