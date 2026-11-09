import argparse
import json
import os

for key in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS", "NUMBA_NUM_THREADS"):
    os.environ[key] = "1"

from lab.phase3.observation import health
from lab.phase4.config import LABEL, REFERENCE, bps_fraction, load_config
from lab.phase4.mexc import API_BASE, BOOK_TICKER, EXCHANGE_INFO, KLINES, PROFILE_INTERVALS
from lab.phase4.protocol import readiness, register_draft
from lab.phase4.sensitivity import OUTPUT, run
from lab.storage import read_json


def emit(record):
    print(json.dumps(record, indent=2, default=str, allow_nan=False))


def main():
    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab Phase 4")
    actions = parser.add_subparsers(dest="action", required=True)
    for name in ("validate", "sensitivity", "report", "register-draft", "study-state", "health"):
        actions.add_parser(name)
    action = parser.parse_args().action
    if action == "health":
        emit(health())
        return
    if action == "report":
        location = OUTPUT / read_json(OUTPUT / "latest-sensitivity.json")["directory"]
        print((location / "SUMMARY.md").read_text(encoding="utf-8"))
        return
    config = load_config()
    if action == "sensitivity":
        print(run(config) / "SUMMARY.md")
    elif action == "register-draft":
        print(register_draft(config))
    else:
        record = {"config_sha256": config.identity, **readiness(config)}
        if action == "validate":
            scenario = config.published.execution_sensitivity(REFERENCE)
            record.update(
                label=LABEL, reference=REFERENCE.model_dump(), published_scenario=scenario.model_dump(),
                published_fee_fraction=bps_fraction(scenario.fee_bps), reference_fee_fraction=bps_fraction(REFERENCE.fee_bps),
                approximate_published_round_trip_friction_fraction=2 * bps_fraction(scenario.fee_bps + scenario.proxy_bps),
                offline_contract={"host": API_BASE, "public_get_paths": [KLINES, EXCHANGE_INFO, BOOK_TICKER],
                                  "profiles": PROFILE_INTERVALS, "transport": "disabled"},
            )
        emit(record)


if __name__ == "__main__":
    main()
