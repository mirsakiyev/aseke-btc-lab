import argparse
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys

from lab.config import PROFILE_IDS, ROOT, load_profiles
from lab.data import TARGET_START, backfill, compare_feeds, coverage, feed, snapshot
from lab.model import dependency_versions, windows
from lab.research import compare_latest, latest_run, run
from lab.storage import DATA, RUNS, put_bytes, read_json, utc, utc_now, write_json


def emit(value: object) -> None:
    print(json.dumps(value, indent=2, default=str, allow_nan=False))


def command_output(args: list[str]) -> str:
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, timeout=20).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        return f"Unavailable ({exc})"


def diagnostics() -> dict:
    disk = shutil.disk_usage(ROOT)
    hardware = command_output([
        "powershell.exe", "-NoProfile", "-Command",
        """$ErrorActionPreference='Stop'
        $cpu = Get-CimInstance Win32_Processor
        $system = Get-CimInstance Win32_ComputerSystem
        [pscustomobject]@{
          cpu = @($cpu | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors)
          ram_bytes = $system.TotalPhysicalMemory
        } | ConvertTo-Json -Compress -Depth 3""",
    ])
    return {
        "captured_at": utc_now(), "hardware": json.loads(hardware) if hardware.startswith("{") else hardware,
        "disk_free_bytes": disk.free, "disk_total_bytes": disk.total, "os": platform.platform(),
        "python": sys.version, "python_path": sys.executable,
        "git": command_output(["git", "--version"]),
        "gpu_rows": command_output([
            "nvidia-smi", "--query-gpu=index,name,memory.total,driver_version", "--format=csv,noheader",
        ]).splitlines(),
        "dependencies": dependency_versions(),
    }


def validate() -> dict:
    rules = windows()
    cutoff = utc(rules.data_cutoff)
    hourly, hourly_record = snapshot(3600)
    fine, fine_record = snapshot(300)
    result = {"profiles": {}, "resolution_comparison": compare_feeds(hourly, fine)}
    for profile in load_profiles().values():
        frame, record = (hourly, hourly_record) if profile.seconds == 3600 else (fine, fine_record)
        folder = feed(profile.seconds)
        conflict = folder / "conflict.json"
        result["profiles"][profile.id] = {
            **coverage(frame, profile.seconds, end=cutoff), "snapshot": record,
            "unpublished_conflicts": read_json(conflict) if conflict.exists() else None,
        }
    return result


def report() -> Path:
    destination = RUNS / "summary.md"
    profiles = load_profiles()
    out = [
        "# ASEKE BTC Lab development results",
        "",
        "Independent hypothetical spot accounts. Returns are marked to observed candle closes.",
        "The locked final holdout was excluded. Paper approval has not been granted.",
        "",
        "| Profile | Run ID | State | Training labels | Eval. labels | Baseline return | Candidate return |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    details = []
    for profile in profiles.values():
        item = latest_run(profile.id)
        if not item:
            out.append(f"| {profile.label} | — | No run | — | — | — | — |")
            continue
        directory, manifest = item
        period = manifest["periods"].get("evaluation", {})
        baseline, candidate = period.get("baseline"), period.get("candidate")
        metrics = [f"{row['net_return']:.2%}" if row else "—" for row in (baseline, candidate)]
        train_samples = manifest.get("training_samples", {}).get("usable", 0)
        test_samples = period.get("samples", {}).get("usable", 0)
        out.append(f"| {profile.label} | {directory.name} | {manifest['readiness']} | "
                   f"{train_samples} | {test_samples} | {metrics[0]} | {metrics[1]} |")
        cov = manifest["coverage"]
        details.extend([
            "", f"## {profile.label}", "",
            f"- {cov['bars']:,} observed execution bars from {cov['first_open']} through {cov['last_close']}. "
            f"{cov['internal_gap_bars']:,} internal missing bars in the frozen target range.",
            f"- {manifest['economic_evidence']}.",
            f"- [Manifest]({profile.id}/{directory.name}/manifest.json)",
        ])
        for label, account in (("Baseline", baseline), ("Candidate", candidate)):
            if account:
                details.append(f"- {label} covered {account['evaluation_start']} through "
                               f"{account['evaluation_end']}, status **{account['status']}**.")
        if manifest.get("blocker"):
            details.append(f"- {manifest['blocker']}.")
    common = compare_latest()
    out.extend(details)
    out.extend(["", "## Common dates", "", "```json", json.dumps(common, indent=2, default=str), "```", ""])
    put_bytes(destination, "\n".join(out).encode("utf-8"))
    write_json(RUNS / "common-dates.json", common)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("diagnostics")
    commands.add_parser("validate")
    commands.add_parser("report")
    download = commands.add_parser("backfill")
    download.add_argument("--seconds", type=int, choices=TARGET_START, required=True)
    download.add_argument("--start")
    download.add_argument("--end")
    download.add_argument("--max-chunks", type=int, default=50)
    for name in ("baseline", "train"):
        research = commands.add_parser(name)
        research.add_argument("profile", choices=(*PROFILE_IDS, "all"))
    args = parser.parse_args()
    if args.command == "diagnostics":
        record = diagnostics()
        write_json(RUNS / "environment.json", record)
        emit(record)
    elif args.command == "validate":
        record = validate()
        write_json(RUNS / "data-validation.json", record)
        emit(record)
    elif args.command == "report":
        print(report())
    elif args.command == "backfill":
        end = args.end or windows().data_cutoff
        emit(backfill(args.seconds, args.start or TARGET_START[args.seconds], end, args.max_chunks))
    else:
        profiles = load_profiles()
        for key in profiles if args.profile == "all" else (args.profile,):
            path = run(profiles[key], train=args.command == "train")
            print(f"{key}: {path}", flush=True)


if __name__ == "__main__":
    main()
