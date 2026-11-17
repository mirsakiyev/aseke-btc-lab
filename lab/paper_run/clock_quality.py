"""Read-only Windows clock observations for the paper clock qualification audit."""

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from ipaddress import ip_address
from itertools import pairwise
import json
from math import isfinite
from pathlib import Path
import re
import subprocess
import time


TARGET_SECONDS = 0.250
MAX_SYNC_AGE_SECONDS = 18_000
STEP_NS = 100_000_000


@dataclass(frozen=True)
class WindowsStatus:
    state: int
    phase_offset_seconds: float
    root_delay_seconds: float
    root_dispersion_seconds: float
    sync_age_seconds: float
    last_successful_sync: str
    source: str
    reference_id: str
    stratum: int
    leap: int
    last_sync_error: int

    @property
    def reported_bound_seconds(self):
        return abs(self.phase_offset_seconds) + self.root_dispersion_seconds + self.root_delay_seconds / 2

    @classmethod
    def parse(cls, output):
        fields = dict(line.split(":", 1) for line in output.splitlines() if ":" in line)
        fields = {key.strip(): value.strip() for key, value in fields.items()}

        def seconds(key):
            match = re.fullmatch(r"([+-]?[0-9]+(?:\.[0-9]+)?)s", fields[key])
            if not match or not isfinite(value := float(match[1])):
                raise ValueError(f"Invalid Windows Time field {key}")
            return value

        def code(key):
            match = re.match(r"[0-9]+(?:\b|$)", fields[key])
            if not match:
                raise ValueError(f"Invalid Windows Time field {key}")
            return int(match[0])

        return cls(code("State Machine"), seconds("Phase Offset"), seconds("Root Delay"),
                   seconds("Root Dispersion"), seconds("Time since Last Good Sync Time"),
                   fields["Last Successful Sync Time"], fields["Source"], fields["ReferenceId"],
                   code("Stratum"), code("Leap Indicator"), code("Last Sync Error"))


@dataclass(frozen=True)
class Anchor:
    wall_ns: int
    monotonic_ns: int


def anchor():
    return Anchor(time.time_ns(), time.monotonic_ns())


def _clock_step(before, after):
    elapsed = after.monotonic_ns - before.monotonic_ns
    return elapsed < 0 or abs(after.wall_ns - before.wall_ns - elapsed) > STEP_NS


def _network_reference(value):
    match = re.fullmatch(r"0x[0-9a-fA-F]{8} \(source IP:\s+([^)]+)\)", value)
    if not match:
        return False
    try:
        address = ip_address(match[1].strip())
    except ValueError:
        return False
    return not (address.is_loopback or address.is_unspecified or address.is_multicast)


def _sample_problem(status):
    if status is None:
        return "WINDOWS_STATUS_UNAVAILABLE"
    if status.sync_age_seconds > MAX_SYNC_AGE_SECONDS:
        return "WINDOWS_SYNC_SAMPLE_EXPIRED"
    if (status.leap != 0 or status.last_sync_error or not 1 <= status.stratum <= 15
            or not status.source or not _network_reference(status.reference_id)
            or not status.last_successful_sync or min(status.sync_age_seconds, status.root_delay_seconds,
                                                      status.root_dispersion_seconds) < 0):
        return "WINDOWS_SYNC_SAMPLE_INVALID"
    if status.state not in (1, 2):
        return "WINDOWS_CLOCK_DISCIPLINE_UNVERIFIED"
    if status.reported_bound_seconds > TARGET_SECONDS:
        return "WINDOWS_REPORTED_BOUND_EXCEEDS_TARGET"
    return None


@dataclass(frozen=True)
class Observation:
    started: Anchor
    finished: Anchor
    windows: WindowsStatus | None
    error: str | None = None


def _sync_window(observation):
    age_ns = round(observation.windows.sync_age_seconds * 1_000_000_000)
    return observation.started.monotonic_ns - age_ns, observation.finished.monotonic_ns - age_ns


def _consistent_sync_timeline(before, after):
    first_start, first_end = _sync_window(before)
    next_start, next_end = _sync_window(after)
    if before.windows.last_successful_sync != after.windows.last_successful_sync:
        return next_start > first_end + STEP_NS
    return next_start <= first_end + STEP_NS and first_start <= next_end + STEP_NS


def assess(observations):
    """Assess this process's live observation series for diagnostics only."""
    observations = tuple(observations)
    points = tuple(point for obs in observations for point in (obs.started, obs.finished))
    if any(_clock_step(first, second) for first, second in pairwise(points)):
        return "CLOCK_STEP_DETECTED", "WALL_AND_MONOTONIC_ELAPSED_DISAGREE"
    if not observations:
        return "CLOCK_UNVERIFIED", "WINDOWS_STATUS_UNAVAILABLE"
    latest = observations[-1].windows
    if problem := _sample_problem(latest):
        state = "CLOCK_STALE" if problem == "WINDOWS_SYNC_SAMPLE_EXPIRED" else "CLOCK_UNVERIFIED"
        return state, problem
    if latest.state == 1:
        return "CLOCK_UNVERIFIED", "HOLD_REQUIRES_INDEPENDENT_CLOCK_EVIDENCE"
    if any(_sample_problem(obs.windows) or obs.windows.state != 2
           or (obs.windows.source, obs.windows.reference_id) != (latest.source, latest.reference_id)
           for obs in observations):
        return "CLOCK_UNVERIFIED", "WINDOWS_SOURCE_OR_DISCIPLINE_NOT_STABLE"
    if any(not _consistent_sync_timeline(before, after) for before, after in pairwise(observations)):
        return "CLOCK_UNVERIFIED", "WINDOWS_SYNC_SAMPLE_TIMELINE_INCONSISTENT"
    if len({obs.windows.last_successful_sync for obs in observations}) < 2:
        return "CLOCK_UNVERIFIED", "NEED_DISTINCT_WINDOWS_SYNC_SAMPLES"
    return "CLOCK_HEALTHY", "WINDOWS_REPORTED_BOUND_WITHIN_TARGET"


def observe(*, read=anchor, query=subprocess.run):
    started = read()
    try:
        result = query(["w32tm", "/query", "/status", "/verbose"], capture_output=True, text=True,
                       timeout=3, check=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        status, error = WindowsStatus.parse(result.stdout), None
    except (OSError, subprocess.SubprocessError, KeyError, TypeError, ValueError) as exc:
        status, error = None, f"{type(exc).__name__}: {exc}"
    return Observation(started, read(), status, error)


def _utc_stamp(point):
    return datetime.fromtimestamp(point.wall_ns / 1e9, timezone.utc).isoformat()


def report(observations):
    observations = tuple(observations)
    state, reason = assess(observations)
    return {"mode": "SHADOW_ONLY", "state": state, "reason": reason, "target_seconds": TARGET_SECONDS,
            "independent_reference_verified": False,
            "observations": [
                {**asdict(obs),
                 "captured_at": _utc_stamp(obs.finished),
                 "windows_reported_bound_seconds": obs.windows.reported_bound_seconds if obs.windows else None}
                for obs in observations]}


# This is an offline review of operator-supplied records. It is not an
# authenticated time source and is never read by the managed runner.
NTP_REFERENCES = ("time-a-g.nist.gov", "time.cloudflare.com", "ntp.netnod.se")
NTP_REVIEW_MAX_AGE_SECONDS = 60


def _ntp_window(path):
    document = json.loads(path.read_text(encoding="utf-8"))
    if (not isinstance(document, dict) or document.get("authenticated") is not False
            or document.get("utc_clock") != "GetSystemTimePreciseAsFileTime"):
        raise ValueError("Expected an unauthenticated precise-clock NTP capture")
    sources = {}
    points = []
    intervals = []
    for source in document["results"]:
        host = source["host"]
        if host not in NTP_REFERENCES:
            continue
        if host in sources:
            raise ValueError(f"Duplicate reference in NTP window: {host}")
        accepted = []
        errors = []
        for sample in source["attempts"]:
            if "error" in sample:
                errors.append(sample["error"])
                continue
            sent = Anchor(sample["sent_wall_ns"], sample["sent_monotonic_ns"])
            received = Anchor(sample["received_wall_ns"], sample["received_monotonic_ns"])
            if _clock_step(sent, received) or received.wall_ns < sent.wall_ns:
                raise ValueError("Clock step within an NTP request")
            points.extend((sent, received))
            offset, uncertainty, rtt, delay, dispersion, age = (
                sample["offset_remote_minus_local_s"], sample["uncertainty_halfwidth_s"], sample["network_rtt_s"],
                sample["server_root_delay_s"], sample["server_root_dispersion_s"], sample["reference_age_s"])
            if not all(isfinite(v) for v in (offset, uncertainty, rtt, delay, dispersion, age)):
                raise ValueError(f"Non-finite timing evidence from {host}")
            if (sample["leap"] != 0 or not 1 <= sample["stratum"] <= 15 or min(rtt, delay, dispersion, age) < 0
                    or age > 3600 or uncertainty < (rtt + delay) / 2 + dispersion):
                raise ValueError(f"Inconsistent timing evidence from {host}")
            accepted.append({"offset_seconds": offset, "uncertainty_seconds": uncertainty,
                             "round_trip_seconds": rtt, "root_delay_seconds": delay,
                             "root_dispersion_seconds": dispersion, "reference_age_seconds": age})
            intervals.append((offset - uncertainty, offset + uncertainty))
        sources[host] = {"accepted": accepted, "errors": errors}
    if not points:
        return {"path": str(path), "state": "NTP_INSUFFICIENT_REFERENCES", "sources": sources}, None, None
    ordered = sorted(points, key=lambda point: point.monotonic_ns)
    start, end = ordered[0], ordered[-1]
    if any(_clock_step(start, point) for point in ordered):
        raise ValueError("Clock step within NTP observation window")
    bound = max(max(abs(low), abs(high)) for low, high in intervals)
    overlap = max(low for low, _ in intervals), min(high for _, high in intervals)
    state = "NTP_CONDITIONAL_AGREEMENT"
    if any(len(sources.get(host, {}).get("accepted", ())) < 3 for host in NTP_REFERENCES):
        state = "NTP_INSUFFICIENT_REFERENCES"
    elif bound > TARGET_SECONDS:
        state = "NTP_CONDITIONAL_BOUND_EXCEEDS_TARGET"
    elif overlap[0] > overlap[1]:
        state = "NTP_REFERENCES_DISAGREE"
    result = {"path": str(path), "state": state, "started_at": _utc_stamp(start), "ended_at": _utc_stamp(end),
              "max_conditional_bound_seconds": bound, "conditional_offset_overlap_seconds": overlap,
              "wall_minus_monotonic_seconds": (end.wall_ns - start.wall_ns - end.monotonic_ns + start.monotonic_ns) / 1e9,
              "sources": sources}
    return result, start, end


def _ntp_review_anchor():
    from ctypes import byref, c_uint64, windll

    # Use the same clock pair as the precise NTP capture. On Python 3.12
    # for Windows, monotonic_ns and perf_counter_ns can have different origins.
    filetime = c_uint64()
    read_wall = windll.kernel32.GetSystemTimePreciseAsFileTime
    before = time.perf_counter_ns()
    read_wall(byref(filetime))
    after = time.perf_counter_ns()
    return Anchor((filetime.value - 116_444_736_000_000_000) * 100, (before + after) // 2)


def summarize_ntp(paths, *, now):
    """Report repeatability without promoting saved or unauthenticated samples to trust."""
    windows = []
    timeline = []
    for path in paths:
        try:
            result, start, end = _ntp_window(path)
        except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
            windows.append({"path": str(path), "state": "NTP_EVIDENCE_INVALID", "error": str(error)})
            continue
        windows.append(result)
        if start is not None:
            timeline.append((start, end))
    timeline.sort(key=lambda pair: pair[0].wall_ns)
    state = "NTP_REPEATABLE_CONDITIONAL_AGREEMENT"
    age = None
    if any(window["state"] != "NTP_CONDITIONAL_AGREEMENT" for window in windows) or not timeline:
        state = "NTP_EVIDENCE_UNVERIFIED"
    elif len(timeline) < 2 or any(before[1].wall_ns >= after[0].wall_ns for before, after in pairwise(timeline)):
        state = "NTP_NEED_DISTINCT_WINDOWS"
    else:
        age = (now.monotonic_ns - timeline[-1][1].monotonic_ns) / 1e9
        first = timeline[0][0]
        edges = [point for pair in timeline for point in pair] + [now]
        if (age < 0 or any(_clock_step(first, point) for point in edges)
                or any(_clock_step(before, after) for before, after in pairwise(edges))):
            state = "NTP_CLOCK_STEP_OR_MONOTONIC_ORIGIN_MISMATCH"
        elif age > NTP_REVIEW_MAX_AGE_SECONDS:
            state = "NTP_EVIDENCE_STALE"
    return {"state": state, "authenticated": False, "admission_authorized": False,
            "latest_sample_age_seconds": age, "windows": windows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--interval", type=int, default=2, help="Seconds between local status queries")
    parser.add_argument("--ntp-evidence", action="append", type=Path, default=[],
                        help="Review a saved, unauthenticated NTP capture. Repeat for multiple windows.")
    args = parser.parse_args()
    if args.samples < 1 or args.interval < 1:
        parser.error("samples and interval must be positive")
    observations = []
    for index in range(args.samples):
        if index:
            time.sleep(args.interval)
        observations.append(observe())
    result = report(observations)
    if args.ntp_evidence:
        result["independent_ntp_review_v1"] = summarize_ntp(args.ntp_evidence, now=_ntp_review_anchor())
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
