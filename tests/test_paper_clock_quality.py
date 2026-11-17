import json
import subprocess
from dataclasses import replace
from types import SimpleNamespace

import pytest

from lab.paper_run import clock_quality as clock


OUTPUT = """Leap Indicator: 0(no warning)
Stratum: 5 (secondary reference - syncd by (S)NTP)
Root Delay: 0.0292835s
Root Dispersion: 3.8159702s
ReferenceId: 0xA83DD74A (source IP:  168.61.215.74)
Last Successful Sync Time: 2026-11-16 11:30:57 PM
Source: time.windows.com,0x8
Phase Offset: -0.0354199s
State Machine: 1 (Hold)
Last Sync Error: 0 (The command completed successfully.)
Time since Last Good Sync Time: 197.3116183s"""


def observations(*statuses, steps=()):
    wall = 1_794_932_000_000_000_000
    for index, status in enumerate(statuses):
        mono = index * 1_000_000_000
        stepped = steps[index] if steps else 0
        yield clock.Observation(clock.Anchor(wall + mono + stepped, mono),
                                clock.Anchor(wall + mono + stepped + 1000, mono + 1000), status)


def test_hold_with_small_offset_reports_excess_uncertainty():
    status = clock.WindowsStatus.parse(OUTPUT)
    assert status.state == 1 and status.phase_offset_seconds == pytest.approx(-0.0354199)
    result = clock.report(list(observations(status, status)))
    assert result["mode"] == "SHADOW_ONLY"
    assert result["state"] == "CLOCK_UNVERIFIED"
    assert result["reason"] == "WINDOWS_REPORTED_BOUND_EXCEEDS_TARGET"
    assert result["observations"][0]["windows_reported_bound_seconds"] == pytest.approx(3.86603185)
    assert not result["independent_reference_verified"]
    assert json.loads(json.dumps(result)) == result


def test_stable_low_distance_hold_still_requires_independent_evidence():
    status = replace(clock.WindowsStatus.parse(OUTPUT), phase_offset_seconds=-.04, root_dispersion_seconds=.03)
    later = replace(status, last_successful_sync="later")
    assert clock.assess(list(observations(status, later))) == (
        "CLOCK_UNVERIFIED", "HOLD_REQUIRES_INDEPENDENT_CLOCK_EVIDENCE")


def test_sync_needs_distinct_good_samples_from_a_stable_network_source():
    first = replace(clock.WindowsStatus.parse(OUTPUT), state=2, root_dispersion_seconds=.03)
    same_sync = replace(first, sync_age_seconds=first.sync_age_seconds + 1)
    assert clock.assess(observations(first, same_sync))[1] == "NEED_DISTINCT_WINDOWS_SYNC_SAMPLES"
    older_sync = replace(first, last_successful_sync="older", sync_age_seconds=first.sync_age_seconds + 5)
    assert clock.assess(observations(first, older_sync))[1] == "WINDOWS_SYNC_SAMPLE_TIMELINE_INCONSISTENT"
    second = replace(first, last_successful_sync="later")
    assert clock.assess(list(observations(first, second))) == (
        "CLOCK_HEALTHY", "WINDOWS_REPORTED_BOUND_WITHIN_TARGET")
    changed_source = replace(second, source="different.example")
    assert clock.assess(list(observations(first, changed_source)))[1] == "WINDOWS_SOURCE_OR_DISCIPLINE_NOT_STABLE"
    local_source = replace(second, stratum=1, reference_id="0x4C4F434C (LOCL)", source="Local CMOS Clock")
    assert clock.assess(list(observations(first, local_source)))[1] == "WINDOWS_SYNC_SAMPLE_INVALID"
    loopback = replace(second, reference_id="0x7f000001 (source IP: 127.0.0.1)")
    assert clock.assess(list(observations(first, loopback)))[1] == "WINDOWS_SYNC_SAMPLE_INVALID"
    # A new diagnostic process starts a new series. A report is never imported as live trust.
    assert clock.assess(list(observations(second)))[0] == "CLOCK_UNVERIFIED"


@pytest.mark.parametrize("offset", [-5, 5])
def test_five_second_error_fails_closed_with_good_reported_dispersion(offset):
    status = replace(clock.WindowsStatus.parse(OUTPUT), phase_offset_seconds=offset, root_dispersion_seconds=.03)
    assert clock.assess(list(observations(status)))[1] == "WINDOWS_REPORTED_BOUND_EXCEEDS_TARGET"


@pytest.mark.parametrize("jump", [-5_000_000_000, 5_000_000_000])
def test_wall_clock_jump_overshadows_small_ntp_offset(jump):
    status = clock.WindowsStatus.parse(OUTPUT)
    assert clock.assess(list(observations(status, status, steps=(0, jump)))) == (
        "CLOCK_STEP_DETECTED", "WALL_AND_MONOTONIC_ELAPSED_DISAGREE")


def test_missing_stale_and_malformed_windows_evidence():
    status = replace(clock.WindowsStatus.parse(OUTPUT), sync_age_seconds=18_001)
    assert clock.assess(list(observations(status))) == ("CLOCK_STALE", "WINDOWS_SYNC_SAMPLE_EXPIRED")
    assert clock.assess(list(observations(None))) == ("CLOCK_UNVERIFIED", "WINDOWS_STATUS_UNAVAILABLE")
    reads = iter((clock.Anchor(0, 0), clock.Anchor(5, 5)))
    result = clock.observe(read=lambda: next(reads), query=lambda *args, **kwargs: SimpleNamespace(stdout="garbled"))
    assert result.windows is None and "KeyError" in result.error


def test_observe_uses_local_read_only_query_and_reports_service_failure():
    reads = iter((clock.Anchor(0, 0), clock.Anchor(5, 5)))

    def command(args, **kwargs):
        assert args == ["w32tm", "/query", "/status", "/verbose"]
        assert kwargs["check"] and kwargs["timeout"] == 3
        raise subprocess.CalledProcessError(1, args)

    observed = clock.observe(read=lambda: next(reads), query=command)
    assert observed.windows is None and "CalledProcessError" in observed.error
    assert clock.assess([observed])[0] == "CLOCK_UNVERIFIED"


def ntp_window(tmp_path, name, *, second=0, offset=-.015, missing=False, step_ns=0):
    base = 1_794_932_000_000_000_000
    results = []
    for reference in clock.NTP_REFERENCES:
        attempts = []
        for n in range(3):
            sent = (second + n) * 1_000_000_000
            attempts.append({
                "sent_wall_ns": base + sent + step_ns, "sent_monotonic_ns": sent,
                "received_wall_ns": base + sent + step_ns + 20_000_000,
                "received_monotonic_ns": sent + 20_000_000,
                "offset_remote_minus_local_s": offset, "uncertainty_halfwidth_s": .026,
                "network_rtt_s": .020, "server_root_delay_s": .004, "server_root_dispersion_s": .003,
                "reference_age_s": 14., "leap": 0, "stratum": 2,
            })
        results.append({"host": reference, "attempts": attempts})
    if missing:
        results[0]["attempts"] = [{"error": "TimeoutError"}]
    path = tmp_path / name
    path.write_text(json.dumps({
        "authenticated": False, "utc_clock": "GetSystemTimePreciseAsFileTime", "results": results,
    }), encoding="utf-8")
    return path, lambda elapsed: clock.Anchor(base + elapsed * 1_000_000_000, elapsed * 1_000_000_000)


def test_repeatable_ntp_evidence_while_in_hold_is_conditional_only(tmp_path):
    first, anchor = ntp_window(tmp_path, "first.json")
    second, _ = ntp_window(tmp_path, "second.json", second=15)
    result = clock.summarize_ntp([second, first], now=anchor(20))
    assert result["state"] == "NTP_REPEATABLE_CONDITIONAL_AGREEMENT"
    assert not result["authenticated"] and not result["admission_authorized"]
    assert result["latest_sample_age_seconds"] == pytest.approx(2.98)
    assert result["windows"][0]["max_conditional_bound_seconds"] == pytest.approx(.041)
    assert clock.summarize_ntp([first, first], now=anchor(20))["state"] == "NTP_NEED_DISTINCT_WINDOWS"
    hold = clock.WindowsStatus.parse(OUTPUT)
    assert clock.assess(observations(hold, hold))[0] == "CLOCK_UNVERIFIED"


def test_ntp_window_ages_out_and_detects_clock_step(tmp_path):
    first, anchor = ntp_window(tmp_path, "first.json")
    second, _ = ntp_window(tmp_path, "second.json", second=15)
    assert clock.summarize_ntp([first, second], now=anchor(100))["state"] == "NTP_EVIDENCE_STALE"
    assert clock.summarize_ntp([first, second], now=anchor(16))["state"] == (
        "NTP_CLOCK_STEP_OR_MONOTONIC_ORIGIN_MISMATCH")
    stepped, _ = ntp_window(tmp_path, "stepped.json", second=15, step_ns=2_000_000_000)
    assert clock.summarize_ntp([first, stepped], now=anchor(20))["state"] == (
        "NTP_CLOCK_STEP_OR_MONOTONIC_ORIGIN_MISMATCH")


@pytest.mark.parametrize("missing,offset,window_state", [
    (True, -.015, "NTP_INSUFFICIENT_REFERENCES"),
    (False, 5., "NTP_CONDITIONAL_BOUND_EXCEEDS_TARGET"),
    (False, -5., "NTP_CONDITIONAL_BOUND_EXCEEDS_TARGET"),
])
def test_timeout_and_large_errors_cannot_qualify_ntp_review(tmp_path, missing, offset, window_state):
    first, anchor = ntp_window(tmp_path, "first.json")
    second, _ = ntp_window(tmp_path, "second.json", second=15, offset=offset, missing=missing)
    result = clock.summarize_ntp([first, second], now=anchor(20))
    assert result["state"] == "NTP_EVIDENCE_UNVERIFIED"
    assert result["windows"][1]["state"] == window_state
    assert not result["admission_authorized"]


def test_ntp_sources_with_nonoverlapping_bounds_are_rejected(tmp_path):
    path, anchor = ntp_window(tmp_path, "disagreement.json")
    data = json.loads(path.read_text())
    for sample in data["results"][0]["attempts"]:
        sample["offset_remote_minus_local_s"] = .065
    path.write_text(json.dumps(data))
    result = clock.summarize_ntp([path], now=anchor(4))
    assert result["windows"][0]["state"] == "NTP_REFERENCES_DISAGREE"
    assert not result["admission_authorized"]


def test_review_anchor_uses_the_capture_counter():
    import time

    before = time.perf_counter_ns()
    point = clock._ntp_review_anchor()
    assert before <= point.monotonic_ns <= time.perf_counter_ns()
    assert abs(point.wall_ns - time.time_ns()) < clock.STEP_NS
