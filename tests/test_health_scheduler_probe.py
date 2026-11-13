from types import SimpleNamespace

from lab.health import __main__ as watchdog


def test_windows_powershell_51_task_probe_reports_missing_registration(monkeypatch):
    name = "ASEKE scheduler evidence fixture 1e467db0-4752-4a8d-ad48-856dd3d33e78"
    monkeypatch.setattr(watchdog, "TASKS", (name,))
    assert watchdog.task_evidence() == {name: {"state": "MISSING"}}


def test_task_state_and_task_identity_metadata_are_distinct(monkeypatch):
    names = ("active", "denied", "bad-result")
    lines = b"active|4||267009|2026-11-13T15:00:00.0000000|1899-12-30T00:00:00.0000000\ndenied|UNKNOWN\nbad-result|3|1|invalid||\n"
    monkeypatch.setattr(watchdog, "TASKS", names)
    monkeypatch.setattr(watchdog.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=lines))

    evidence = watchdog.task_evidence()

    assert evidence["active"] == {"state": "RUNNING", "logon": None, "result": 267009,
                                  "last": "2026-11-13T15:00:00.0000000", "next": ""}
    assert evidence["denied"] == evidence["bad-result"] == {"state": "UNKNOWN"}
