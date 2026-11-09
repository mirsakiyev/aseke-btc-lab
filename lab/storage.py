import json
import msvcrt
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pandas as pd

from lab.config import ROOT

DATA = ROOT / "data"
RUNS = ROOT / "runs"
MODELS = ROOT / "models"


def utc(value: str | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def file_hash(path: Path) -> str:
    with path.open("rb") as handle:
        from hashlib import file_digest
        return file_digest(handle, "sha256").hexdigest()


def put_bytes(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{uuid4().hex}.tmp")
    try:
        temporary.write_bytes(body)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path: Path, record: object) -> None:
    put_bytes(path, json.dumps(record, indent=2, default=str, allow_nan=False).encode("utf-8"))


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def immutable_bytes(directory: Path, body: bytes, extension: str) -> str:
    digest = sha256(body).hexdigest()
    path = directory / f"{digest}.{extension}"
    if not path.exists():
        put_bytes(path, body)
    return digest


@contextmanager
def writer_lock(path: Path):
    """OS releases the lock on exit, including if the process crashes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise RuntimeError(f"Another writer owns {path}") from exc
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
