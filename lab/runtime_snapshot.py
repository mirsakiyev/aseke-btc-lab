"""Publish complete runtime documents under immutable Windows filenames."""

import json
import logging
import os
import re
from hashlib import sha256
from uuid import uuid4


_NAME = re.compile(r"(\d{16})-([a-f0-9]{64})\.json")
_KEEP = 3
_REMOVE_PER_SAVE = 8
_LOG = logging.getLogger(__name__)


def _directory(path):
    return path.with_name(path.name + ".snap")


def _snapshots(directory):
    # Enumerate immutable names only. An unfinished temporary is never read.
    with os.scandir(directory) as entries:
        return sorted(entry.name for entry in entries if _NAME.fullmatch(entry.name))


def read(path, legacy):
    """Use the latest immutable checkpoint, or a pre-adoption legacy reader."""
    directory = _directory(path)
    for attempt in range(2):
        try:
            names = _snapshots(directory)
        except FileNotFoundError:
            if attempt:
                raise
            return legacy()
        if not names:
            if attempt:
                raise FileNotFoundError(directory)
            return legacy()
        latest = names[-1]
        try:
            data = (directory / latest).read_bytes()
        except FileNotFoundError:
            if not attempt:
                continue  # The producer advanced and retired the selected file.
            raise
        if sha256(data).hexdigest() != _NAME.fullmatch(latest)[2]:
            raise ValueError(f"Runtime snapshot hash mismatch: {directory / latest}")
        return json.loads(data)


def publish(path, value, *, cleanup=True):
    """The existing sole owner publishes to a fresh name and retains recent copies.

    In particular, no reader has to opt into DELETE sharing. Reader contention
    can delay removing its old snapshot, but cannot delay replacing its bytes
    because publication does not replace any destination.
    """
    directory = _directory(path)
    directory.mkdir(parents=True, exist_ok=True)
    names = _snapshots(directory)
    sequence = int(_NAME.fullmatch(names[-1])[1]) + 1 if names else 1
    if sequence >= 10**16:
        raise ValueError(f"Runtime snapshot sequence exhausted: {directory}")
    data = json.dumps(value, separators=(",", ":"), default=str, allow_nan=False).encode("utf-8")
    name = f"{sequence:016d}-{sha256(data).hexdigest()}.json"
    temp = directory / f".{uuid4().hex}.tmp"
    try:
        with temp.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        # Windows rename refuses an existing destination. The owner lease is
        # required by the caller and sequence order does not use wall time.
        temp.rename(directory / name)
    except BaseException:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            _LOG.exception("Runtime snapshot temporary cleanup failed: %s", temp)
        raise
    if cleanup:
        _trim(directory, [*names, name], _REMOVE_PER_SAVE)


def trim(path, *, limit):
    directory = _directory(path)
    try:
        names = _snapshots(directory)
    except OSError:
        _LOG.exception("Runtime snapshot directory unavailable for retention cleanup: %s", directory)
        return
    _trim(directory, names, limit)


def _trim(directory, names, limit):
    expired_names = names[:max(0, len(names) - _KEEP)]
    for expired in expired_names[:limit]:
        try:
            (directory / expired).unlink(missing_ok=True)
        except PermissionError:
            # The reader owns this inode. Future publications may retire it.
            continue
        except OSError:
            _LOG.exception("Runtime snapshot retention cleanup failed: %s", directory / expired)
