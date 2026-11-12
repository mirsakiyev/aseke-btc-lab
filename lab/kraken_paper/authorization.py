"""Verify and reference private Kraken Market Data permission evidence."""

from hashlib import sha256
import json
from pathlib import Path
import re

import pandas as pd

from lab.config import ROOT as PROJECT, fingerprint


PROVIDER = "Kraken"
CONTACT = "marketdata@kraken.com"
TERMS = "https://www.kraken.com/legal/ca-terms"
API_NOTICE = "https://docs-legacy.kraken.com/api/docs/guides/global-intro/"
RATE_LIMITS = "https://support.kraken.com/ca/articles/206548367-what-are-the-api-rate-limits-"
SCOPE = frozenset({
    "ontario_public_spot_access", "automated_collection", "local_raw_and_derived_retention",
    "historical_feature_initialization", "private_rules_only_prospective_paper",
    "spot_execution_evidence",
})
MAX_DOCUMENT = 256 * 1024


def utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Permission and market clocks require an explicit UTC offset")
    return stamp.tz_convert("UTC")


def bounded_bytes(path, limit=MAX_DOCUMENT):
    with Path(path).open("rb") as handle:
        data = handle.read(limit + 1)
    if not 0 < len(data) <= limit:
        raise ValueError("Evidence file is empty or exceeds its byte limit")
    return data


def _private(path):
    resolved = Path(path).resolve(strict=True)
    if resolved.is_relative_to(PROJECT.resolve()):
        raise ValueError("Private permission correspondence must be outside the repository")
    return resolved


def _text(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 240:
        raise ValueError("A bounded permission evidence reference or reviewer is required")
    return value.strip()


def validate(record, at):
    """Check reviewed scope and effective clocks at each authorized action."""
    if (record.get("provider") != PROVIDER or record.get("contact") != CONTACT
            or record.get("jurisdiction") != "CA-ON" or record.get("market") != "SPOT"
            or record.get("instrument") != "BTC-USD" or record.get("authorization_type") != "WRITTEN_PERMISSION"
            or record.get("approved_scope") != sorted(SCOPE)
            or record.get("paper_only") is not True or record.get("api_key_required") is not False
            or record.get("terms") != TERMS or record.get("api_notice") != API_NOTICE
            or record.get("rate_limits") != RATE_LIMITS):
        raise PermissionError("Kraken correspondence has no verified scope for all requested paper uses")
    hashes = [record.get(key) for key in ("original_request_sha256", "provider_response_sha256")]
    if any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes):
        raise PermissionError("Original request and provider response hashes are required")
    if hashes[0] == hashes[1]:
        raise PermissionError("Separate original request and provider response evidence is required")
    _text(record.get("evidence_reference"))
    _text(record.get("reviewed_by"))
    approved, observed, now = map(utc, (record["operator_approved_at"], record["evidence_observed_at"], at))
    granted = utc(record["provider_granted_at"]) if record.get("provider_granted_at") else None
    expires = utc(record["expires_at"]) if record.get("expires_at") else None
    if (not observed <= approved <= now or (granted is not None and granted > observed)
            or (expires is not None and now >= expires)):
        raise PermissionError("Kraken permission or operator approval is outside its effective clock")
    return record


def read_review(path, *, at):
    """Check an operator's reviewed scope against separately retained correspondence.

    The private descriptor states the reviewer's scope assessment. The original
    request and reply are byte-verified. A copied reply alone cannot grant scope.
    """
    descriptor = _private(path)
    document = json.loads(bounded_bytes(descriptor))
    if not isinstance(document, dict):
        raise ValueError("Permission review must be a JSON object")
    record = {key: document.get(key) for key in (
        "provider", "contact", "jurisdiction", "market", "instrument", "authorization_type",
        "approved_scope", "paper_only", "api_key_required", "evidence_reference", "reviewed_by",
        "original_request_sha256", "provider_response_sha256", "provider_granted_at",
        "operator_approved_at", "evidence_observed_at", "expires_at",
    )}
    record.update(terms=TERMS, api_notice=API_NOTICE, rate_limits=RATE_LIMITS)
    validate(record, at)
    for file_field, hash_field in (("original_request_path", "original_request_sha256"),
                                  ("provider_response_path", "provider_response_sha256")):
        original = _private(descriptor.parent / document[file_field])
        if sha256(bounded_bytes(original)).hexdigest() != record[hash_field]:
            raise PermissionError("Private Kraken permission evidence checksum failed")
    return record


def evidence_id(record):
    return fingerprint(["kraken-written-spot-permission-v1", record])
