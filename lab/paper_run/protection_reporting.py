"""Read-only validation of paper outcomes and their nominal capital ancestry."""

import json
from math import isfinite


VERSION = "protection-research-validation-v1"
INCIDENT_EVENTS = {
    "PROTECTION_GAP", "UNMODELED_TIER", "PROTECTION_INCIDENT_OPENED",
    "PROTECTION_INCIDENT_IMPORTED", "PROTECTION_INCIDENT_UPDATED", "PROTECTION_INCIDENT_EVIDENCE_ADDED",
    "PROTECTION_INCIDENT_RECOVERED",
    "PROTECTION_INCIDENT_ADJUDICATED",
}


def _numbers(record):
    keys = ("net", "net_r", "fees", "funding", "holding_minutes")
    return all(type(record.get(key)) in (float, int) and isfinite(record[key]) for key in keys)


def _references(record):
    """Collect dependencies without trusting an empty annotation to erase ancestry."""
    references = {"incident_ids": set(), "position_ids": set(), "segment_ids": set()}
    for key in ("capital_dependency", "research_provenance"):
        value = record.get(key)
        if not isinstance(value, dict):
            continue
        values = (value, value.get("capital_dependency") or {})
        for dependency in values:
            if not isinstance(dependency, dict):
                continue
            for field in ("incident_ids", "position_ids"):
                identifiers = dependency.get(field, ())
                if isinstance(identifiers, (list, tuple)):
                    references[field].update(identifier for identifier in identifiers if isinstance(identifier, str))
            if isinstance(dependency.get("segment_id"), str):
                references["segment_ids"].add(dependency["segment_id"])
    return references


def _merge(target, additions):
    for field, identifiers in additions.items():
        target[field].update(identifiers)


def _serializable(references):
    return {field: sorted(identifiers) for field, identifiers in references.items()}


def summarize(db, lane, book):
    """Keep nominal economics separate from validation of every closed outcome.

    This projection never writes to the account or incident journal. An
    adjudication or operational resume does not validate a provisional close.
    Historical capital ancestry remains excluded until a separately reviewed
    analytical resolution is implemented.
    """
    events = [(key, kind, idea, recorded_at, json.loads(raw))
              for key, kind, idea, recorded_at, raw in db.execute(
                  "SELECT key,kind,idea,recorded_at,body FROM event WHERE book=? ORDER BY rowid",
                  (lane["book_id"],))]
    affected = {}
    for _, kind, idea, _, body in events:
        if kind in INCIDENT_EVENTS:
            position = body.get("position_id") or idea
            if position:
                identifiers = affected.setdefault(position, set())
                incident = body.get("incident_id")
                if isinstance(incident, str):
                    identifiers.add(incident)

    inherited = {"incident_ids": set(), "position_ids": set(), "segment_ids": set()}
    outcomes = []
    validated = []
    nominal = []
    for key, kind, position_id, recorded_at, body in events:
        if kind == "BOOK_PROSPECTIVE_RESUME":
            _merge(inherited, _references(body))
        if kind != "CLOSED":
            continue
        dependencies = {field: identifiers.copy() for field, identifiers in inherited.items()}
        _merge(dependencies, _references(body))
        reasons = []
        if body.get("provisional"):
            reasons.append("PROVISIONAL_CLOSE")
        if body.get("operator_intervened") or body.get("exit_kind") == "OPERATOR_REQUESTED_PAPER_EXIT":
            reasons.append("OPERATOR_INTERVENED")
        if position_id in affected:
            reasons.append("HISTORICAL_PROTECTION_INCIDENT")
        if any(dependencies.values()):
            reasons.append("CAPITAL_DEPENDENCY")
        if not _numbers(body):
            reasons.append("INCOMPLETE_OUTCOME_ACCOUNTING")
        if reasons:
            inherited["position_ids"].add(position_id)
            inherited["incident_ids"].update(affected.get(position_id, ()))
            _merge(inherited, _references(body))
        else:
            validated.append(body)
        nominal.append(body)
        outcomes.append({"event_id": key, "position_id": position_id, "recorded_at": recorded_at,
                         "validation_state": "EXCLUDED" if reasons else "VALIDATED",
                         "exclusion_reasons": reasons, "capital_dependency": _serializable(dependencies),
                         "incident_ids": sorted(affected.get(position_id, ())),
                         "net": body.get("net"), "net_r": body.get("net_r")})

    count = len(validated)
    net = sum(record["net"] for record in validated)
    gains = sorted((record["net"] for record in validated if record["net"] > 0), reverse=True)
    all_nominal_nets = all(type(record.get("net")) in (int, float) and isfinite(record["net"]) for record in nominal)
    active = book.get("position") or {}
    active_uncertainty = bool(active.get("unresolved_protection") or active.get("id") in affected)
    excluded = len(nominal) - count
    return {
        "research_validation_policy": VERSION,
        "research_state": "QUARANTINED" if excluded or any(inherited.values()) or active_uncertainty else "VALIDATED",
        "validated_closed_count": count,
        "excluded_closed_count": excluded,
        "nominal_closed_count": len(nominal),
        "nominal_closed_net": sum(record["net"] for record in nominal) if all_nominal_nets else None,
        "validated_closed_net": net,
        "validated_expectancy": net / count if count else None,
        "validated_expectancy_r": sum(record["net_r"] for record in validated) / count if count else None,
        "validated_closed_fees": sum(record["fees"] for record in validated),
        "validated_closed_funding": sum(record["funding"] for record in validated),
        "validated_win_rate": sum(record["net"] > 0 for record in validated) / count if count else None,
        "validated_top_three_share_of_positive_gains": sum(gains[:3]) / sum(gains) if gains else None,
        "validated_average_hold_minutes": sum(record["holding_minutes"] for record in validated) / count if count else None,
        "capital_dependency": _serializable(inherited),
        "current_position_historical_uncertainty": active_uncertainty,
        "outcomes": outcomes,
    }
