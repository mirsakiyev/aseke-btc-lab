import json

import pandas as pd
import streamlit as st

from lab.strategy_lab import comparison, prospective
from lab.strategy_lab.registry import MODE, MODEL_VALIDATION, PROFILES, connect, get, inventory, source_content
from lab.strategy_lab.specification import stage, template, validate


def job_index(db):
    return [{**dict(row), **json.loads(row["request"])} for row in db.execute(
        "SELECT hash,state,created_at,request FROM job ORDER BY created_at DESC"
    )]


def overview(strategies, jobs):
    metrics = st.columns(4)
    metrics[0].metric("Strategy variants", len(strategies))
    metrics[1].metric("Distinct families", len({row["family_id"] for row in strategies}))
    metrics[2].metric("Queued jobs", sum(row["state"] == "QUEUED" for row in jobs))
    metrics[3].metric("Running jobs", sum(row["state"] in ("RUNNING", "CANCEL_REQUESTED") for row in jobs))
    rows = []
    for profile in PROFILES:
        group = [row for row in strategies if row["spec"].get("profile") == profile]
        tested = sum(any("unit_test" in (item["reference"] or {}) for item in row["history"]) for row in group)
        rows.append({"Profile": profile.title(), "Variants": len(group), "Families": len({r["family_id"] for r in group}),
                     "Draft": sum(r["state"] == "DRAFT" for r in group),
                     "Implemented": sum(r["state"] not in ("DRAFT", "SPECIFIED", "REJECTED", "ARCHIVED") for r in group),
                     "Sandbox tested": tested, "Sandbox untested": len(group) - tested,
                     **{state.title(): sum(r["state"] == state for r in group) for state in ("SHORTLISTED", "REJECTED", "INCONCLUSIVE")}})
    st.dataframe(rows, hide_index=True, width="stretch")
    if jobs:
        st.dataframe([{key: row[key] for key in ("hash", "profile", "state", "created_at")} for row in jobs[:30]],
                     hide_index=True, width="stretch")


def library(db, strategies, jobs):
    search = st.text_input("Search strategies")
    filters = st.columns(2)
    profile = filters[0].selectbox("Profile", ["All", *PROFILES])
    family = filters[1].selectbox("Family", ["All", *sorted({row["family_id"] for row in strategies})])
    matches = [row for row in strategies if (profile == "All" or row["spec"].get("profile") == profile)
               and (family == "All" or row["family_id"] == family) and search.casefold() in
               " ".join(str(row["spec"].get(k, "")) for k in ("strategy_id", "family_id", "name", "description")).casefold()]
    if not matches:
        st.info("No matching strategy versions")
        return
    by_id = {row["strategy_id"]: row for row in matches}
    strategy = st.selectbox("Strategy", list(by_id), format_func=lambda key: by_id[key]["spec"].get("name") or key)
    versions = db.execute("SELECT hash,version FROM strategy_version WHERE strategy_id=? ORDER BY version DESC",
                          (strategy,)).fetchall()
    numbers = {row["hash"]: row["version"] for row in versions}
    selected = st.selectbox("Version", list(numbers), format_func=lambda key: f"v{numbers[key]} · {key[:12]}", key=f"version/{strategy}")
    record = get(db, selected)
    spec = record["spec"]
    version = spec.get("implementation_version") or record["created_at"]
    st.caption(f"{record['state']} · {record['family_id']} · {version}")
    st.code(selected, language=None)
    st.write(spec.get("description"))
    for field, label in (("entry_rules", "Entry"), ("exit_rules", "Exit"), ("invalidation_rules", "Invalidation"),
                         ("limitations", "Known limitations")):
        rules = spec.get(field)
        if isinstance(rules, list) and rules:
            st.markdown(f"**{label}**")
            for item in rules:
                st.write(item)
    latest = next((row for row in jobs if row["version"] == selected and row["state"] == "COMPLETE"), None)
    readiness = spec.get("historical_readiness")
    if isinstance(readiness, dict) and readiness.get("at_registration"):
        st.caption(f"Historical readiness at registration · {readiness['at_registration']}")
        if readiness.get("required"):
            st.write(readiness["required"])
    if latest:
        verdict = db.execute("SELECT body FROM checkpoint WHERE job_hash=? AND stage='verdict'", (latest["hash"],)).fetchone()
        st.caption(f"Latest historical screen · {json.loads(verdict[0])['status']} · {latest['hash'][:12]}")
    with st.expander("Frozen specification and provenance"):
        st.json(spec, expanded=False)
        source = spec.get("source", {})
        for label, sha_field, filename_field in (("Download reference attachment", "sha256", "filename"),
                                                  ("Download supplied description", "description_sha256", "description_filename")):
            if source.get(sha_field) and source.get(filename_field):
                try:
                    content = source_content(db, source[sha_field])
                    st.download_button(label, content, file_name=source[filename_field],
                                       mime="application/octet-stream", key=f"{selected}/{sha_field}")
                except ValueError:
                    st.caption("Referenced attachment is unavailable in this registry")
    if differences := spec.get("source_differences"):
        with st.expander("Source and description differences"):
            st.json(differences, expanded=True)
    extensions = spec.get("potential_v2")
    if isinstance(extensions, list) and extensions:
        with st.expander("Potential research extensions"):
            for item in extensions:
                st.write(item)
    with st.expander("Lifecycle, test and evaluation references"):
        st.json(record["history"], expanded=False)


def comparisons(db, jobs):
    completed = {row["hash"]: row for row in jobs if row["state"] == "COMPLETE"}
    if not completed:
        st.info("Completed research reports will appear here.")
        return
    keys = st.multiselect("Completed experiments", list(completed),
                          format_func=lambda key: f"{completed[key]['profile']} · {key[:12]}", max_selections=20)
    if not keys:
        return
    try:
        result = comparison.compare(db, keys)
    except ValueError as exc:
        st.warning(str(exc))
        return
    st.subheader(result["status"].replace("_", " ").title())
    attempts = result["selection"]
    st.write(f"{attempts['registered_requests']} profile requests across {attempts['versions']} versions "
             f"and {attempts['families']} entry families. {attempts['interpretation']}")
    metrics = ("closed_trades", "return", "net_expectancy", "mean_net_r", "win_rate", "profit_factor",
               "drawdown_observed_marks", "sharpe", "sortino", "exposure_fraction_of_observed_marks", "turnover",
               "fees", "adverse_already_in_gross", "funding", "accounting_complete")
    rows, curves = [], []
    for row in result["rows"]:
        account = row["metrics"]
        label = f"{row['strategy']} · {row['job'][:8]}"
        rows.append({"Strategy": label, "Status": row["status"], "Period start UTC": row["start"],
                     "Period end UTC": row["end_exclusive"], **{key: account[key] for key in metrics}})
        if account["accounting_complete"]:
            equity = pd.DataFrame(row["equity"], columns=["at", "equity"]).set_index("at")
            equity.index = pd.to_datetime(equity.index, utc=True).tz_convert("America/Toronto")
            curves.append(equity.equity.rename(label))
        with st.expander(f"Coverage, baseline, stress and uncertainty · {label}"):
            st.json({"coverage": row["coverage"], "uncertainty": account["uncertainty"], "baseline": row["baseline"]["metrics"],
                     "cash_return": result["cash_reference_return"], "stress": row["stress"],
                     "validation": row["validation"], "reproducibility": row["reproducibility"]}, expanded=False)
    st.dataframe(rows, hide_index=True, width="stretch")
    if curves:
        st.line_chart(pd.concat(curves, axis=1))
    if result["ranked_job_ids"]:
        st.caption("Exploratory order · " + ", ".join(key[:12] for key in result["ranked_job_ids"]))


def new_strategy():
    profile = st.selectbox("Draft profile", list(PROFILES), key="new_profile")
    with st.form("stage_strategy"):
        value = st.text_area("Strategy specification (JSON)", json.dumps(template(profile), indent=2),
                             height=350, key=f"draft/{profile}")
        prior = st.text_input("Current version hash for a revision")
        attachment = st.file_uploader("Optional source attachment (up to 2 MiB)", max_upload_size=2)
        check = st.form_submit_button("Validate")
        submit = st.form_submit_button("Save draft")
    if check or submit:
        try:
            spec = json.loads(value)
            if not isinstance(spec, dict):
                raise ValueError("Expected a JSON object")
            if submit:
                upload = (attachment.name, attachment.getvalue()) if attachment else None
                with connect(write=True) as db:
                    record = stage(db, spec, expected=prior.strip() or None, source_upload=upload)
                spec = record["spec"]
                st.success(f"Draft v{record['version']} saved")
                st.code(record["hash"], language=None)
            issues = validate(spec)
            if issues:
                st.dataframe(issues, hide_index=True, width="stretch")
            else:
                st.success("Required fields are present. Review the rules before implementation.")
        except (OSError, TypeError, ValueError) as exc:
            st.error(str(exc))


def proposals(db, jobs):
    completed = {row["hash"]: row["version"] for row in jobs if row["state"] == "COMPLETE"}
    saved = [prospective.read(db, row[0]) for row in db.execute("SELECT hash FROM proposal")]
    if saved:
        with st.expander("Saved disabled proposals"):
            st.json(saved, expanded=False)
    if not completed:
        st.info("A completed historical screen is required to prepare a proposal.")
        return
    key = st.selectbox("Historical screen", list(completed), format_func=lambda key: key[:16])
    value = prospective.preview(db, completed[key], key)
    st.caption("Disabled · Awaiting explicit approval")
    for issue in value["eligibility_issues"]:
        st.warning(issue)
    for risk in value["risks"]:
        st.write(risk)
    if value["eligibility_issues"]:
        return
    defaults = {"start": "", "horizon_days": 90, "minimum_closed_trades": 25, "execution_model": "",
                "data_availability": dict.fromkeys(("source", "receipt_rule", "gap_policy", "economic_path"), "")}
    with st.form("prospective_proposal"):
        raw = st.text_area("Proposal settings (JSON)", json.dumps(defaults, indent=2), height=270)
        save = st.form_submit_button("Save disabled proposal")
    if save:
        try:
            with connect(write=True) as writer:
                manifest = prospective.propose(writer, completed[key], key, json.loads(raw))
            st.json(manifest, expanded=False)
        except (TypeError, KeyError, ValueError) as exc:
            st.error(str(exc))


def panel():
    st.title("Strategy Library / Research Lab")
    st.caption(f"{MODEL_VALIDATION} · {MODE}")
    view = st.radio("Research Lab section", ("Overview", "Strategy Library", "Research Comparison", "New Strategy",
                                             "Prospective Experiment Proposal"), horizontal=True)
    if view == "New Strategy":
        new_strategy()
        return
    with connect() as db:
        jobs = job_index(db)
        if view == "Overview":
            overview(inventory(db), jobs)
        elif view == "Strategy Library":
            library(db, inventory(db), jobs)
        elif view == "Research Comparison":
            comparisons(db, jobs)
        else:
            proposals(db, jobs)
