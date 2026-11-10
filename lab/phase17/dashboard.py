import streamlit as st

from lab.phase17.report import status


def panel():
    st.title("KuCoin native learning readiness")
    data = status()
    st.info(f"{data['approval']} · {data['execution']}")
    if "profiles" not in data:
        st.warning("Register the native development stream from PowerShell.")
        return
    st.caption(f"Registration {data['registration'][:16]} · recurring training {'on' if data['recurring_training'] else 'off'} · "
               f"next managed check {data['next_managed_check'] or 'pending'}")
    st.write("The managed worker records public prices and receipts. Label qualification and candidate fitting have separate gates.")
    if latest := data["last_managed"]:
        st.caption(f"Last managed collection check {latest['at']} · {latest['launcher']}")
    name = st.radio("Profile", ("day", "position", "swing"), horizontal=True, format_func=str.title, key="phase17-profile")
    if data["pilot"]["state"] == "PILOT_REGISTERED":
        pilot = data["pilot"]["profiles"][name]
        st.subheader("Phase 17.1 fitting pilot")
        st.write(f"**{pilot['fit_status']}** · {pilot['reason']}")
        st.caption(f"Permitted label start {pilot['label_start']} · {pilot['feature_readiness']}")
        if evidence := pilot["feature_evidence"]:
            st.caption(f"{evidence['rows']} checked feature-only bars through {evidence['cutoff']} · actual last receipt {evidence['receipt_last']} · "
                       f"context available as of {evidence['available_as_of']}")
        st.write(f"Mature train {pilot['train']} / {pilot['train_required'] or '—'} · mature inner {pilot['inner']} / {pilot['inner_required'] or '—'} · "
                 f"separate evaluation {pilot['evaluation']['state']}")
        st.caption(f"Next managed check {pilot['next_managed_check'] or 'pending'} · daily sample review {pilot['next_daily_sample_review']}")
        st.subheader("Original Phase 17 full-fold workflow")
    profile = data["profiles"][name]
    st.write(f"**{profile['state']}** · {profile['reason']}")
    st.caption(f"{profile['instrument']} · {profile['market']} · feature context lower bound {profile['feature_context_lower_bound']}")
    if capture := profile["capture"]:
        st.write(f"Last source interval {capture['start']} to {capture['end_exclusive']} exclusive")
        st.dataframe([{"Basis": basis, "Rows": values['rows'], "Last source close": values['source_last_close'],
                       "Last receipt": values['last_receipt']} for basis, values in capture['bases'].items()
                      if profile["market"] == "FUTURES" or basis == "trade"], hide_index=True)
        st.caption(f"Trade gaps {capture['trade_gaps']} · corrections {capture['corrections']}"
                   + (f" · paired {capture['paired']} · mark gaps {capture['mark_gaps']}" if profile["market"] == "FUTURES"
                      else " · mark-price pairing and funding N/A"))
    counts = {"Feature-ready decisions": profile["feature_ready_decisions"], "Eligible baseline setups": profile["eligible_baseline_setups"],
              "Mature permitted labels": profile["mature_permitted_labels"], "New mature labels": profile["new_mature_labels"],
              "Published Phase 15 ideas across books": profile["paper"].get("published_ideas", 0),
              "Label replay hypothetical executions": profile["hypothetical_executions"],
              "Closed native evaluation trades": profile["closed_evaluation_trades"], "Completed stress cases": profile["stress_complete_cases"]}
    st.dataframe([{"Measure": key, "Count": count} for key, count in counts.items()], hide_index=True)
    mins = profile["minimums"]
    st.caption(f"Maturity floor {mins['new_mature']} new · folds {mins['training']} train / {mins['inner']} inner / {mins['outer']} outer · "
               f"economic floor {mins['closed_economic']} closed · retraining interval {mins['interval_days']} days")
    st.write(f"Admitted bundle {profile['admitted_bundle'] or 'pending'} · training {profile['training_state']}")
    st.caption(f"Last native training result {profile['last_native_result'] or 'pending'} · last Phase 16 result "
               f"{profile['last_phase16_result'] or 'pending'} · next eligibility {profile['next_training_check'] or 'awaiting qualified data'}")
    if profile["exclusions"]:
        st.write("Unresolved and excluded outcomes", profile["exclusions"])
    if not profile["candidate_queue_enabled"]:
        st.caption("Baseline readiness only. The shelved Swing filter remains outside the fitting queue.")
    st.code(".\\phase17.ps1 status\n.\\phase17.ps1 report\n.\\phase17.ps1 enable\n.\\phase17.ps1 pause\n.\\phase17.ps1 disable", language="powershell")
