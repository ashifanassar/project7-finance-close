from ..state import CloseState
from ..utils import persist_state, audit_event


def variance_analyst(state: CloseState) -> CloseState:
    unmatched = state.get("unmatched_records", [])
    explanations = []

    for item in unmatched:
        variance = float(item.get("variance_amount", 0) or 0)

        if variance == 0:
            explanation = "No variance detected"
            confidence = 0.95
        elif variance <= 500:
            explanation = "Likely minor timing or rounding difference"
            confidence = 0.88
        elif variance <= 5000:
            explanation = "Possible FX variance or delayed settlement"
            confidence = 0.82
        else:
            explanation = "High-value unexplained variance; requires review"
            confidence = 0.60

        explanations.append({
            "exception_id": item.get("exception_id"),
            "variance_amount": variance,
            "exception_type": item.get("exception_type"),
            "explanation": explanation,
            "confidence_score": confidence,
        })

    state["variance_explanations"] = explanations
    state["status"] = "variance_analyzed"

    project_id = state["project_id"]
    run_id = state["run_id"]

    audit_event(
        project_id=project_id,
        run_id=run_id,
        agent_id="variance_analyst",
        action="VARIANCE_ANALYZED",
        resource="month_end_exceptions",
        policy_check_result="N/A",
        details=f"Generated {len(explanations)} variance explanations",
    )
    persist_state(project_id, run_id, "variance_analyst", state["status"], state)
    return state
