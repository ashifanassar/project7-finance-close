from ..state import CloseState
from ..policies.controls import evaluate_controls
from ..utils import persist_state, audit_event


def apply_controls(state: CloseState) -> CloseState:
    explanations = state.get("variance_explanations", [])
    result = evaluate_controls(explanations)

    state["controls_passed"] = result["controls_passed"]
    state["approvals_required"] = result["approvals_required"]
    state["materiality_breach"] = result["materiality_breach"]
    state["approval_items"] = result["approval_items"]
    state["control_reasons"] = result["reasons"]
    state["status"] = "controls_applied"

    project_id = state["project_id"]
    run_id = state["run_id"]

    policy_result = "APPROVAL_REQUIRED" if state["approvals_required"] else "AUTO_APPROVED"

    audit_event(
        project_id=project_id,
        run_id=run_id,
        agent_id="apply_controls",
        action="CONTROLS_EVALUATED",
        resource="finance_controls",
        policy_check_result=policy_result,
        details=" | ".join(result["reasons"]) if result["reasons"] else "No approval required",
    )
    persist_state(project_id, run_id, "apply_controls", state["status"], state)
    return state