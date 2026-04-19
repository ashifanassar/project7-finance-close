
from ..state import CloseState
from ..utils import persist_state, audit_event


def finalize_run(state: CloseState) -> CloseState:
    if state.get("approvals_required"):
        state["final_message"] = "Run paused pending approval"
    else:
        state["final_message"] = "Run completed successfully"

    state["status"] = "completed"

    project_id = state["project_id"]
    run_id = state["run_id"]

    audit_event(
        project_id=project_id,
        run_id=run_id,
        agent_id="finalize_run",
        action="RUN_COMPLETED",
        resource="finance_close",
        policy_check_result="PENDING_HITL" if state.get("approvals_required") else "COMPLETED",
        details=state["final_message"],
    )
    persist_state(project_id, run_id, "finalize_run", state["status"], state)
    return state
