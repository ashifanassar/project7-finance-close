from ..state import CloseState
from ..utils import persist_state, audit_event


def initialize_run(state: CloseState) -> CloseState:
    state["status"] = "initialized"

    project_id = state["project_id"]
    run_id = state["run_id"]

    audit_event(
        project_id=project_id,
        run_id=run_id,
        agent_id="initialize_run",
        action="RUN_STARTED",
        resource="finance_close",
        policy_check_result="N/A",
        details=f"Close run initialized for period {state.get('period')}",
    )
    persist_state(project_id, run_id, "initialize_run", state["status"], state)
    return state