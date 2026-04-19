
from ..state import CloseState
from ..clients.bigquery_client import FinanceBQClient
from ..utils import persist_state, audit_event


def request_approval(state: CloseState) -> CloseState:
    project_id = state["project_id"]
    run_id = state["run_id"]
    approval_items = state.get("approval_items", [])

    bq = FinanceBQClient(project_id)
    bq.insert_approval_tasks(run_id, approval_items)

    state["approvals_gathered"] = False
    state["status"] = "approval_required"
    state["final_message"] = "Approval required before close can proceed"

    audit_event(
        project_id=project_id,
        run_id=run_id,
        agent_id="request_approval",
        action="APPROVAL_TASKS_CREATED",
        resource="approval_tasks",
        policy_check_result="PENDING_HITL",
        details=f"Created {len(approval_items)} approval task(s)",
    )
    persist_state(project_id, run_id, "request_approval", state["status"], state)
    return state