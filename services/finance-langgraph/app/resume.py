from .clients.bigquery_client import FinanceBQClient
from .nodes.generate_audit_pack import generate_audit_pack
from .nodes.finalize_run import finalize_run


def resume_after_approval(project_id: str, run_id: str):
    bq = FinanceBQClient(project_id)
    latest = bq.get_latest_agent_state(run_id)

    if not latest:
        raise RuntimeError(f"No agent state found for run_id={run_id}")

    state = latest.get("state_payload", {})
    if not isinstance(state, dict):
        raise RuntimeError("Latest state payload is invalid")

    state["project_id"] = project_id
    state["run_id"] = run_id
    state["approvals_required"] = False
    state["approvals_gathered"] = True
    state["status"] = "approved_resume"

    state = generate_audit_pack(state)
    state = finalize_run(state)
    return state