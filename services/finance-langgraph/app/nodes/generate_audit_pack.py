
from ..state import CloseState
from ..utils import persist_state, audit_event


def generate_audit_pack(state: CloseState) -> CloseState:
    run_id = state["run_id"]
    project_id = state["project_id"]

    state["audit_pack_url"] = f"gs://{project_id}-audit-dev/audit_packs/{run_id}.pdf"
    state["status"] = "audit_pack_generated"

    audit_event(
        project_id=project_id,
        run_id=run_id,
        agent_id="generate_audit_pack",
        action="AUDIT_PACK_PREPARED",
        resource="audit_pack",
        policy_check_result="AUTO_APPROVED",
        details=state["audit_pack_url"],
    )
    persist_state(project_id, run_id, "generate_audit_pack", state["status"], state)
    return state