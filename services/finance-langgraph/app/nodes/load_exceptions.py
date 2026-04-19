
from ..state import CloseState
from ..clients.bigquery_client import FinanceBQClient
from ..utils import persist_state, audit_event


def load_exceptions(state: CloseState) -> CloseState:
    project_id = state["project_id"]
    run_id = state["run_id"]

    bq = FinanceBQClient(project_id)
    exceptions = bq.get_exceptions(run_id)

    state["unmatched_records"] = exceptions
    state["exception_count"] = len(exceptions)
    state["status"] = "exceptions_loaded"

    audit_event(
        project_id=project_id,
        run_id=run_id,
        agent_id="load_exceptions",
        action="EXCEPTIONS_LOADED",
        resource="month_end_exceptions",
        policy_check_result="N/A",
        details=f"Loaded {len(exceptions)} exceptions for run_id={run_id}",
    )
    persist_state(project_id, run_id, "load_exceptions", state["status"], state)
    return state