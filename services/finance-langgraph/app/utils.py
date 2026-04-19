from typing import Any, Dict
from datetime import datetime
from decimal import Decimal
from .clients.bigquery_client import FinanceBQClient


def make_json_safe(obj: Any):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj


def persist_state(project_id: str, run_id: str, graph_node: str, status: str, state: Dict[str, Any]) -> None:
    bq = FinanceBQClient(project_id)

    safe_state = dict(state)

    if "unmatched_records" in safe_state:
        safe_state["unmatched_records"] = safe_state["unmatched_records"][:5]

    if "variance_explanations" in safe_state:
        safe_state["variance_explanations"] = safe_state["variance_explanations"][:5]

    safe_state = make_json_safe(safe_state)

    bq.insert_agent_state(
        run_id=run_id,
        graph_node=graph_node,
        status=status,
        state_payload=safe_state,
    )


def audit_event(
    project_id: str,
    run_id: str,
    agent_id: str,
    action: str,
    resource: str,
    policy_check_result: str,
    details: str
) -> None:
    bq = FinanceBQClient(project_id)
    bq.insert_audit_log(
        run_id=run_id,
        agent_id=agent_id,
        action=action,
        resource=resource,
        policy_check_result=policy_check_result,
        details=details,
    )