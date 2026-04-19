
from typing import TypedDict, List, Dict, Any


class CloseState(TypedDict, total=False):
    project_id: str
    run_id: str
    period: str
    mode: str
    status: str

    reconciliation_run_id: str
    exception_count: int
    unmatched_records: List[Dict[str, Any]]
    variance_explanations: List[Dict[str, Any]]

    controls_passed: bool
    materiality_breach: bool
    approvals_required: bool
    approvals_gathered: bool
    approval_items: List[Dict[str, Any]]
    control_reasons: List[str]

    audit_pack_url: str
    final_message: str
    error_message: str
