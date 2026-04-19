from langgraph.graph import StateGraph, END
from .state import CloseState

from .nodes.initialize_run import initialize_run
from .nodes.load_exceptions import load_exceptions
from .nodes.variance_analyst import variance_analyst
from .nodes.apply_controls import apply_controls
from .nodes.request_approval import request_approval
from .nodes.generate_audit_pack import generate_audit_pack
from .nodes.finalize_run import finalize_run


def approval_router(state: CloseState) -> str:
    if state.get("approvals_required", False):
        return "approval_required"
    return "no_approval_required"


def build_graph():
    workflow = StateGraph(CloseState)

    workflow.add_node("initialize_run", initialize_run)
    workflow.add_node("load_exceptions", load_exceptions)
    workflow.add_node("variance_analyst", variance_analyst)
    workflow.add_node("apply_controls", apply_controls)
    workflow.add_node("request_approval", request_approval)
    workflow.add_node("generate_audit_pack", generate_audit_pack)
    workflow.add_node("finalize_run", finalize_run)

    workflow.set_entry_point("initialize_run")
    workflow.add_edge("initialize_run", "load_exceptions")
    workflow.add_edge("load_exceptions", "variance_analyst")
    workflow.add_edge("variance_analyst", "apply_controls")

    workflow.add_conditional_edges(
        "apply_controls",
        approval_router,
        {
            "approval_required": "request_approval",
            "no_approval_required": "generate_audit_pack",
        },
    )

    workflow.add_edge("request_approval", "finalize_run")
    workflow.add_edge("generate_audit_pack", "finalize_run")
    workflow.add_edge("finalize_run", END)

    return workflow.compile()
