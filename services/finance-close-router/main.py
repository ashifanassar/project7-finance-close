import os
import uuid
import logging
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery

app = FastAPI(title="Finance Close Router Agent")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET", "finance")
BQ_RUN_TABLE = os.environ.get("BQ_RUN_TABLE", "close_run_control_v2")
SLACK_RELAY_URL = os.environ.get("SLACK_RELAY_URL", "").rstrip("/")

# Optional downstream URLs
RECON_AGENT_URL = os.environ.get("RECON_AGENT_URL", "")
VARIANCE_AGENT_URL = os.environ.get("VARIANCE_AGENT_URL", "")
CONTROLS_AGENT_URL = os.environ.get("CONTROLS_AGENT_URL", "")
APPROVALS_AGENT_URL = os.environ.get("APPROVALS_AGENT_URL", "")
AUDIT_PACK_AGENT_URL = os.environ.get("AUDIT_PACK_AGENT_URL", "")

bq = bigquery.Client(project=PROJECT_ID)


# ----------------------------
# Helpers
# ----------------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_table_id() -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{BQ_RUN_TABLE}"


def new_run_id() -> str:
    return f"run-{uuid.uuid4().hex[:12]}"


# ----------------------------
# Models
# ----------------------------
class StartCloseRunRequest(BaseModel):
    period: str
    run_type: str = "dry_run"   # dry_run / monthly
    created_by: str = "manual"
    trigger_mode: str = "api"   # api / cli / scheduler
    run_id: Optional[str] = None


class ApprovalDecisionRequest(BaseModel):
    run_id: str
    decision: str               # approved / denied
    reviewer_id: str = "controller"
    notes: Optional[str] = None


class CloseStateResponse(BaseModel):
    run_id: str
    status: str
    period: str
    run_type: str
    approval_required: Optional[bool] = None
    approval_status: Optional[str] = None
    audit_pack_url: Optional[str] = None


# ----------------------------
# BigQuery state management
# ----------------------------
def insert_run_event(
    run_id: str,
    period: str,
    run_type: str,
    status: str,
    created_by: str = "system",
    reconciliation_run_id: Optional[str] = None,
    controls_passed: Optional[bool] = None,
    approval_required: Optional[bool] = None,
    approval_status: Optional[str] = None,
    audit_pack_url: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    row = {
        "run_id": run_id,
        "period": period,
        "run_type": run_type,
        "status": status,
        "reconciliation_run_id": reconciliation_run_id,
        "controls_passed": controls_passed,
        "approval_required": approval_required,
        "approval_status": approval_status,
        "audit_pack_url": audit_pack_url,
        "created_by": created_by,
        "notes": notes,
        "event_ts": utc_now(),
    }

    logger.info("Inserting run event: %s", row)
    errors = bq.insert_rows_json(run_table_id(), [row])
    if errors:
        raise RuntimeError(f"Failed to insert run event: {errors}")


def fetch_run_record(run_id: str) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT *
    FROM `{run_table_id()}`
    WHERE run_id = @run_id
    ORDER BY event_ts DESC
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
        ]
    )
    rows = list(bq.query(query, job_config=job_config).result())
    if not rows:
        return None
    return dict(rows[0].items())


# ----------------------------
# Agent stubs
# Replace these with real HTTP calls later
# ----------------------------
def trigger_reconciliation(period: str, run_type: str, run_id: str) -> Dict[str, Any]:
    logger.info("Triggering reconciliation for run_id=%s period=%s", run_id, period)
    return {
        "reconciliation_run_id": f"recon-{uuid.uuid4().hex[:10]}",
        "status": "success",
        "matched_count": 980,
        "unmatched_count": 12
    }


def evaluate_variances(run_id: str) -> Dict[str, Any]:
    logger.info("Evaluating variances for run_id=%s", run_id)
    return {
        "status": "success",
        "unmatched_records": [
            {"variance_id": "var-001", "type": "timing_difference", "amount": 8500, "confidence_score": 0.91},
            {"variance_id": "var-002", "type": "fx_variance", "amount": 15000, "confidence_score": 0.72},
        ]
    }


def apply_controls(run_id: str, unmatched_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    logger.info("Applying controls for run_id=%s", run_id)

    approval_required = False
    reasons = []

    for rec in unmatched_records:
        amount = rec.get("amount", 0)
        vtype = rec.get("type", "")
        confidence = rec.get("confidence_score", 1.0)

        if amount > 10000:
            approval_required = True
            reasons.append("material_variance_gt_10000")
        if vtype == "fx_variance":
            approval_required = True
            reasons.append("fx_variance_requires_review")
        if confidence < 0.8:
            approval_required = True
            reasons.append("low_confidence_requires_hitl")

    return {
        "status": "success",
        "controls_passed": True,
        "approval_required": approval_required,
        "reasons": list(sorted(set(reasons)))
    }


def trigger_approval(run_id: str, reasons: List[str], period: str, run_type: str) -> Dict[str, Any]:
    if not SLACK_RELAY_URL:
        logger.warning("SLACK_RELAY_URL is not configured")
        return {
            "status": "not_configured",
            "channel": "slack_relay",
            "approval_status": "pending",
        }

    payload = {
        "source": "finance-close-router",
        "event_type": "finance_close_approval_required",
        "run_id": run_id,
        "period": period,
        "run_type": run_type,
        "reasons": reasons,
        "message": (
            f"Finance Close Approval Required\n"
            f"Run ID: {run_id}\n"
            f"Period: {period}\n"
            f"Run Type: {run_type}\n"
            f"Reasons: {', '.join(reasons)}"
        ),
    }

    resp = requests.post(
        f"{SLACK_RELAY_URL}/notify",
        json=payload,
        timeout=30,
    )

    logger.info("Slack relay response status=%s body=%s", resp.status_code, resp.text)

    if resp.status_code >= 300:
        raise RuntimeError(f"Slack relay failed: {resp.status_code} {resp.text}")

    return {
        "status": "sent",
        "channel": "slack_relay",
        "approval_status": "pending",
    }

def generate_audit_pack(run_id: str, period: str) -> Dict[str, Any]:
    logger.info("Generating audit pack for run_id=%s", run_id)
    return {
        "status": "success",
        "audit_pack_url": f"gs://finance-audit-packs-prod/{period}/{run_id}/audit_pack.pdf"
    }


# ----------------------------
# Core orchestration
# ----------------------------
def run_close_workflow(run_id: str, period: str, run_type: str) -> Dict[str, Any]:
    try:
        insert_run_event(
            run_id=run_id,
            period=period,
            run_type=run_type,
            status="RECONCILIATION_RUNNING",
            created_by="router",
            notes="Reconciliation started"
        )

        recon_result = trigger_reconciliation(period=period, run_type=run_type, run_id=run_id)
        if recon_result.get("status") != "success":
            insert_run_event(
                run_id=run_id,
                period=period,
                run_type=run_type,
                status="FAILED",
                created_by="router",
                notes="Reconciliation failed"
            )
            raise RuntimeError("Reconciliation failed")

        reconciliation_run_id = recon_result["reconciliation_run_id"]
        insert_run_event(
            run_id=run_id,
            period=period,
            run_type=run_type,
            status="RECONCILIATION_COMPLETED",
            created_by="router",
            reconciliation_run_id=reconciliation_run_id,
            notes="Reconciliation completed"
        )

        variance_result = evaluate_variances(run_id)
        if variance_result.get("status") != "success":
            insert_run_event(
                run_id=run_id,
                period=period,
                run_type=run_type,
                status="FAILED",
                created_by="router",
                reconciliation_run_id=reconciliation_run_id,
                notes="Variance evaluation failed"
            )
            raise RuntimeError("Variance evaluation failed")

        unmatched_records = variance_result.get("unmatched_records", [])
        insert_run_event(
            run_id=run_id,
            period=period,
            run_type=run_type,
            status="VARIANCES_EVALUATED",
            created_by="router",
            reconciliation_run_id=reconciliation_run_id,
            notes=f"Variance evaluation completed. unmatched_count={len(unmatched_records)}"
        )

        controls_result = apply_controls(run_id, unmatched_records)
        if controls_result.get("status") != "success":
            insert_run_event(
                run_id=run_id,
                period=period,
                run_type=run_type,
                status="FAILED",
                created_by="router",
                reconciliation_run_id=reconciliation_run_id,
                notes="Controls evaluation failed"
            )
            raise RuntimeError("Controls evaluation failed")

        controls_passed = controls_result["controls_passed"]
        approval_required = controls_result["approval_required"]
        reasons = controls_result.get("reasons", [])

        if not controls_passed:
            insert_run_event(
                run_id=run_id,
                period=period,
                run_type=run_type,
                status="BLOCKED",
                created_by="router",
                reconciliation_run_id=reconciliation_run_id,
                controls_passed=False,
                approval_required=True,
                approval_status="blocked",
                notes="Controls failed"
            )
            return {"run_id": run_id, "status": "BLOCKED"}

        insert_run_event(
            run_id=run_id,
            period=period,
            run_type=run_type,
            status="CONTROLS_PASSED",
            created_by="router",
            reconciliation_run_id=reconciliation_run_id,
            controls_passed=True,
            approval_required=approval_required,
            approval_status="pending" if approval_required else "not_required",
            notes="Controls passed"
        )

        if approval_required:
            trigger_approval(run_id, reasons, period, run_type)
            insert_run_event(
                run_id=run_id,
                period=period,
                run_type=run_type,
                status="AWAITING_APPROVAL",
                created_by="router",
                reconciliation_run_id=reconciliation_run_id,
                controls_passed=True,
                approval_required=True,
                approval_status="pending",
                notes=f"Awaiting approval. reasons={','.join(reasons)}"
            )
            return {
                "run_id": run_id,
                "status": "AWAITING_APPROVAL",
                "approval_required": True
            }

        audit_result = generate_audit_pack(run_id, period)
        if audit_result.get("status") != "success":
            insert_run_event(
                run_id=run_id,
                period=period,
                run_type=run_type,
                status="FAILED",
                created_by="router",
                reconciliation_run_id=reconciliation_run_id,
                controls_passed=True,
                approval_required=False,
                approval_status="not_required",
                notes="Audit pack generation failed"
            )
            raise RuntimeError("Audit pack generation failed")

        audit_pack_url = audit_result["audit_pack_url"]
        insert_run_event(
            run_id=run_id,
            period=period,
            run_type=run_type,
            status="AUDIT_PACK_GENERATED",
            created_by="router",
            reconciliation_run_id=reconciliation_run_id,
            controls_passed=True,
            approval_required=False,
            approval_status="not_required",
            audit_pack_url=audit_pack_url,
            notes="Audit pack generated"
        )

        insert_run_event(
            run_id=run_id,
            period=period,
            run_type=run_type,
            status="COMPLETED",
            created_by="router",
            reconciliation_run_id=reconciliation_run_id,
            controls_passed=True,
            approval_required=False,
            approval_status="not_required",
            audit_pack_url=audit_pack_url,
            notes="Close workflow completed successfully"
        )

        return {
            "run_id": run_id,
            "status": "COMPLETED",
            "approval_required": False,
            "audit_pack_url": audit_pack_url
        }

    except Exception as exc:
        logger.exception("Workflow failed for run_id=%s", run_id)
        try:
            insert_run_event(
                run_id=run_id,
                period=period,
                run_type=run_type,
                status="FAILED",
                created_by="router",
                notes=f"Workflow failed: {str(exc)}"
            )
        except Exception:
            logger.exception("Failed to insert FAILED event for run_id=%s", run_id)
        raise


# ----------------------------
# API endpoints
# ----------------------------
@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/start-close-run", response_model=CloseStateResponse)
def start_close_run(req: StartCloseRunRequest):
    run_id = req.run_id or new_run_id()

    try:
        insert_run_event(
            run_id=run_id,
            period=req.period,
            run_type=req.run_type,
            status="STARTED",
            created_by=req.created_by,
            approval_status="not_required",
            notes=f"Run created via {req.trigger_mode}"
        )

        result = run_close_workflow(run_id, req.period, req.run_type)

        row = fetch_run_record(run_id)
        return CloseStateResponse(
            run_id=run_id,
            status=result["status"],
            period=req.period,
            run_type=req.run_type,
            approval_required=row.get("approval_required") if row else None,
            approval_status=row.get("approval_status") if row else None,
            audit_pack_url=row.get("audit_pack_url") if row else None
        )

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/approval-decision", response_model=CloseStateResponse)
def approval_decision(req: ApprovalDecisionRequest):
    row = fetch_run_record(req.run_id)
    if not row:
        raise HTTPException(status_code=404, detail="run_id not found")

    if row["status"] != "AWAITING_APPROVAL":
        raise HTTPException(
            status_code=400,
            detail=f"Run is not awaiting approval. Current status={row['status']}"
        )

    decision = req.decision.lower().strip()

    if decision == "denied":
        insert_run_event(
            run_id=req.run_id,
            period=row["period"],
            run_type=row["run_type"],
            status="BLOCKED",
            created_by=req.reviewer_id,
            reconciliation_run_id=row.get("reconciliation_run_id"),
            controls_passed=row.get("controls_passed"),
            approval_required=True,
            approval_status="denied",
            audit_pack_url=row.get("audit_pack_url"),
            notes=f"Denied by {req.reviewer_id}. {req.notes or ''}"
        )
        latest = fetch_run_record(req.run_id)
        return CloseStateResponse(
            run_id=req.run_id,
            status=latest["status"],
            period=latest["period"],
            run_type=latest["run_type"],
            approval_required=latest.get("approval_required"),
            approval_status=latest.get("approval_status"),
            audit_pack_url=latest.get("audit_pack_url")
        )

    if decision != "approved":
        raise HTTPException(status_code=400, detail="decision must be approved or denied")

    insert_run_event(
        run_id=req.run_id,
        period=row["period"],
        run_type=row["run_type"],
        status="APPROVED",
        created_by=req.reviewer_id,
        reconciliation_run_id=row.get("reconciliation_run_id"),
        controls_passed=row.get("controls_passed"),
        approval_required=True,
        approval_status="approved",
        audit_pack_url=row.get("audit_pack_url"),
        notes=f"Approved by {req.reviewer_id}. {req.notes or ''}"
    )

    audit_result = generate_audit_pack(req.run_id, row["period"])
    if audit_result.get("status") != "success":
        insert_run_event(
            run_id=req.run_id,
            period=row["period"],
            run_type=row["run_type"],
            status="FAILED",
            created_by="router",
            reconciliation_run_id=row.get("reconciliation_run_id"),
            controls_passed=row.get("controls_passed"),
            approval_required=True,
            approval_status="approved",
            notes="Audit pack generation failed after approval"
        )
        raise HTTPException(status_code=500, detail="Audit pack generation failed")

    insert_run_event(
        run_id=req.run_id,
        period=row["period"],
        run_type=row["run_type"],
        status="AUDIT_PACK_GENERATED",
        created_by="router",
        reconciliation_run_id=row.get("reconciliation_run_id"),
        controls_passed=row.get("controls_passed"),
        approval_required=True,
        approval_status="approved",
        audit_pack_url=audit_result["audit_pack_url"],
        notes="Audit pack generated after approval"
    )

    insert_run_event(
        run_id=req.run_id,
        period=row["period"],
        run_type=row["run_type"],
        status="COMPLETED",
        created_by="router",
        reconciliation_run_id=row.get("reconciliation_run_id"),
        controls_passed=row.get("controls_passed"),
        approval_required=True,
        approval_status="approved",
        audit_pack_url=audit_result["audit_pack_url"],
        notes="Close workflow completed after approval"
    )

    latest = fetch_run_record(req.run_id)
    return CloseStateResponse(
        run_id=req.run_id,
        status=latest["status"],
        period=latest["period"],
        run_type=latest["run_type"],
        approval_required=latest.get("approval_required"),
        approval_status=latest.get("approval_status"),
        audit_pack_url=latest.get("audit_pack_url")
    )


@app.get("/runs/{run_id}", response_model=CloseStateResponse)
def get_run(run_id: str):
    row = fetch_run_record(run_id)
    if not row:
        raise HTTPException(status_code=404, detail="run_id not found")

    return CloseStateResponse(
        run_id=row["run_id"],
        status=row["status"],
        period=row["period"],
        run_type=row["run_type"],
        approval_required=row.get("approval_required"),
        approval_status=row.get("approval_status"),
        audit_pack_url=row.get("audit_pack_url")
    )