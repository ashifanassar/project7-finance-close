from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from .graph import build_graph
from .clients.bigquery_client import FinanceBQClient
from .resume import resume_after_approval

app = FastAPI(title="Finance Close LangGraph Service")
graph = build_graph()


class RunCloseRequest(BaseModel):
    project_id: str
    run_id: str
    period: str
    mode: str = "dry_run"


class ApproveRequest(BaseModel):
    project_id: str
    run_id: str
    approver_comment: str = "Approved via API"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run-close")
def run_close(request: RunCloseRequest):
    try:
        initial_state = {
            "project_id": request.project_id,
            "run_id": request.run_id,
            "period": request.period,
            "mode": request.mode,
        }
        result = graph.invoke(initial_state)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/approve")
def approve_run(request: ApproveRequest):
    try:
        bq = FinanceBQClient(request.project_id)
        bq.approve_tasks(request.run_id, request.approver_comment)
        result = resume_after_approval(request.project_id, request.run_id)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))