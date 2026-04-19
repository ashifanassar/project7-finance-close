from kfp import dsl
from kfp.compiler import Compiler


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=["google-cloud-bigquery==3.25.0"],
)
def init_run_component(
    project_id: str,
    table_id: str,
    run_id: str,
    period: str,
    run_type: str,
    triggered_by: str,
) -> str:
    from datetime import datetime, timezone
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    now = datetime.now(timezone.utc).isoformat()

    row = {
        "run_id": run_id,
        "period": period,
        "run_type": run_type,
        "status": "STARTED",
        "started_ts": now,
        "updated_ts": now,
        "triggered_by": triggered_by,
        "notes": "Run initialized by Vertex AI Pipeline",
    }

    errors = client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(f"Failed to insert run control row: {errors}")

    print(f"Inserted run control row: {row}")
    return run_id


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=["requests"],
)
def invoke_router_component(
    router_url: str,
    run_id: str,
    period: str,
    run_type: str,
    triggered_by: str,
) -> str:
    import requests

    payload = {
        "run_id": run_id,
        "period": period,
        "run_type": run_type,
        "created_by": triggered_by,
        "trigger_mode": "vertex_pipeline"
    }

    print(f"Calling Router: {router_url}")
    print(f"Payload: {payload}")

    response = requests.post(router_url, json=payload, timeout=60)

    if response.status_code != 200:
        raise RuntimeError(f"Router call failed: {response.status_code} {response.text}")

    print(f"Router triggered successfully: {response.text}")
    return response.text


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=["google-cloud-bigquery==3.25.0"],
)
def finalize_run_component(
    project_id: str,
    table_id: str,
    run_id: str,
    period: str,
    run_type: str,
    triggered_by: str,
    final_status: str,
    notes: str,
) -> str:
    from datetime import datetime, timezone
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    now = datetime.now(timezone.utc).isoformat()

    row = {
        "run_id": run_id,
        "period": period,
        "run_type": run_type,
        "status": final_status,
        "started_ts": now,
        "updated_ts": now,
        "triggered_by": triggered_by,
        "notes": notes,
    }

    errors = client.insert_rows_json(table_id, [row])
    print(f"Finalize insert response: {errors}")

    if errors:
        raise RuntimeError(f"Failed to insert finalize row: {errors}")

    print(f"Inserted finalize row: {row}")
    return final_status


@dsl.pipeline(
    name="finance-close-e2e-pipeline",
)
def finance_close_pipeline(
    project_id: str = "project7-492104",
    run_control_table: str = "project7-492104.finance_audit_dev.close_run_control",
    router_url: str = "https://finance-close-router-741187623089.us-central1.run.app/start-close-run",
    run_id: str = "close-2026-04-prod-001",
    period: str = "2026-04",
    run_type: str = "dry_run",
    triggered_by: str = "ashifa",
):
    init_task = init_run_component(
        project_id=project_id,
        table_id=run_control_table,
        run_id=run_id,
        period=period,
        run_type=run_type,
        triggered_by=triggered_by,
    )

    router_task = invoke_router_component(
        router_url=router_url,
        run_id=init_task.output,
        period=period,
        run_type=run_type,
        triggered_by=triggered_by,
    )
    router_task.set_caching_options(False)

    finalize_task = finalize_run_component(
    project_id=project_id,
    table_id=run_control_table,
    run_id=init_task.output,
    period=period,
    run_type=run_type,
    triggered_by=triggered_by,
    final_status="AWAITING_APPROVAL",
    notes="Router triggered successfully; waiting for Slack approval",
    )
    finalize_task.after(router_task)
    finalize_task.set_caching_options(False)


if __name__ == "__main__":
    Compiler().compile(
        pipeline_func=finance_close_pipeline,
        package_path="finance_close_pipeline.json",
    )