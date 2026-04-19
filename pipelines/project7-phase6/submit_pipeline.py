from google.cloud import aiplatform

PROJECT_ID = "project7-492104"
REGION = "us-central1"
PIPELINE_ROOT = "gs://project7-finance-audit-packs/pipeline-runs/"
TEMPLATE_PATH = "gs://project7-finance-audit-packs/pipelines/finance_close_pipeline.json"

aiplatform.init(project=PROJECT_ID, location=REGION)

job = aiplatform.PipelineJob(
    display_name="finance-close-run-001",
    template_path=TEMPLATE_PATH,
    pipeline_root=PIPELINE_ROOT,
    parameter_values={
        "project_id": "project7-492104",
        "run_control_table": "project7-492104.finance_audit_dev.close_run_control",
        "workflow_status_table": "project7-492104.finance_audit_dev.workflow_status",
        "run_id": "close-apr-2026-run-006",
        "request_id": "req-600",
        "period": "2026-04",
        "run_type": "dry-run",
        "triggered_by": "ashifa",
    },
    enable_caching=False,
)

job.run(sync=False)
print("Pipeline submitted successfully.")