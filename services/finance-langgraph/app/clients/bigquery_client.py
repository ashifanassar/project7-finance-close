from datetime import datetime, timezone
from typing import List, Dict, Any
from google.cloud import bigquery
import json


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FinanceBQClient:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def get_approval_tasks(self, run_id: str) -> List[Dict[str, Any]]:
        query = f"""
        SELECT
          approval_task_id,
          run_id,
          exception_id,
          approval_type,
          approver_group,
          status,
          requested_at,
          approved_at,
          comments
        FROM `{self.project_id}.finance_audit_dev.approval_tasks`
        WHERE run_id = @run_id
        ORDER BY requested_at
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
            ]
        )
        rows = self.client.query(query, job_config=job_config).result()
        return [dict(row.items()) for row in rows]

    def approve_tasks(self, run_id: str, approver_comment: str = "Approved via API") -> None:
        query = f"""
        UPDATE `{self.project_id}.finance_audit_dev.approval_tasks`
        SET
          status = 'APPROVED',
          approved_at = CURRENT_TIMESTAMP(),
          comments = CONCAT(IFNULL(comments, ''), ' | ', @approver_comment)
        WHERE run_id = @run_id
          AND status = 'PENDING'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("approver_comment", "STRING", approver_comment),
            ]
        )
        self.client.query(query, job_config=job_config).result()

    def get_latest_agent_state(self, run_id: str) -> Dict[str, Any] | None:
        query = f"""
        SELECT
          run_id,
          graph_node,
          status,
          state_payload,
          updated_at
        FROM `{self.project_id}.finance_audit_dev.agent_run_state`
        WHERE run_id = @run_id
        ORDER BY updated_at DESC
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
            ]
        )
        rows = list(self.client.query(query, job_config=job_config).result())
        if not rows:
            return None

        row = dict(rows[0].items())
        payload = row.get("state_payload")
        if isinstance(payload, str):
            try:
                row["state_payload"] = json.loads(payload)
            except Exception:
                pass
        return row

    def get_exceptions(self, run_id: str) -> List[Dict[str, Any]]:
        query = f"""
        SELECT
          run_id,
          exception_id,
          gl_id,
          bank_id,
          variance_amount,
          exception_type,
          agent_explanation,
          confidence_score,
          reviewer_id,
          status,
          created_at
        FROM `{self.project_id}.finance_core_dev.month_end_exceptions`
        WHERE run_id = @run_id
        ORDER BY created_at
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
            ]
        )
        rows = self.client.query(query, job_config=job_config).result()
        return [dict(row.items()) for row in rows]

    def insert_audit_log(
        self,
        run_id: str,
        agent_id: str,
        action: str,
        resource: str,
        policy_check_result: str,
        details: str
    ) -> None:
        table_id = f"{self.project_id}.finance_audit_dev.audit_log"
        row = {
            "timestamp": utc_now_iso(),
            "run_id": run_id,
            "agent_id": agent_id,
            "action": action,
            "resource": resource,
            "policy_check_result": policy_check_result,
            "details": details,
        }
        errors = self.client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"BigQuery insert_audit_log failed: {errors}")

    def insert_agent_state(
        self,
        run_id: str,
        graph_node: str,
        status: str,
        state_payload: Dict[str, Any]
    ) -> None:
        table_id = f"{self.project_id}.finance_audit_dev.agent_run_state"
        row = {
            "run_id": run_id,
            "graph_node": graph_node,
            "status": status,
            "state_payload": json.dumps(state_payload),
            "updated_at": utc_now_iso(),
        }
        errors = self.client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"BigQuery insert_agent_state failed: {errors}")

    def insert_approval_tasks(
        self,
        run_id: str,
        approval_items: List[Dict[str, Any]]
    ) -> None:
        if not approval_items:
            return

        values_sql = []
        query_parameters = [
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
        ]

        for i, item in enumerate(approval_items, start=1):
            approval_task_id_param = f"approval_task_id_{i}"
            exception_id_param = f"exception_id_{i}"
            comments_param = f"comments_{i}"

            values_sql.append(
                f"(@run_id, @{approval_task_id_param}, @{exception_id_param}, "
                f"'FINANCE_CONTROLLER_REVIEW', 'L2_CONTROLLER', 'PENDING', "
                f"CURRENT_TIMESTAMP(), NULL, @{comments_param})"
            )

            query_parameters.extend([
                bigquery.ScalarQueryParameter(
                    approval_task_id_param, "STRING", f"{run_id}-approval-{i}"
                ),
                bigquery.ScalarQueryParameter(
                    exception_id_param, "STRING", item.get("exception_id")
                ),
                bigquery.ScalarQueryParameter(
                    comments_param, "STRING", " | ".join(item.get("reasons", []))
                ),
            ])

        query = f"""
        INSERT INTO `{self.project_id}.finance_audit_dev.approval_tasks`
        (
          run_id,
          approval_task_id,
          exception_id,
          approval_type,
          approver_group,
          status,
          requested_at,
          approved_at,
          comments
        )
        VALUES
        {', '.join(values_sql)}
        """

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        self.client.query(query, job_config=job_config).result()