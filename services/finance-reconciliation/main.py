import argparse
from pathlib import Path
from jinja2 import Template
from google.cloud import bigquery


BASE_DIR = Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "sql"


def load_sql_template(filename: str) -> Template:
    content = (SQL_DIR / filename).read_text()
    return Template(content)


def run_query(client: bigquery.Client, query: str, label: str) -> None:
    print(f"\n--- Running: {label} ---")
    print(query)
    job = client.query(query)
    job.result()
    print(f"Completed: {label}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--start_date", required=True)
    parser.add_argument("--end_date", required=True)
    parser.add_argument("--amount_tolerance", type=float, default=500.0)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project_id)

    exact_sql = load_sql_template("exact_match.sql.j2").render(
        project_id=args.project_id,
        run_id=args.run_id,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    tolerance_sql = load_sql_template("tolerance_match.sql.j2").render(
        project_id=args.project_id,
        run_id=args.run_id,
        start_date=args.start_date,
        end_date=args.end_date,
        amount_tolerance=args.amount_tolerance,
    )

    unmatched_sql = load_sql_template("unmatched_extract.sql.j2").render(
        project_id=args.project_id,
        run_id=args.run_id,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    run_query(client, exact_sql, "Exact Match")
    run_query(client, tolerance_sql, "Tolerance Match")
    run_query(client, unmatched_sql, "Unmatched Extract")

    print("\nReconciliation run completed successfully.")
    print(f"Run ID: {args.run_id}")


if __name__ == "__main__":
    main()
