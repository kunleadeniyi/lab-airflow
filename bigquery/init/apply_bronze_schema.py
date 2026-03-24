"""
Apply the BigQuery bronze DDL to the dbt-airflow-project-f1 project.
Run from the project root:
    python bigquery/init/apply_bronze_schema.py

Requires GOOGLE_APPLICATION_CREDENTIALS to be set, or will use ADC.
"""
import os
from pathlib import Path
from google.cloud import bigquery

PROJECT = "dbt-airflow-project-f1"
DDL_FILE = Path(__file__).parent / "01_bronze_tables.sql"


def main():
    client = bigquery.Client(project=PROJECT)
    ddl = DDL_FILE.read_text()

    # Split on semicolons; keep only statements that contain actual SQL (not pure comments)
    def has_sql(stmt):
        return any(
            line.strip() and not line.strip().startswith("--")
            for line in stmt.splitlines()
        )

    statements = [s.strip() for s in ddl.split(";") if s.strip() and has_sql(s)]

    print(f"Applying {len(statements)} DDL statements to project {PROJECT}...\n")
    for stmt in statements:
        # Extract table name for logging
        first_line = stmt.split("\n")[0]
        print(f"  Running: {first_line[:80]}...")
        job = client.query(stmt)
        job.result()
        print(f"  OK\n")

    print("All bronze tables created successfully.")


if __name__ == "__main__":
    os.environ.setdefault(
        "GOOGLE_APPLICATION_CREDENTIALS",
        str(Path(__file__).parents[2] / "dbt-airflow-sa.json"),
    )
    main()
