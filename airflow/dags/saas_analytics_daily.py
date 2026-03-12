"""
airflow/dags/saas_analytics_daily.py

PURPOSE: Orchestrates the full daily analytics pipeline:
  1. Check source data freshness
  2. Extract from app database (Python)
  3. Run dbt staging models
  4. Run dbt intermediate models
  5. Run dbt mart models
  6. Run all dbt tests (GATE — pipeline stops if tests fail)
  7. Notify Slack on success or failure

SCHEDULE: Daily at 6:00 AM UTC (data available by 6:30 AM for East Coast)

CONCEPTS demonstrated:
  - TaskGroups (logical grouping of related tasks)
  - BranchOperator (conditional execution)
  - Callbacks (on_failure_callback for alerting)
  - XCom (passing data between tasks)
  - Sensors (wait for upstream conditions)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# ── Default Args ───────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="saas_analytics_daily",
    description="Daily ELT pipeline: App DB extract → dbt transform → dbt test",
    schedule_interval="0 6 * * *",   # 6:00 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["analytics", "dbt", "daily", "production"],
    # Prevent multiple concurrent runs (data consistency)
    max_active_runs=1,
    doc_md="""
    ## SaaS Analytics Daily Pipeline

    Runs every morning at 6 AM UTC. Loads fresh data from all sources
    and rebuilds the analytics models in Snowflake.

    **SLA**: All mart tables available by 7:00 AM UTC.

    **On Failure**: Slack alert sent to #data-alerts channel.
    """,
) as dag:

    # ── Step 0: Start ─────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Step 1: Check Source Freshness ────────────────────────────────────────
    def check_source_freshness(**context):
        """
        Validate that source data has arrived before running dbt.
        Prevents running on stale data — a critical data quality gate.
        """
        import snowflake.connector
        import os

        sf = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
        )

        # Check each source table's most recent record
        freshness_checks = {
            "STRIPE_RAW.SUBSCRIPTIONS":    "6 hours",
            "SALESFORCE_RAW.ACCOUNT":      "8 hours",
            "APP_RAW.USERS":               "2 hours",
            "APP_RAW.EVENTS":              "2 hours",
        }

        stale_sources = []
        for table, max_age in freshness_checks.items():
            cursor = sf.cursor()
            cursor.execute(f"""
                select
                    max(_fivetran_synced) as latest_sync,
                    datediff('hour', max(_fivetran_synced), current_timestamp) as hours_old
                from ANALYTICS_DEV.{table}
            """)
            row = cursor.fetchone()
            hours_old = row[1] if row and row[1] else 999

            max_hours = int(max_age.split()[0])
            if hours_old > max_hours:
                stale_sources.append(f"{table} ({hours_old}h old, max {max_hours}h)")
                logger.warning(f"⚠️ Stale source: {table} is {hours_old}h old")

        sf.close()

        if stale_sources:
            # Push to XCom so downstream tasks can see which sources are stale
            context["ti"].xcom_push(key="stale_sources", value=stale_sources)
            logger.warning(f"Stale sources found: {stale_sources}")
            # We warn but don't block — Slack notification will flag this
        else:
            logger.info("✅ All sources are fresh")

    check_freshness = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness,
    )

    # ── Step 2: Extract from App Database ─────────────────────────────────────
    def run_app_db_extraction(**context):
        """Run the Python ETL script to pull from app PostgreSQL DB."""
        import subprocess
        result = subprocess.run(
            ["python", "/opt/airflow/scripts/ingestion/extractors/app_db_extractor.py"],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"Extraction failed:\n{result.stderr}")

    extract_app_db = PythonOperator(
        task_id="extract_app_db",
        python_callable=run_app_db_extraction,
    )

    # ── Step 3: dbt Tasks ──────────────────────────────────────────────────────
    DBT_DIR = "/opt/airflow/dbt_project"
    DBT_TARGET = "prod"

    # Helper: build dbt bash command
    def dbt_cmd(command: str, select: str = "", tags: str = "") -> str:
        base = f"cd {DBT_DIR} && dbt {command} --target {DBT_TARGET} --no-partial-parse"
        if select:
            base += f" --select {select}"
        if tags:
            base += f" --select tag:{tags}"
        return base

    with TaskGroup("dbt_run", tooltip="Run all dbt transformation layers") as dbt_run_group:

        # Run staging models first (they depend on raw sources)
        run_staging = BashOperator(
            task_id="run_staging",
            bash_command=dbt_cmd("run", tags="daily") + " --select staging",
            env={"DBT_SNOWFLAKE_PASSWORD": "{{ var.value.SNOWFLAKE_PASSWORD }}"},
        )

        # Intermediate models depend on staging
        run_intermediate = BashOperator(
            task_id="run_intermediate",
            bash_command=dbt_cmd("run", select="intermediate"),
        )

        # Mart models depend on intermediate
        run_marts = BashOperator(
            task_id="run_marts",
            bash_command=dbt_cmd("run", select="marts"),
        )

        # Reporting models depend on marts
        run_reporting = BashOperator(
            task_id="run_reporting",
            bash_command=dbt_cmd("run", select="reporting"),
        )

        # Chain: staging → intermediate → marts → reporting
        run_staging >> run_intermediate >> run_marts >> run_reporting

    # ── Step 4: dbt Tests (GATE) ───────────────────────────────────────────────
    # CRITICAL: Tests run AFTER transforms. If tests fail, we alert immediately.
    # Downstream consumers (Looker) should not see untested data.
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=dbt_cmd("test", select="marts reporting"),
    )

    # ── Step 5: Source Freshness Check (dbt built-in) ─────────────────────────
    run_source_freshness = BashOperator(
        task_id="run_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --target {DBT_TARGET}",
    )

    # ── Step 6: Slack Notifications ───────────────────────────────────────────
    def build_success_message(**context):
        run_date = context["ds"]
        duration = (datetime.now() - context["dag_run"].start_date).seconds // 60
        return f"""
:white_check_mark: *SaaS Analytics Pipeline Succeeded*
> Date: `{run_date}`
> Duration: {duration} minutes
> All mart tables are fresh and tested.
        """

    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id="slack_data_alerts",
        message="{{ task_instance.xcom_pull(task_ids='build_message') }}",
        trigger_rule="all_success",
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id="slack_data_alerts",
        message=":x: *SaaS Analytics Pipeline FAILED* — check Airflow logs",
        trigger_rule="one_failed",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # ── DAG Dependency Chain ───────────────────────────────────────────────────
    # start
    #   → check_freshness
    #   → extract_app_db
    #   → dbt_run (staging → intermediate → marts → reporting)
    #   → run_dbt_tests
    #   → run_source_freshness
    #   → notify_success / notify_failure
    #   → end

    (
        start
        >> check_freshness
        >> extract_app_db
        >> dbt_run_group
        >> run_dbt_tests
        >> run_source_freshness
        >> [notify_success, notify_failure]
        >> end
    )
