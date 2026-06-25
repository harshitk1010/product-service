"""
Main Airflow DAG — orchestrates the batch analytics pipeline.

Schedule: hourly
Flow:
  1. data_quality_check_raw  → verify Kafka consumers are writing to S3
  2. bronze_to_silver         → Spark batch job: clean + enrich
  3. silver_to_snowflake      → COPY INTO Snowflake from S3
  4. dbt_run                  → transform + build marts
  5. dbt_test                 → data quality gates
  6. refresh_power_bi         → trigger Power BI dataset refresh
  7. notify_success           → Slack/PagerDuty notification

Retry strategy: exponential backoff (30s, 60s, 120s) for transient failures.
SLA: pipeline must complete within 45 minutes of trigger.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
    "sla": timedelta(minutes=45),
}

S3_BUCKET = Variable.get("s3_bucket", default_var="ecommerce-analytics-lake")
SNOWFLAKE_CONN = "snowflake_analytics"
SPARK_MASTER = Variable.get("spark_master", default_var="spark://spark-master:7077")
DBT_PROJECT_DIR = "/opt/airflow/dbt/ecommerce_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt/profiles"


# ── Helper functions ──────────────────────────────────────────────────────────

def check_raw_data_availability(**context) -> str:
    """
    Verify that the Spark Streaming job wrote data in the last hour.
    Returns 'bronze_to_silver' if data exists, 'alert_no_data' if not.
    """
    execution_date = context["execution_date"]
    hour = execution_date.strftime("%Y/%m/%d/%H")
    s3 = S3Hook(aws_conn_id="aws_default")

    keys = s3.list_keys(
        bucket_name=S3_BUCKET,
        prefix=f"bronze/events/processing_date={execution_date.strftime('%Y-%m-%d')}/processing_hour={execution_date.hour}/",
    )

    if not keys:
        return "alert_no_data"
    return "bronze_to_silver"


def check_dbt_test_results(**context) -> str:
    """Branch based on dbt test outcome stored in XCom."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="dbt_test", key="return_value")
    if result and result != 0:
        return "alert_dbt_failure"
    return "refresh_power_bi"


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="ecommerce_analytics_pipeline",
    description="Hourly batch analytics: Bronze → Silver → Snowflake → dbt",
    schedule_interval="@hourly",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "analytics", "production"],
    doc_md="""
    ## E-Commerce Analytics Pipeline

    Transforms raw events from S3 Bronze layer through Silver, loads into
    Snowflake, runs dbt models and tests, then refreshes BI dashboards.

    **SLA**: 45 minutes from trigger time.
    **On-call**: #data-engineering Slack channel.
    """,
) as dag:

    start = DummyOperator(task_id="start")

    # ── Step 1: Data quality gate ──────────────────────────────────────────────
    check_raw_data = BranchPythonOperator(
        task_id="check_raw_data_availability",
        python_callable=check_raw_data_availability,
        provide_context=True,
    )

    alert_no_data = SlackWebhookOperator(
        task_id="alert_no_data",
        slack_webhook_conn_id="slack_data_alerts",
        message=(
            ":warning: *E-Commerce Pipeline*: No raw data found in S3 for "
            "{{ execution_date }}. Kafka consumer may be down."
        ),
    )

    # ── Step 2: Bronze to Silver (Spark batch) ────────────────────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"""
        spark-submit \
          --master {SPARK_MASTER} \
          --deploy-mode cluster \
          --num-executors 4 \
          --executor-cores 4 \
          --executor-memory 8g \
          --driver-memory 4g \
          --conf spark.sql.shuffle.partitions=32 \
          --conf spark.dynamicAllocation.enabled=true \
          --conf spark.dynamicAllocation.maxExecutors=8 \
          /opt/spark/jobs/bronze_to_silver.py \
          --execution-date {{{{ ds }}}} \
          --s3-bucket {S3_BUCKET}
        """,
    )

    # ── Step 3: Load Silver to Snowflake ──────────────────────────────────────
    load_user_events = SnowflakeOperator(
        task_id="load_user_events_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=f"""
        COPY INTO RAW_DB.EVENTS.USER_EVENTS
        FROM @RAW_DB.STAGES.S3_SILVER_STAGE/user-events/
          processing_date={{{{ ds }}}}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
        PURGE = FALSE;
        """,
        warehouse="ANALYTICS_LOAD_WH",
        database="RAW_DB",
        schema="EVENTS",
    )

    load_order_events = SnowflakeOperator(
        task_id="load_order_events_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=f"""
        COPY INTO RAW_DB.EVENTS.ORDER_EVENTS
        FROM @RAW_DB.STAGES.S3_SILVER_STAGE/order-events/
          processing_date={{{{ ds }}}}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE;
        """,
        warehouse="ANALYTICS_LOAD_WH",
        database="RAW_DB",
        schema="EVENTS",
    )

    load_product_events = SnowflakeOperator(
        task_id="load_product_events_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=f"""
        COPY INTO RAW_DB.EVENTS.PRODUCT_EVENTS
        FROM @RAW_DB.STAGES.S3_SILVER_STAGE/product-events/
          processing_date={{{{ ds }}}}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE;
        """,
        warehouse="ANALYTICS_LOAD_WH",
        database="RAW_DB",
        schema="EVENTS",
    )

    # ── Step 4: dbt run ───────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target prod \
          --vars '{{"execution_date": "{{{{ ds }}}}"}}'  \
          --select staging+ \
          --threads 8
        """,
    )

    # ── Step 5: dbt test ──────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target prod \
          --select staging+ \
          --threads 8
        exit $?
        """,
    )

    check_dbt_tests = BranchPythonOperator(
        task_id="check_dbt_test_results",
        python_callable=check_dbt_test_results,
        provide_context=True,
    )

    alert_dbt_failure = SlackWebhookOperator(
        task_id="alert_dbt_failure",
        slack_webhook_conn_id="slack_data_alerts",
        message=(
            ":x: *dbt Tests FAILED* for {{ ds }}. "
            "Data quality issues detected. Check Airflow logs immediately."
        ),
    )

    # ── Step 6: Refresh Power BI ──────────────────────────────────────────────
    refresh_power_bi = BashOperator(
        task_id="refresh_power_bi",
        bash_command="""
        python /opt/airflow/scripts/powerbi_refresh.py \
          --dataset-id $POWERBI_DATASET_ID \
          --workspace-id $POWERBI_WORKSPACE_ID
        """,
        env={
            "POWERBI_DATASET_ID": Variable.get("powerbi_dataset_id", default_var=""),
            "POWERBI_WORKSPACE_ID": Variable.get("powerbi_workspace_id", default_var=""),
        },
    )

    # ── Step 7: Success notification ──────────────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id="slack_data_alerts",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        message=(
            ":white_check_mark: *E-Commerce Analytics Pipeline* completed "
            "successfully for {{ ds }}. All dbt tests passed."
        ),
    )

    end = DummyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    start >> check_raw_data
    check_raw_data >> [alert_no_data, bronze_to_silver]
    alert_no_data >> end

    bronze_to_silver >> [load_user_events, load_order_events, load_product_events]
    [load_user_events, load_order_events, load_product_events] >> dbt_run
    dbt_run >> dbt_test >> check_dbt_tests
    check_dbt_tests >> [alert_dbt_failure, refresh_power_bi]
    alert_dbt_failure >> end
    refresh_power_bi >> notify_success >> end
