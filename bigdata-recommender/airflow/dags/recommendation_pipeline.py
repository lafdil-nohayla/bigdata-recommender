"""
Airflow DAG — Big Data Recommendation Pipeline
───────────────────────────────────────────────
Tasks:
  1. check_data          → verify Reviews.csv exists
  2. start_kafka_producer → trigger producer container
  3. train_als_model     → submit Spark batch training job
  4. evaluate_model      → read RMSE from report.json
  5. start_streaming     → launch Spark streaming job
  6. notify              → log summary

Schedule: @daily (can be triggered manually)
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner":            "bigdata",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_MASTER   = os.getenv("SPARK_MASTER",   "spark://spark-master:7077")
DATA_PATH      = os.getenv("DATA_PATH",       "/opt/airflow/data/Reviews.csv")
MODEL_PATH     = os.getenv("MODEL_PATH",       "/opt/spark-models/als_model")
REPORT_PATH    = os.getenv("REPORT_PATH",      "/opt/spark-models/report.json")
SPARK_APPS_DIR = "/opt/airflow/spark"


# ── Task callables ─────────────────────────────────────────────────────────────

def check_data_exists(**ctx):
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            f"Dataset not found at {DATA_PATH}. "
            "Download from https://www.kaggle.com/snap/amazon-fine-food-reviews "
            "and place Reviews.csv in the ./data/ directory."
        )
    size_mb = os.path.getsize(DATA_PATH) / (1024 ** 2)
    print(f"✓ Dataset found: {DATA_PATH} ({size_mb:.1f} MB)")


def read_model_report(**ctx):
    if not os.path.exists(REPORT_PATH):
        print("No report found — model may not have been trained yet.")
        return
    with open(REPORT_PATH) as f:
        report = json.load(f)
    print("=" * 50)
    print("Model Evaluation Report")
    print("=" * 50)
    for k, v in report.items():
        print(f"  {k:20s}: {v}")
    print("=" * 50)
    # Push RMSE to XCom
    ctx["ti"].xcom_push(key="rmse", value=report.get("rmse"))


def notify_summary(**ctx):
    rmse = ctx["ti"].xcom_pull(task_ids="evaluate_model", key="rmse")
    print(f"""
╔══════════════════════════════════════════╗
║   Pipeline completed successfully!       ║
║   RMSE on test set : {str(rmse):>18s}   ║
║   Model saved to   : {MODEL_PATH[:20]:>18s}   ║
╚══════════════════════════════════════════╝
    """)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="bigdata_recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end ALS recommendation pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["bigdata", "kafka", "spark", "als", "recommendation"],
) as dag:

    # Task 1 — Verify data
    t_check_data = PythonOperator(
        task_id="check_data",
        python_callable=check_data_exists,
    )

    # Task 2 — Submit Spark training job (batch)
    t_train = BashOperator(
        task_id="train_als_model",
        bash_command=f"""
            docker exec spark-master /opt/spark/bin/spark-submit \
                --master {SPARK_MASTER} \
                --deploy-mode client \
                --driver-memory 2g \
                --executor-memory 2g \
                --executor-cores 2 \
                --packages org.postgresql:postgresql:42.6.0 \
                {SPARK_APPS_DIR}/training/train_als.py
        """,
        execution_timeout=timedelta(hours=2),
    )

    # Task 3 — Evaluate / read report
    t_evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=read_model_report,
    )

    # Task 4 — Start Kafka producer (restart if already running)
    t_producer = BashOperator(
        task_id="start_kafka_producer",
        bash_command="docker restart kafka-producer || true",
    )

    # Task 5 — Launch Spark streaming job (background)
    t_streaming = BashOperator(
        task_id="start_streaming_recommender",
        bash_command=f"""
            docker exec -d spark-master /opt/spark/bin/spark-submit \
                --master {SPARK_MASTER} \
                --deploy-mode client \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
                {SPARK_APPS_DIR}/streaming/streaming_recommender.py
        """,
    )

    # Task 6 — Notify
    t_notify = PythonOperator(
        task_id="notify_summary",
        python_callable=notify_summary,
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    t_check_data >> t_train >> t_evaluate >> t_producer >> t_streaming >> t_notify
