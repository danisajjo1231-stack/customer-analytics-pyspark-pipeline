
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = (
    "/mnt/c/Users/HP/OneDrive/Desktop/"
    "Danish Data/customer_analytics_pyspark"
)

PYTHON = "/home/danish/pyspark_venv/bin/python"


with DAG(
    dag_id="customer_analytics_pipeline",
    description="Automated PySpark customer analytics pipeline",
    start_date=pendulum.datetime(
        2026, 10, 28, tz="Asia/Karachi"
    ),
    schedule="0 2 * * *",
    catchup=False,
    default_args={
        "owner": "danish",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["PySpark", "Data Engineering", "Customer Analytics"],
) as dag:

    business_analysis = BashOperator(
        task_id="customer_business_analysis",
        bash_command=(
            f'cd "{PROJECT_DIR}" && '
            f'"{PYTHON}" '
            "processing/customer_business_analysis.py"
        ),
    )

    validate_silver = BashOperator(
        task_id="validate_silver_parquet",
        bash_command=f"""
set -e
cd "{PROJECT_DIR}"

"{PYTHON}" -c '
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("AirflowSilverValidation")
    .master("local[*]")
    .getOrCreate()
)

try:
    df = spark.read.parquet(
        "datasets/silver/online_retail_cleaned"
    )
    total = df.count()

    print(f"Silver Parquet rows: {{total:,}}")

    if total != 1033036:
        raise ValueError(
            f"Expected 1,033,036 rows, found {{total:,}}"
        )

    print("Silver validation PASSED")

finally:
    spark.stop()
'
""",
    )

    business_analysis >> validate_silver

