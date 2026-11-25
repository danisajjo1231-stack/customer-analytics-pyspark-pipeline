import os
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

# Spark ko isi Python ko use karne ke liye batao
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


def create_spark_session():
    return (
        SparkSession.builder
        .appName("CustomerAnalytics")
        .master("local[*]")
        .config("spark.pyspark.python", python_path)
        .getOrCreate()
    )


def main():
    project_root = Path(__file__).resolve().parent.parent
    excel_file = project_root / "datasets" / "raw" / "online_retail_II.xlsx"
    bronze_folder = project_root / "datasets" / "bronze"
    bronze_file = bronze_folder / "online_retail_combined.csv"

    if not excel_file.exists():
        raise FileNotFoundError(f"Dataset not found: {excel_file}")

    bronze_folder.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    try:
        print("Spark Version:", spark.version)

        print("Reading Excel sheets...")
        sheet_1 = pd.read_excel(excel_file, sheet_name="Year 2009-2010")
        sheet_2 = pd.read_excel(excel_file, sheet_name="Year 2010-2011")
        combined_data = pd.concat([sheet_1, sheet_2], ignore_index=True)
        print("Total records:", len(combined_data))

        combined_data.to_csv(bronze_file, index=False, encoding="utf-8")
        print("Bronze CSV created successfully!")
        print("Saved at:", bronze_file)

        print("Reading CSV with PySpark...")
        spark_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(bronze_file))
        )

        print("PySpark schema:")
        spark_df.printSchema()

        print("First 5 records:")
        spark_df.show(5, truncate=False)

        print("Total records in PySpark:", spark_df.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()