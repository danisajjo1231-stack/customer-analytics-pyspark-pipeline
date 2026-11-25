import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    lit,
    to_timestamp
)

# Spark ko isi Python ko use karne ke liye batao
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

def create_spark_session():
    """Create and return a Spark session."""

    spark = (
        SparkSession.builder
        .appName("CustomerAnalytics")
        .master("local[*]")
        .getOrCreate()
    )

    return spark

def clean_customer_data(spark):

    # 2. Read Bronze data
    df = spark.read.csv(
        "datasets/bronze/online_retail_combined.csv",
        header=True,
        inferSchema=True,
    )

    # 3. Basic information
    total_rows = df.count()

    print("\n===== BASIC INFORMATION =====")
    print("Total Rows:", total_rows)
    print("Total Columns:", len(df.columns))
    print("Column Names:", df.columns)

    print("\n===== DATA TYPES =====")
    df.printSchema()

    # 4. Statistical summary
    print("\n===== STATISTICAL SUMMARY =====")
    df.describe().show(truncate=False)

    # 5. Missing values
    print("\n===== MISSING VALUES =====")

    for column in df.columns:
        missing = df.filter(
            col(column).isNull()
            | (trim(col(column).cast("string")) == "")
        ).count()

        percentage = (missing / total_rows) * 100

        print(
            column,
            "| Missing:", missing,
            "| Percentage:", round(percentage, 2), "%",
        )

    # 6. Exact duplicate rows
    print("\n===== DUPLICATE RECORDS =====")

    unique_rows = df.dropDuplicates().count()

    print("Total Rows:", total_rows)
    print("Unique Rows:", unique_rows)
    print("Duplicate Rows:", total_rows - unique_rows)

    # 7. Invalid values
    print("\n===== INVALID VALUE CHECKS =====")

    print("Negative Quantity:", df.filter(col("Quantity") < 0).count())
    print("Zero Quantity:", df.filter(col("Quantity") == 0).count())
    print("Negative Price:", df.filter(col("Price") < 0).count())
    print("Zero Price:", df.filter(col("Price") == 0).count())

    # Show negative quantity examples
    print("\n===== NEGATIVE QUANTITY EXAMPLES =====")

    df.filter(col("Quantity") < 0).select(
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Customer ID"
    ).show(10, truncate=False)

    # 7A. Cancellation Invoice Analysis
    print("\n===== CANCELLATION INVOICE ANALYSIS =====")

    cancellation_records = df.filter(
        col("Invoice").startswith("C")
    )

    print(
        "Cancellation Invoice Records:",
        cancellation_records.count()
    )

    print(
        "Cancellation Records with Negative Quantity:",
        cancellation_records.filter(
            col("Quantity") < 0
        ).count()
    )

    print("\n===== CANCELLATION INVOICE EXAMPLES =====")

    cancellation_records.select(
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "Price",
        "Customer ID"
    ).show(10, truncate=False)

    # 7B. Duplicate Record Investigation
    print("\n===== DUPLICATE RECORD EXAMPLES =====")

    duplicate_df = df.groupBy(df.columns).count().filter(
        col("count") > 1
    )

    duplicate_df.show(10, truncate=False)

    # Show negative price examples
    print("\n===== NEGATIVE PRICE EXAMPLES =====")

    df.filter(col("Price") < 0).select(
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Customer ID"
    ).show(10, truncate=False)

    # Show zero price examples
    print("\n===== ZERO PRICE EXAMPLES =====")

    df.filter(col("Price") == 0).select(
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Customer ID"
    ).show(10, truncate=False)

    # 8. Customer ID check
    print("\n===== MISSING CUSTOMER ID INVESTIGATION =====")

    missing_customer_df = df.filter(
        col("Customer ID").isNull()
    )

    print(
        "Records with Missing Customer ID:",
        missing_customer_df.count()
    )

    print("\n===== MISSING CUSTOMER ID EXAMPLES =====")

    missing_customer_df.select(
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Country"
    ).show(10, truncate=False)

    print("\n===== MISSING CUSTOMER ID: BUSINESS ANALYSIS =====")

    # Missing Customer ID aur negative quantity
    print(
        "Missing Customer ID + Negative Quantity:",
        df.filter(
            col("Customer ID").isNull() &
            (col("Quantity") < 0)
        ).count()
    )

    # Missing Customer ID aur positive quantity
    print(
        "Missing Customer ID + Positive Quantity:",
        df.filter(
            col("Customer ID").isNull() &
            (col("Quantity") > 0)
        ).count()
    )

    # Missing Customer ID aur zero price
    print(
        "Missing Customer ID + Zero Price:",
        df.filter(
            col("Customer ID").isNull() &
            (col("Price") == 0)
        ).count()
    )

    # Missing Customer ID aur positive price
    print(
        "Missing Customer ID + Positive Price:",
        df.filter(
            col("Customer ID").isNull() &
            (col("Price") > 0)
        ).count()
    )

    print("\n===== MISSING CUSTOMER ID + MISSING PRICE =====")

    missing_customer_missing_price = df.filter(
        col("Customer ID").isNull() &
        col("Price").isNull()
    )

    print(
        "Records with Missing Customer ID and Missing Price:",
        missing_customer_missing_price.count()
    )

    missing_customer_missing_price.select(
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Country"
    ).show(10, truncate=False)

    print("\n===== MISSING CUSTOMER ID + NEGATIVE PRICE =====")

    missing_customer_negative_price = df.filter(
        col("Customer ID").isNull() &
        (col("Price") < 0)
    )

    print(
        "Records with Missing Customer ID and Negative Price:",
        missing_customer_negative_price.count()
    )

    missing_customer_negative_price.select(
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Country"
    ).show(10, truncate=False)

    df = df.withColumn(
        "transaction_type",
        when(
            (col("Description") == "Adjust bad debt") &
            (col("Price") < 0),
            "ACCOUNTING_ADJUSTMENT"
        ).otherwise("OTHER")
    )

    # Display the result
    print("===== TRANSACTION TYPE RESULTS =====")

    df.filter(
        col("Description") == "Adjust bad debt"
    ).select(
        "Invoice",
        "Description",
        "Quantity",
        "Price",
        "transaction_type"
    ).show(10, truncate=False)

    # Count accounting adjustments
    print("===== ACCOUNTING ADJUSTMENT COUNT =====")

    print(
        "Accounting adjustments:",
        df.filter(
            col("transaction_type") == "ACCOUNTING_ADJUSTMENT"
        ).count()
    )

    # ==========================================
    # STEP: IN-MEMORY DATA CLEANING
    # ==========================================

    print("\n===== STEP 1: REMOVE EXACT DUPLICATES =====")

    # Original source ka row count
    bronze_count = df.count()

    # Derived transaction_type column ko hata kar original data par deduplication
    original_columns = [
        column for column in df.columns
        if column != "transaction_type"
    ]

    df_silver = df.select(*original_columns).dropDuplicates()

    silver_count = df_silver.count()

    print("Original Bronze rows:", bronze_count)
    print("Rows after deduplication:", silver_count)
    print("Duplicate rows removed:", bronze_count - silver_count)

    print("\n===== STEP 2: CLEAN DESCRIPTIONS =====")

    # Blank aur missing descriptions ko identify karke Unknown set karna
    df_silver = df_silver.withColumn(
        "Description",
        when(
            col("Description").isNull()
            | (trim(col("Description")) == ""),
            lit("Unknown")
        ).otherwise(trim(col("Description")))
    )

    print(
        "Unknown descriptions:",
        df_silver.filter(
            col("Description") == "Unknown"
        ).count()
    )

    print("\n===== STEP 3: CLASSIFY TRANSACTIONS =====")

    # Invoice C se start hone par cancellation flag
    df_silver = df_silver.withColumn(
        "is_cancelled_invoice",
        upper(trim(col("Invoice").cast("string"))).startswith("C")
    )

    # Negative quantity ko return/refund-related flag
    df_silver = df_silver.withColumn(
        "is_return",
        col("Quantity") < 0
    )

    # Zero aur negative prices ko flag karna
    df_silver = df_silver.withColumn(
        "is_zero_price",
        col("Price") == 0
    )

    df_silver = df_silver.withColumn(
        "is_negative_price",
        col("Price") < 0
    )

    # Accounting adjustments ko identify karna
    df_silver = df_silver.withColumn(
        "is_accounting_adjustment",
        (col("Description") == "Adjust bad debt")
        & (col("Price") < 0)
    )

    # Priority-based transaction classification
    df_silver = df_silver.withColumn(
        "transaction_type",
        when(
            col("is_accounting_adjustment"),
            "ACCOUNTING_ADJUSTMENT"
        ).when(
            col("is_cancelled_invoice"),
            "CANCELLED_INVOICE"
        ).when(
            col("is_return"),
            "RETURN_OR_REFUND"
        ).when(
            col("is_zero_price"),
            "ZERO_PRICE_TRANSACTION"
        ).when(
            col("is_negative_price"),
            "NEGATIVE_PRICE_TRANSACTION"
        ).otherwise(
            "REGULAR_TRANSACTION"
        )
    )

    print("\n===== STEP 4: CUSTOMER ID QUALITY =====")

    # Missing Customer IDs ko delete nahi karna
    df_silver = df_silver.withColumn(
        "customer_id_status",
        when(
            col("Customer ID").isNull(),
            "MISSING_CUSTOMER_ID"
        ).otherwise(
            "KNOWN_CUSTOMER"
        )
    )

    print("\n===== STEP 5: VALIDATION =====")

    print("Rows after deduplication:", silver_count)

    print(
        "Missing Customer IDs:",
        df_silver.filter(
            col("Customer ID").isNull()
        ).count()
    )

    print(
        "Unknown descriptions:",
        df_silver.filter(
            col("Description") == "Unknown"
        ).count()
    )

    print(
        "Negative Quantity:",
        df_silver.filter(
            col("Quantity") < 0
        ).count()
    )

    print(
        "Zero Price:",
        df_silver.filter(
            col("Price") == 0
        ).count()
    )

    print(
        "Negative Price:",
        df_silver.filter(
            col("Price") < 0
        ).count()
    )

    print(
        "Accounting Adjustments:",
        df_silver.filter(
            col("is_accounting_adjustment")
        ).count()
    )

    print("\n===== TRANSACTION CLASSIFICATION =====")

    df_silver.groupBy(
        "transaction_type"
    ).count().orderBy(
        col("count").desc()
    ).show(truncate=False)

    print("\n===== CLEANING COMPLETED =====")

    # Silver output ko Parquet format mein save karna
    silver_path = "datasets/silver/online_retail_cleaned"

    print("\n===== SAVING SILVER DATA AS PARQUET =====")

    df_silver.write \
        .mode("overwrite") \
        .parquet(silver_path)

    print("Parquet data saved successfully:", silver_path)

    # Saved Parquet data ko dobara read karna
    df_parquet = spark.read.parquet(silver_path)

    # Row count verify karna
    parquet_count = df_parquet.count()

    print("\n===== PARQUET VALIDATION =====")
    print("Cleaned DataFrame rows:", silver_count)
    print("Parquet rows:", parquet_count)

    # Row counts match hone chahiye
    if silver_count != parquet_count:
        raise ValueError(
            "Validation failed: Silver and Parquet row counts do not match."
        )

    print("Row count validation: PASSED")

    # Saved data ka schema check karna
    print("\n===== PARQUET SCHEMA =====")
    df_parquet.printSchema()

    return df_parquet


def main():
    spark = create_spark_session()

    try:
        df_silver = clean_customer_data(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()