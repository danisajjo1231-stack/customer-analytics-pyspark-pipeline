from pyspark.sql.functions import col
from customer_data_cleaning import (
    create_spark_session,
    clean_customer_data
)


def main():

    spark = create_spark_session()

    try:
        # Step 1: Cleaned DataFrame hasil karo
        df_silver = clean_customer_data(spark)

        # Step 2: DataFrame ko SQL temporary view banao
        df_silver.createOrReplaceTempView(
            "customer_transactions"
        )

        print("\n===== SPARK SQL: TOP 10 CUSTOMERS =====")

        # Step 3: SQL query execute karo
        top_customers = spark.sql("""
            SELECT
                `Customer ID`,
                COUNT(DISTINCT Invoice) AS invoice_count,
                SUM(Quantity) AS net_quantity,
                ROUND(SUM(Quantity * Price), 2)
                    AS transaction_value
            FROM customer_transactions
            WHERE `Customer ID` IS NOT NULL
            GROUP BY `Customer ID`
            ORDER BY transaction_value DESC
            LIMIT 10
        """)

        top_customers.show(truncate=False)

        print("\n===== SPARK SQL: TRANSACTION TYPES =====")

        # Step 4: Har transaction type ka count
        transaction_types = spark.sql("""
            SELECT
                transaction_type,
                COUNT(*) AS record_count
            FROM customer_transactions
            GROUP BY transaction_type
            ORDER BY record_count DESC
        """)

        transaction_types.show(truncate=False)

        print("\n===== SPARK SQL: COUNTRY SUMMARY =====")

        # Step 5: Country-wise transactions
        country_summary = spark.sql("""
            SELECT
                Country,
                COUNT(*) AS transaction_rows,
                COUNT(DISTINCT Invoice) AS invoice_count,
                SUM(Quantity) AS total_quantity
            FROM customer_transactions
            GROUP BY Country
            ORDER BY transaction_rows DESC
            LIMIT 10
        """)

        country_summary.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

