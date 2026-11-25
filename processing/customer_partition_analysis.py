
from customer_data_cleaning import (
    create_spark_session,
    clean_customer_data
)


def main():

    spark = create_spark_session()

    try:
        # Step 1: Cleaned data hasil karo
        df_silver = clean_customer_data(spark)

        print("\n===== PARTITION ANALYSIS =====")

        # Step 2: Current number of partitions
        current_partitions = df_silver.rdd.getNumPartitions()

        print("Current partitions:", current_partitions)

        # Step 3: Har partition mein kitni rows hain?
        partition_counts = (
            df_silver
            .select("Invoice")
            .rdd
            .mapPartitions(lambda rows: [sum(1 for _ in rows)])
            .collect()
        )

        print("\nRows in each partition:")
        for index, row_count in enumerate(partition_counts):
            print(f"Partition {index}: {row_count} rows")

        # Step 4: Data ko 8 partitions mein repartition karo
        df_repartitioned = df_silver.repartition(8)

        print("\nPartitions after repartition:", 
              df_repartitioned.rdd.getNumPartitions())

        # Step 5: Repartitioned data par simple analysis
        print("\n===== COUNTRY ANALYSIS =====")

        df_repartitioned.groupBy("Country").count().show(10)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

