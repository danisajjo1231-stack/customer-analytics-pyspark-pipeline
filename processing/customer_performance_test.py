import time

from pyspark.sql.functions import count

from customer_data_cleaning import (
    create_spark_session,
    clean_customer_data
)


def main():

    spark = create_spark_session()

    print(
    "Original shuffle partitions:",
    spark.conf.get("spark.sql.shuffle.partitions")
)

    spark.conf.set("spark.sql.shuffle.partitions", "8")

    print(
    "Updated shuffle partitions:",
    spark.conf.get("spark.sql.shuffle.partitions")
)
    
    try:
        # Step 1: Clean the dataset
        df_silver = clean_customer_data(spark)

        print("\n===== PERFORMANCE TEST =====")

        # Step 2: Cache data so both tests can reuse it
        df_silver.cache()

        # Materialize the cache before timing the tests
        df_silver.count()

        # Step 3: Test with 4 partitions
        df_4 = df_silver.repartition(4)
        df_4.groupBy("Country").count().explain("formatted")
        
        start_time = time.perf_counter()
        

        df_4.groupBy("Country").agg(
            count("*").alias("transaction_count")
        ).count()

        time_4 = time.perf_counter() - start_time

        print(f"4 partitions time: {time_4:.2f} seconds")

        # Step 4: Test with 8 partitions
        df_8 = df_silver.repartition(8)

        start_time = time.perf_counter()

        df_8.groupBy("Country").agg(
            count("*").alias("transaction_count")
        ).count()

        time_8 = time.perf_counter() - start_time

        print(f"8 partitions time: {time_8:.2f} seconds")

        # Step 5: Compare results
        print("\n===== PERFORMANCE COMPARISON =====")

        if time_4 < time_8:
            print("4 partitions were faster in this test.")
        elif time_8 < time_4:
            print("8 partitions were faster in this test.")
        else:
            print("Both tests took approximately the same time.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

