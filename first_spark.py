from pyspark.sql import SparkSession

# Create a Spark session

spark = SparkSession.builder \
    .appName("CustomerAnalytics") \
    .master("local[*]") \
    .getOrCreate()
print("Apache Spark started successfully!")
print("Spark Version:", spark.version)