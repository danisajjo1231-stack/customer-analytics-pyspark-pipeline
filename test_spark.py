import os
import sys

# Use the same Python interpreter for Spark workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PythonWorkerTest")
    .master("local[1]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Python executable:", sys.executable)
print("Spark version:", spark.version)

data = [(1, "Ali"), (2, "Sara"), (3, "Ahmed")]
df = spark.createDataFrame(data, ["id", "name"])

df.show()

# Test Python worker execution
result = spark.sparkContext.parallelize([1, 2, 3], 1).map(
    lambda number: number * 2
).collect()

print("Python worker test:", result)

spark.stop()