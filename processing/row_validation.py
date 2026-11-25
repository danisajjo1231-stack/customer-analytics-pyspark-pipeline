from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("ExactRowValidation") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("datasets/silver/online_retail_cleaned")

records = [
    ("489436", "21756"),
    ("489445", "35951"),
    ("489531", "21034"),
    ("489532", "72008"),
    ("489533", "84352"),
    ("489536", "22147"),
    ("489552", "21479"),
    ("489561", "35051B"),
    ("489594", "20977"),
    ("489599", "22321")
]

keys = spark.createDataFrame(records, ["Invoice", "StockCode"])

result = df.join(keys, ["Invoice", "StockCode"], "inner")

result.select(
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country"
).orderBy("Invoice", "Quantity").show(30, truncate=False)
df.select("InvoiceDate").show(10, truncate=False)

print("InvoiceDate Data Type:")
df.printSchema()
print("Spark Session Timezone:")
print(spark.conf.get("spark.sql.session.timeZone"))
spark.stop()



