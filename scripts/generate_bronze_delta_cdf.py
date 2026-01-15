from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("LeadPulse-Bronze")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.jars.packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.1.0",
            "com.crealytics:spark-excel_2.12:3.5.1_0.20.4"
        ])
    )
    .getOrCreate()
)

RAW_PATH = "data/raw/Online Retail.xlsx"
BRONZE_PATH = "data/bronze/delta_bronze_cdf"

# ✅ Bronze schema — ALL STRING (best practice)
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True),
])

# ✅ Spark-Excel SAFE read
df = (
    spark.read.format("excel")
    .schema(schema)                     # 🔑 no inferSchema
    .option("header", "true")
    .option("dataAddress", "'Sheet1'!A1")
    .option("maxRowsInMemory", 1000)     # 🔑 streaming
    .option("workbookPassword", None)
    .load(RAW_PATH)
)

# Normalize column names
df = df.selectExpr(
    *[f"`{c}` as {c.strip().replace(' ', '_')}" for c in df.columns]
)

# Write Bronze Delta with CDF
(
    df.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .save(BRONZE_PATH)
)

print("✅ Bronze Delta table created successfully with CDF")
spark.stop()
