from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("LeadPulse-Silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .getOrCreate()
)

BRONZE_PATH = "data/bronze/delta_bronze_cdf"
SILVER_PATH = "data/silver/delta_silver"

bronze_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .load(BRONZE_PATH)
)

bronze_changes = bronze_cdf.filter(
    col("_change_type").isin("insert", "update_postimage")
)

silver_df = (
    bronze_changes
    .withColumn("Quantity", col("Quantity").cast(IntegerType()))
    .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))
    .withColumn("InvoiceDate", to_timestamp("InvoiceDate"))
    .withColumn("CustomerID", col("CustomerID").cast(LongType()))
    .filter(col("InvoiceNo").isNotNull())
)

silver_df = silver_df.dropDuplicates(["InvoiceNo", "StockCode"])

# 🚨 DROP CDC METADATA COLUMNS
silver_df = silver_df.drop(
    "_change_type",
    "_commit_version",
    "_commit_timestamp"
)

(
    silver_df.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .save(SILVER_PATH)
)

print("✅ Silver Delta table created successfully")
spark.stop()
