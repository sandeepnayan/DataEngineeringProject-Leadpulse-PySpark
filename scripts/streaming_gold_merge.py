from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("LeadPulse-Gold-Streaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .getOrCreate()
)

SILVER_PATH = "data/silver/delta_silver"
GOLD_CURRENT = "data/gold/gold_lead_current"

# Read Silver with CDF
silver_stream = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load(SILVER_PATH)

# Merge into Gold
if DeltaTable.isDeltaTable(spark, GOLD_CURRENT):
    gold_table = DeltaTable.forPath(spark, GOLD_CURRENT)
    gold_table.alias("gold").merge(
        silver_stream.alias("silver"),
        "gold.LeadID = silver.LeadID"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    silver_stream.write.format("delta").save(GOLD_CURRENT)

print("✅ Streaming MERGE applied to Gold Current")
spark.stop()

