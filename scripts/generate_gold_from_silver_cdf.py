from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

spark = (
    SparkSession.builder
    .appName("LeadPulse-Gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .getOrCreate()
)

SILVER_PATH = "data/silver/delta_silver"

GOLD_CURRENT = "data/gold/gold_lead_current"
GOLD_STATUS = "data/gold/gold_lead_status_history"
GOLD_AGENT = "data/gold/gold_lead_agent_history"

CHECKPOINT = "data/gold/_silver_cdf_checkpoint.txt"

# --------------------------------------------------
# 1. Read last processed version
# --------------------------------------------------
last_version = int(open(CHECKPOINT).read()) if os.path.exists(CHECKPOINT) else 0

silver_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_version)
    .load(SILVER_PATH)
)

changes = silver_cdf.filter(
    col("_change_type").isin("insert", "update_postimage")
)

# --------------------------------------------------
# 2. Enrich data (simulate lead concepts)
# --------------------------------------------------
leads = (
    changes
    .withColumnRenamed("InvoiceNo", "LeadID")
    .withColumn("Status", when(col("Quantity") > 5, "QUALIFIED").otherwise("NEW"))
    .withColumn("StatusChangeDate", col("InvoiceDate"))
    .withColumn("AssignedAgent", concat(lit("Agent-"), (col("CustomerID") % 3)))
    .withColumn("AssignmentDate", col("InvoiceDate"))
    .withColumn("Calls", lit(1))
    .withColumn("Chats", lit(1))
)

# ==================================================
# SCD TYPE 1 — CURRENT LEAD SNAPSHOT
# ==================================================
current_df = (
    leads
    .groupBy("LeadID")
    .agg(
        last("Status").alias("CurrentStatus"),
        last("AssignedAgent").alias("CurrentAgent"),
        sum("Calls").alias("TotalCalls"),
        sum("Chats").alias("TotalChats"),
        max("InvoiceDate").alias("LastUpdatedAt")
    )
)

if not os.path.exists(GOLD_CURRENT):
    current_df.write.format("delta").mode("overwrite").save(GOLD_CURRENT)
else:
    spark.sql(f"""
        MERGE INTO delta.`{GOLD_CURRENT}` t
        USING current_df s
        ON t.LeadID = s.LeadID
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# ==================================================
# SCD TYPE 2 — STATUS HISTORY
# ==================================================
status_window = Window.partitionBy("LeadID").orderBy("StatusChangeDate")

status_df = (
    leads
    .select("LeadID", "Status", "StatusChangeDate")
    .withColumn("next_date", lead("StatusChangeDate").over(status_window))
    .withColumn("StatusEndDate", col("next_date"))
    .withColumn("IsActive", col("next_date").isNull())
    .withColumn("IsLastStatus", col("next_date").isNull())
    .drop("next_date")
    .withColumnRenamed("StatusChangeDate", "StatusStartDate")
)

status_df.write.format("delta").mode("overwrite").save(GOLD_STATUS)

# ==================================================
# SCD TYPE 2 — AGENT HISTORY
# ==================================================
agent_window = Window.partitionBy("LeadID").orderBy("AssignmentDate")

agent_df = (
    leads
    .select("LeadID", "AssignedAgent", "AssignmentDate")
    .withColumn("next_date", lead("AssignmentDate").over(agent_window))
    .withColumn("AssignmentEndDate", col("next_date"))
    .withColumn("IsLastAssignedAgent", col("next_date").isNull())
    .drop("next_date")
    .withColumnRenamed("AssignmentDate", "AssignmentStartDate")
)

agent_df.write.format("delta").mode("overwrite").save(GOLD_AGENT)

# --------------------------------------------------
# 3. Update checkpoint
# --------------------------------------------------
max_version = silver_cdf.agg(max("_commit_version")).collect()[0][0]
if max_version is not None:
    with open(CHECKPOINT, "w") as f:
        f.write(str(max_version + 1))

print("✅ Gold layer (SCD1 + SCD2) created successfully")
spark.stop()

