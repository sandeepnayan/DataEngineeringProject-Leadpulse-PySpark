from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

spark = (
    SparkSession.builder
    .appName("LeadPulse-Gold-Refinement")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .getOrCreate()
)

STATUS_PATH = "data/gold/gold_lead_status_history"
AGENT_PATH = "data/gold/gold_lead_agent_history"
CURRENT_PATH = "data/gold/gold_lead_current"
KPI_PATH = "data/gold/gold_customer_performance"
CHECKPOINT = "data/gold/_silver_cdf_checkpoint.txt"

# --------------------------------------------------
# 1. ENSURE CHECKPOINT EXISTS
# --------------------------------------------------
if not os.path.exists(CHECKPOINT):
    with open(CHECKPOINT, "w") as f:
        f.write("0")

# ==================================================
# 2. FIX SCD2 STATUS — DEDUP IsLast FLAG
# ==================================================
status_df = spark.read.format("delta").load(STATUS_PATH)

status_window = Window.partitionBy("LeadID").orderBy(col("StatusStartDate").desc())

status_dedup = (
    status_df
    .withColumn("rn", row_number().over(status_window))
    .withColumn("IsLastStatus", col("rn") == 1)
    .withColumn("IsActive", col("rn") == 1)
    .drop("rn")
)

status_dedup.write.format("delta").mode("overwrite").save(STATUS_PATH)

# ==================================================
# 3. FIX SCD2 AGENT — DEDUP IsLastAssignedAgent
# ==================================================
agent_df = spark.read.format("delta").load(AGENT_PATH)

agent_window = Window.partitionBy("LeadID").orderBy(col("AssignmentStartDate").desc())

agent_dedup = (
    agent_df
    .withColumn("rn", row_number().over(agent_window))
    .withColumn("IsLastAssignedAgent", col("rn") == 1)
    .drop("rn")
)

agent_dedup.write.format("delta").mode("overwrite").save(AGENT_PATH)

# ==================================================
# 4. CUSTOMER PERFORMANCE — RANK & DENSE RANK
# ==================================================
current_df = spark.read.format("delta").load(CURRENT_PATH)

kpi_df = (
    current_df
    .withColumn("EngagementScore", col("TotalCalls") + col("TotalChats"))
)

rank_window = Window.orderBy(col("EngagementScore").desc())
dense_rank_window = Window.orderBy(col("EngagementScore").desc())

kpi_ranked = (
    kpi_df
    .withColumn("rank", rank().over(rank_window))
    .withColumn("dense_rank", dense_rank().over(dense_rank_window))
)

kpi_ranked.write.format("delta").mode("overwrite").save(KPI_PATH)

print("✅ Gold SCD deduplication & ranking completed successfully")
spark.stop()

