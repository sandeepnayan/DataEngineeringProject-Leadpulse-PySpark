from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("LeadPulse-Gold-LateArrivals")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .getOrCreate()
)

GOLD_STATUS = "data/gold/gold_lead_status_history"
GOLD_AGENT = "data/gold/gold_lead_agent_history"

# -----------------------------------------
# 1. STATUS LATE ARRIVALS
# -----------------------------------------
status_df = spark.read.format("delta").load(GOLD_STATUS)

status_window = Window.partitionBy("LeadID").orderBy(col("StatusStartDate").desc())
status_fixed = status_df.withColumn("rn", row_number().over(status_window)) \
                        .withColumn("IsLastStatus", col("rn") == 1) \
                        .drop("rn")

status_fixed.write.format("delta").mode("overwrite").save(GOLD_STATUS)

# -----------------------------------------
# 2. AGENT LATE ARRIVALS
# -----------------------------------------
agent_df = spark.read.format("delta").load(GOLD_AGENT)

agent_window = Window.partitionBy("LeadID").orderBy(col("AssignmentStartDate").desc())
agent_fixed = agent_df.withColumn("rn", row_number().over(agent_window)) \
                      .withColumn("IsLastAssignedAgent", col("rn") == 1) \
                      .drop("rn")

agent_fixed.write.format("delta").mode("overwrite").save(GOLD_AGENT)

print("✅ Late-arriving corrections applied successfully")
spark.stop()

