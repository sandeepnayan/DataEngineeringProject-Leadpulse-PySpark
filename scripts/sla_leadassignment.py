from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("LeadPulse-Gold-SLA")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .getOrCreate()
)

GOLD_AGENT = "data/gold/gold_lead_agent_history"
SLA_PATH = "data/gold/gold_lead_sla"

df = spark.read.format("delta").load(GOLD_AGENT)

window = Window.partitionBy("LeadID").orderBy("AssignmentStartDate")

sla_df = df.withColumn("prev_assign_date", lag("AssignmentStartDate").over(window)) \
           .withColumn("days_since_last_assign", datediff("AssignmentStartDate", "prev_assign_date")) \
           .withColumn("SLA_breach", col("days_since_last_assign") > 2)  # SLA = 2 days threshold

sla_df.write.format("delta").mode("overwrite").save(SLA_PATH)

print("✅ SLA breach detection complete")
spark.stop()

