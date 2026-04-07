from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

spark = SparkSession.builder.appName("Phase4A_Segmentation").getOrCreate()

# Load
customers = spark.read.option("header", True).option("inferSchema", True).csv("/samples/customers.csv")
sales = spark.read.option("header", True).option("inferSchema", True).csv("/samples/sales.csv")

# Clean
customers = customers.dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])
sales = sales.dropna(subset=["customer_id"]).dropDuplicates(["sale_id"]).filter(F.col("total_amount") > 0)

# Total spend + join ONCE
df = sales.groupBy("customer_id") \
    .agg(F.sum("total_amount").alias("total_spend")) \
    .join(customers, "customer_id")

df = df.withColumn("customer_name", F.concat_ws(" ", "first_name", "last_name"))

# TASK 1: Conditional
df_conditional = df.withColumn(
    "segment",
    F.when(F.col("total_spend") > 10000, "Gold")
     .when((F.col("total_spend") >= 5000) & (F.col("total_spend") <= 10000), "Silver")
     .otherwise("Bronze")
)

# TASK 2: Segment Count
segment_count = df_conditional.groupBy("segment") \
    .agg(F.count("*").alias("customer_count"))

# TASK 3: Quantile
q1, q2 = df.approxQuantile("total_spend", [0.33, 0.66], 0)

df_quantile = df.withColumn(
    "segment_quantile",
    F.when(F.col("total_spend") <= q1, "Bronze")
     .when(F.col("total_spend") <= q2, "Silver")
     .otherwise("Gold")
)

# TASK 4: Bucketizer
splits = [-float("inf"), 5000, 10000, float("inf")]
bucketizer = Bucketizer(splits=splits, inputCol="total_spend", outputCol="bucket")

df_bucket = bucketizer.transform(df).withColumn(
    "segment_bucket",
    F.when(F.col("bucket") == 0, "Bronze")
     .when(F.col("bucket") == 1, "Silver")
     .otherwise("Gold")
)

# TASK 5: Window Rank
window = Window.orderBy("total_spend")

df_window = df.withColumn("rank_pct", percent_rank().over(window)).withColumn(
    "segment_rank",
    F.when(F.col("rank_pct") <= 0.33, "Bronze")
     .when(F.col("rank_pct") <= 0.66, "Silver")
     .otherwise("Gold")
)

# TASK 6: Compare (optimized joins)
comparison = df_conditional \
    .join(df_quantile.select("customer_id", "segment_quantile"), "customer_id") \
    .join(df_bucket.select("customer_id", "segment_bucket"), "customer_id") \
    .join(df_window.select("customer_id", "segment_rank"), "customer_id") \
    .select("customer_name", "total_spend", "segment", "segment_quantile", "segment_bucket", "segment_rank")

# Output
df_conditional.select("customer_name", "total_spend", "segment").show()
segment_count.show()
df_quantile.select("customer_name", "total_spend", "segment_quantile").show()
df_bucket.select("customer_name", "total_spend", "segment_bucket").show()
df_window.select("customer_name", "total_spend", "rank_pct", "segment_rank").show()
comparison.show()
