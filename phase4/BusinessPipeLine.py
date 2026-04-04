from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Phase4_Business_Pipeline") \
    .getOrCreate()

# Load Data
customers = spark.read.csv("/samples/customers.csv", header=True, inferSchema=True)
sales = spark.read.csv("/samples/sales.csv", header=True, inferSchema=True)

# Cleaning
customers = customers.dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])
sales = sales.dropna(subset=["customer_id"]).dropDuplicates(["sale_id"]).filter(F.col("total_amount") > 0)

# Create Name
customers = customers.withColumn(
    "customer_name",
    F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
)

# Join once (optimized)
df = sales.join(customers, "customer_id")

# Task 1: Daily Sales
daily_sales = df.groupBy("sale_date") \
    .agg(F.sum("total_amount").alias("total_sales"))

# Task 2: City Revenue
city_revenue = df.groupBy("city") \
    .agg(F.sum("total_amount").alias("total_revenue"))

# Task 3: Top Customers
customer_spend = df.groupBy("customer_id", "customer_name", "city") \
    .agg(F.sum("total_amount").alias("total_spend"))

top_customers = customer_spend.orderBy(F.desc("total_spend")).limit(5)

# Task 4: Repeat Customers
order_counts = df.groupBy("customer_id") \
    .agg(F.count("sale_id").alias("order_count"))

repeat_customers = order_counts.filter(F.col("order_count") > 1)

# Task 5: Segmentation
segmentation = customer_spend.withColumn(
    "segment",
    F.when(F.col("total_spend") > 10000, "Gold")
     .when((F.col("total_spend") >= 5000) & (F.col("total_spend") <= 10000), "Silver")
     .otherwise("Bronze")
)

# Task 6: Final Report
final_df = segmentation.join(order_counts, "customer_id") \
    .select("customer_name", "city", "total_spend", "order_count", "segment")

# Task 7: Save
final_df.write.mode("overwrite").csv("/tmp/report", header=True)

# Output
daily_sales.show()
city_revenue.show()
top_customers.show()
final_df.show()
