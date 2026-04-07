# Task 1: Top 3 Customers per City
from pyspark.sql.functions import sum, col, dense_rank
from pyspark.sql.window import Window

# Step 1: Join orders, customers, payments
df = orders.join(customers, "customer_id") \
           .join(payments, "order_id")

# Step 2: Calculate total spend per customer per city
customer_spend = df.groupBy("customer_id", "customer_city") \
    .agg(sum("payment_value").alias("total_spent"))

# Step 3: Define window (partition by city)
window_spec = Window.partitionBy("customer_city").orderBy(col("total_spent").desc())

# Step 4: Rank customers
ranked_df = customer_spend.withColumn("rank", dense_rank().over(window_spec))

# Step 5: Filter Top 3
top_3_customers = ranked_df.filter(col("rank") <= 3)

# Step 6: Show result
top_3_customers.show()


# Task 2: Running Total of Sales
from pyspark.sql.functions import sum, col, to_date
from pyspark.sql.window import Window

# Step 1: Join orders + payments
df = orders.join(payments, "order_id")

# Step 2: Convert timestamp to date
df = df.withColumn("order_date", to_date("order_purchase_timestamp"))

# Step 3: Aggregate daily sales
daily_sales = df.groupBy("order_date") \
    .agg(sum("payment_value").alias("daily_sales"))

# Step 4: Define window (order by date)
window_spec = Window.orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Step 5: Running total
running_total_df = daily_sales.withColumn(
    "running_total",
    sum("daily_sales").over(window_spec)
)

# Step 6: Show result
running_total_df.show()


# Task 3: Top Products per Category
from pyspark.sql.functions import sum, col, dense_rank
from pyspark.sql.window import Window

# Step 1: Join order_items + products
df = order_items.join(products, "product_id")

# Step 2: Calculate total sales per product per category
product_sales = df.groupBy("product_id", "product_category_name") \
    .agg(sum("price").alias("total_sales"))

# Step 3: Define window (partition by category)
window_spec = Window.partitionBy("product_category_name") \
    .orderBy(col("total_sales").desc())

# Step 4: Rank products within category
ranked_df = product_sales.withColumn(
    "rank",
    dense_rank().over(window_spec)
)

# Step 5: Filter Top 3 products per category
top_products = ranked_df.filter(col("rank") <= 3)

# Step 6: Show result
top_products.show()


# Task 4: Customer Lifetime Value
from pyspark.sql.functions import sum, col

# Step 1: Join orders + payments
df = orders.join(payments, "order_id")

# Step 2: Calculate total spend per customer
clv_df = df.groupBy("customer_id") \
    .agg(sum("payment_value").alias("customer_lifetime_value"))

# Step 3: Sort customers by highest CLV
clv_df = clv_df.orderBy(col("customer_lifetime_value").desc())

# Step 4: Show result
clv_df.show()


# Task 5: Customer Segmentation
from pyspark.sql.functions import sum, col, when

# Step 1: Join orders + payments
df = orders.join(payments, "order_id")

# Step 2: Calculate CLV
clv_df = df.groupBy("customer_id") \
    .agg(sum("payment_value").alias("CLV"))

# Step 3: Segment customers
segmented_df = clv_df.withColumn(
    "segment",
    when(col("CLV") >= 10000, "High Value")
    .when((col("CLV") >= 5000) & (col("CLV") < 10000), "Medium Value")
    .otherwise("Low Value")
)

# Step 4: Show result
segmented_df.show()
