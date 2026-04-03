# Read a CSV file from /samples/
df=(spark.read.format("csv")
    .option("header","true")
    .load("/samples/customers.csv"))
-------------------------------------------
#Inspect Schema in PySpark
df.show()
df.printSchema()
-------------------------------------------
#Identify missing values
df.filter(col("column").isNull()).show()
df.filter(col("age").isNull() | col("name").isNull()).show()
