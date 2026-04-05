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
from pyspark.sql.functions import col
df.filter(col("column").isNull()).show()
df.filter(col("age").isNull() | col("name").isNull()).show()
-------------------------------------------------------------
#Clean data using dropna() 

#Remove rows with ANY null
df_clean = df.dropna()

#Remove rows only if ALL values are null
df_clean = df.dropna(how="all")

#Remove based on specific columns
df_clean = df.dropna(subset=["age", "name"])
---------------------------------------------------------------
#Replace NULL values

#Fill all nulls with zero
df_filled = df.fillna(0)

#Fill specific column
df_filled = df.fillna({"age": 0})
------------------------------------------------------------------
#Read JSON and Parquet sample files
df_json = (
    spark.read
    .option("multiline", "true")
    .json("/samples/file.json")
)
df_json.show()
df_parquet = spark.read.parquet("/samples/file.parquet")
df_parquet.show()
df_parquet.printSchema()
------------------------------------------------------------------
