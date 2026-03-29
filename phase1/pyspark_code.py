dataframe creating

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Practice").getOrCreate()

customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28)
], ["customer_id", "customer_name", "city", "age"])

#To show all customers
customers.show()

#Show customers from Chennai
customers.filter(customers.city == "Chennai")

#Show customers with age > 25
customers.filter(customers.age > 25).show()

#Show only customer_name and city
customers.select(customers.customer_name, customers.city).show()

#Count customers city-wise
customers.groupBy("city").count().show()
