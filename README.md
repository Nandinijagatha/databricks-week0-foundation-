SQL to PySpark – Phase 1: Simple Foundation Pack

Objective:

Build confidence in converting basic SQL queries into PySpark DataFrame operations using simple datasets.

Concepts Covered: show() select() filter() groupBy()

Understanding DataFrame as a table Dataset Used SQL Table CREATE TABLE customers ( customer_id INT, customer_name VARCHAR(50), city VARCHAR(50), age INT );

INSERT INTO customers VALUES (1, 'Ravi', 'Hyderabad', 25), (2, 'Sita', 'Chennai', 32), (3, 'Arun', 'Hyderabad', 28);

PySpark DataFrame customers = spark.createDataFrame([ (1, "Ravi", "Hyderabad", 25), (2, "Sita", "Chennai", 32), (3, "Arun", "Hyderabad", 28) ], ["customer_id", "customer_name", "city", "age"])

Exercises Completed

    Show all customers SQL: SELECT * FROM customers; PySpark: customers.show()

    Show customers from Chennai SQL: SELECT * FROM customers WHERE city = 'Chennai'; PySpark: customers.filter(customers.city == "Chennai").show()

    Show customers with age > 25 SQL: SELECT * FROM customers WHERE age > 25; PySpark: customers.filter(customers.age > 25).show()

    Show only customer_name and city SQL: SELECT customer_name, city FROM customers; PySpark: customers.select("customer_name", "city").show()

    Count customers city-wise SQL: SELECT city, COUNT(*) FROM customers GROUP BY city; PySpark: customers.groupBy("city").count().show()

Learning Outcome Understood how SQL queries map to PySpark operations Practiced filtering, selection, and aggregation Built foundation for data engineering workflows

Author

Jagatha Nandini
