# Phase 2 – Data Transformation using PySpark

## 🔹 Objective

The objective of this phase is to bridge the gap between SQL concepts and PySpark by applying real-world data transformation techniques. This includes working with multiple datasets, performing joins, handling missing data, and generating meaningful business insights.

---

## 🔹 Problem Summary

We were provided with structured datasets such as:

* `customers`
* `sales / orders`

The tasks involved:

* Combining datasets using joins
* Performing aggregations like total spend and average order value
* Identifying patterns such as top customers and inactive customers
* Generating insights at both customer and city level

---

## 🔹 Approach

1. Loaded datasets into PySpark DataFrames
2. Performed data cleaning:

   * Handled null values using multiple approaches (`dropna()`, `filter()`, `coalesce()`)
   * Fixed column name inconsistencies
3. Joined datasets using `customer_id`
4. Applied transformations:

   * `groupBy()` with aggregations (SUM, AVG, COUNT)
   * Filtering conditions (e.g., multiple orders, null checks)
   * Sorting and limiting results
5. Generated final DataFrames for analysis

---

## 🔹 Key Transformations Used

* `join()` → Combine customer and order data
* `groupBy()` → Perform aggregations
* `agg()` → Calculate metrics (sum, avg, count)
* `filter()` → Apply conditions
* `orderBy()` → Sort results (e.g., top customers)
* `coalesce()` → Replace null values with defaults
* `dropna()` → Remove rows with null values
* `left_anti join` → Identify customers with no orders

---

## 🔹 Additional Explorations

* Explored different methods to handle/remove null values:

  * `dropna()` (remove rows)
  * `filter(col().isNotNull())`
  * `coalesce()` for replacing nulls
* Learned how to read different file formats:

  * CSV, JSON, Parquet using `spark.read`
* Practiced writing clean PySpark code:

  * Used bracket `()` style instead of `\` for multi-line queries
  * Improved readability and maintainability of code

---

## 🔹 Output / Results

The following insights were generated:

* Total order amount for each customer
* Top 3 customers by total spend
* Customers with no orders
* City-wise total revenue
* Average order amount per customer
* Customers with more than one order

All outputs are validated and displayed using `display()` in Databricks.

---

## 🔹 Data Engineering Considerations

* Used **LEFT JOIN** to include customers without orders
* Handled null values to avoid incorrect aggregations
* Ensured grouping logic retains correct columns
* Avoided duplication during joins
* Followed best practices for readable and optimized PySpark code

---

## 🔹 Challenges Faced

* Understanding how `groupBy()` affects column selection
* Handling null values after joins
* Fixing syntax and column name errors
* Writing clean multi-line PySpark queries without using `\`

---

## 🔹 Learnings

* Practical understanding of SQL vs PySpark differences
* Importance of data cleaning before transformations
* Efficient use of joins and aggregations in distributed systems
* Handling real-world scenarios like missing data and ranking
* Writing clean and readable PySpark code

---

## 🔹 Files in this Folder

* `sql_queries & pyspark_queries.py` → PySpark implementation
* `phase2_problem_statement.pdf` → Problem description
* `outputs/` → Output screenshots

---

## 🔹 Notes

This phase focuses on applying transformation logic similar to real-world data engineering pipelines, where combining datasets and generating insights is a key responsibility. PySpark enables scalable and efficient data processing for large datasets.

---

## 🔹 Author

Jagatha Nandini
