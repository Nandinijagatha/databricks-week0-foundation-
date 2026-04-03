🚀 **SQL to PySpark – Phase 3A: Data Quality & ETL Pipeline (Practice Pack)**
*(Based on your thought process and learning style)*

---

# 🔹 Objective

The objective of this phase is to move beyond writing simple queries and start thinking like a **data engineer building real pipelines**.

This phase focuses on:

* Understanding messy real-world data
* Performing structured data cleaning
* Building step-by-step ETL workflows using PySpark
* Ensuring reliable and validated outputs for downstream processing

---

# 🔹 Problem Statement

We work with a **real-world messy dataset**:

```python id="data1"
data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]
```

---

## ⚠️ Identified Data Issues

* Missing values (name, city, customer_id)
* Duplicate records
* Invalid data (negative age)
* Primary key issues (customer_id)

---

# 🔹 Architecture Overview

```text id="flow1"
Raw Data → Data Profiling → Data Cleaning → Validation → Aggregation
```

---

# 🔹 Implementation Approach (Step-by-Step Thinking)

---

## 🔹 1. Data Ingestion

* Created DataFrame using PySpark
* Defined schema and column structure
* Loaded raw dataset into Spark

```python id="ing1"
df = spark.createDataFrame(data, ["customer_id", "name", "city", "age"])
df.show()
```

---

## 🔹 2. Data Profiling

* Checked schema structure
* Identified null values
* Counted total records before cleaning

```python id="prof1"
df.printSchema()
df.count()
df.show()
```

---

## 🔹 3. Data Cleaning

Applied step-by-step cleaning rules:

* Remove null primary keys
* Fill missing values
* Remove duplicates
* Filter invalid age values

```python id="clean1"
from pyspark.sql.functions import col

clean_df = (
    df.dropna(subset=["customer_id"])              # remove null IDs
      .fillna({"name": "Unknown", "city": "Unknown"})
      .dropDuplicates()
      .filter(col("age") > 0)
)
```

---

## 🔹 4. Data Validation

* Compared dataset size before and after cleaning
* Ensured no invalid or duplicate records remain

```python id="val1"
print("Before:", df.count())
print("After:", clean_df.count())
clean_df.show()
```

---

## 🔹 5. Aggregation (Business Insight)

* Group data by city
* Count customers per city

```python id="agg1"
result = clean_df.groupBy("city").count()
result.show()
```

---

# 🔹 Key PySpark Operations Used

* `createDataFrame()`
* `printSchema()`
* `count()`
* `dropna()`
* `fillna()`
* `dropDuplicates()`
* `filter()`
* `groupBy()`
* `show()`

---

# 🔹 Results & Deliverables

* Cleaned and structured dataset
* Removed duplicates and invalid records
* Handled missing values properly
* Generated city-wise customer distribution
* Built a complete mini ETL pipeline

---

# 🔹 Data Engineering Best Practices Applied

* Enforced primary key integrity
* Handled missing data systematically
* Applied validation rules after cleaning
* Followed proper ETL sequence
* Ensured reproducibility of pipeline

---

# 🔹 Challenges & Solutions

| Problem           | Solution             |
| ----------------- | -------------------- |
| Missing values    | `fillna()`           |
| Null primary key  | `dropna(subset=...)` |
| Duplicate records | `dropDuplicates()`   |
| Invalid age       | `filter(age > 0)`    |

---

# 🔹 Key Learnings

* Real-world data is always messy
* Cleaning is more important than querying
* Validation ensures correctness of results
* ETL is a structured flow, not random queries
* Simple pipelines are more powerful than complex logic

---

# 🔹 Reflection

### ❓ What happens if cleaning is skipped?

* Wrong insights
* Incorrect aggregations
* Bad business decisions

---

### ❓ What caused major data issues?

* Duplicate records
* Missing identifiers
* Invalid numeric values

---

### ❓ Why is this important in real life?

Because dirty data leads to:

* wrong dashboards
* incorrect reporting
* financial mistakes

---

# 🔹 Data Cleaning Checklist

* Remove null primary keys
* Handle missing values
* Remove duplicates
* Validate numeric ranges
* Check schema correctness
* Validate after transformation

---

# 🔹 Conclusion

This phase demonstrates how raw messy data is transformed into **clean, structured, and usable datasets using PySpark**.

It builds the foundation of real-world data engineering by introducing:

* ETL thinking
* Data quality awareness
* Pipeline-based transformation approach

---


