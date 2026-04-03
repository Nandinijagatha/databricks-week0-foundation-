⚠️ Data Issues Identified

Missing values (name, city, customer_id)
Duplicate records
Invalid values (negative age)
Primary key issues (customer_id null)

🔹 Tasks

1. Identify Data Issues
Check null values
Detect duplicates
Identify invalid age values

2. Data Cleaning
Apply proper cleaning steps:
Remove rows with null customer_id
Handle missing values in name and city

3.Remove duplicate records
Filter invalid age values (age > 0)

4. Validation
Compare row count before and after cleaning
Ensure data integrity is maintained

5. Aggregation
Group data by city
Count number of customers per city

🔹 Key PySpark Operations
createDataFrame()
dropna()
fillna()
dropDuplicates()
filter()
groupBy()
count()
printSchema()

🔹 Key Learnings
Real-world data is always messy
Data cleaning is mandatory before analysis
Invalid data leads to incorrect results
Validation ensures data reliability

🔹 Reflection
❓ What happens if cleaning is skipped?
Incorrect insights
Wrong aggregations
Poor business decisions
❓ Which issue impacted results most?
Duplicate records and invalid values significantly distort results
❓ How would this affect business decisions?
Leads to misleading analytics and wrong strategic decisions

🔹 Cleaning Checklist
Remove null primary keys
Handle missing values
Remove duplicates
Validate numeric ranges (age > 0)
Verify schema consistency
Validate output after transformation

🔹 Conclusion
This phase builds strong fundamentals in data cleaning and preprocessing using PySpark.
It demonstrates how raw messy data is transformed into clean, structured, and reliable datasets before analytics or pipeline development.
