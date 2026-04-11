Phase 6 – Spark Playground Exit Sprint (Advanced Practice Lab)

🔹 Objective
In this phase, the focus is on building strong hands-on skills in PySpark by solving advanced problems involving joins, window functions, date operations, and complete pipeline execution.

🔹 Problem Summary
Worked on datasets such as:

Customers dataset
Orders dataset

The datasets contained:

Null values
Invalid records (negative amounts, incorrect foreign keys)
Duplicate records

Tasks included:

Cleaning and validating data
Performing joins between datasets
Applying aggregations and window functions
Building a complete data pipeline

🔹 Approach

Loaded datasets into PySpark DataFrames
Performed data cleaning:
Removed null values in key columns
Filtered invalid records (negative values)
Trimmed string columns
Removed duplicate records
Validated data integrity:
Identified invalid foreign keys using joins
Performed joins:
Used inner join for valid records
Used left join for validation
Applied transformations:
Aggregations using group-based operations
Filtering invalid data
Applied window functions:
Ranking based on total spend
Running totals and comparison operations
Performed date-based analysis:
Extracted month from date
Calculated monthly metrics
Generated final output dataset

🔹 Key Transformations Used

join() → Combining datasets
left_anti → Identifying invalid foreign keys
groupBy() → Group-based aggregations
agg() → Calculating sum, count
filter() → Removing invalid records
dropna() → Handling null values
dropDuplicates() → Removing duplicates
Window functions → Ranking and analytics
Date functions → Extracting and analyzing time-based data

🔹 Output / Results

Cleaned datasets (customers and orders)
Validated datasets with invalid records identified
Aggregated metrics (total spend, order count)
Customer ranking based on spending
Monthly performance analysis

🔹 Data Engineering Considerations

Ensured proper data cleaning before transformations
Validated referential integrity using joins
Avoided duplicates after joins
Handled null and invalid values carefully
Verified outputs using counts and sample checks

🔹 Challenges Faced

Detecting invalid foreign keys
Handling real-world dirty data
Applying window functions correctly
Managing multiple transformations in a pipeline

🔹 Learnings

Strong understanding of advanced joins
Importance of data validation in pipelines
Practical use of window functions
Handling messy real-world datasets
Building end-to-end PySpark pipelines
