# Phase 4A – Bucketing & Segmentation using PySpark

## 🔹 Objective

The objective of this phase is to understand how continuous numerical data can be transformed into meaningful categories (segmentation). This helps in simplifying analysis and making better business decisions using different bucketing techniques in PySpark.

---

## 🔹 Problem Summary

We were provided with structured datasets such as:

* `customers`
* `sales`

The tasks involved:

* Calculating total spend per customer
* Segmenting customers into categories like Gold, Silver, Bronze
* Applying multiple segmentation techniques
* Comparing different methods of bucketing
* Analyzing distribution of customers across segments

---

## 🔹 Approach

1. Loaded datasets into PySpark DataFrames  
2. Performed data cleaning:

   * Removed null values using `dropna()`
   * Removed duplicates using `dropDuplicates()`
   * Filtered invalid records (e.g., negative or zero sales)
3. Created total spend per customer using aggregation  
4. Applied multiple segmentation techniques:

   * Conditional logic (business rules)
   * Quantile-based segmentation
   * Bucketizer (MLlib)
   * Window-based ranking
5. Compared results from all methods  

---

## 🔹 Key Transformations Used

* `groupBy()` → Calculate total spend per customer  
* `agg()` → Perform aggregations (SUM)  
* `when()` → Apply conditional segmentation logic  
* `approxQuantile()` → Generate quantile thresholds  
* `Bucketizer` → Perform ML-based bucketing  
* `Window()` + `percent_rank()` → Rank-based segmentation  
* `join()` → Combine results for comparison  
* `filter()` → Remove invalid data  
* `dropDuplicates()` → Ensure data uniqueness  

---

## 🔹 Additional Explorations

* Compared **fixed rule segmentation vs data-driven segmentation**
* Observed how **quantile-based segmentation adapts to data distribution**
* Used **CASE expression (`expr`)** for alternative transformation logic
* Practiced writing structured and readable PySpark pipelines
* Understood how segmentation changes with different techniques

---

## 🔹 Output / Results

The following outputs were generated:

* Total spend per customer  
* Customer segmentation using:

  * Business rules (Gold/Silver/Bronze)
  * Quantile-based segmentation  
  * Bucketizer segmentation  
  * Rank-based segmentation  
* Segment-wise customer distribution  
* Final comparison of all segmentation methods  

All outputs are validated and displayed using `show()`.

---

## 🔹 Data Engineering Considerations

* Ensured **clean and valid data** before segmentation  
* Used **appropriate aggregation logic** for accurate spend calculation  
* Avoided duplication during joins  
* Chose segmentation methods based on **use case (business vs data-driven)**  
* Maintained readable and optimized PySpark code  

---

## 🔹 Challenges Faced

* Understanding differences between multiple segmentation techniques  
* Handling edge cases in bucket boundaries  
* Interpreting quantile values correctly  
* Applying window functions for ranking  
* Ensuring consistency across all segmentation outputs  

---

## 🔹 Learnings

* Clear understanding of **bucketing vs segmentation**  
* Difference between **business rules and data-driven approaches**  
* Importance of segmentation in real-world analytics  
* Practical use of **MLlib (Bucketizer)** and **window functions**  
* Writing scalable and clean PySpark transformation pipelines  

---

## 🔹 Files in this Folder

* `pyspark.py` → PySpark implementation  
* `phase4A_problem_statement.pdf` → Problem description  
* `outputs/` → Output screenshots  

---

## 🔹 Notes

This phase focuses on segmentation techniques widely used in real-world scenarios such as customer analytics, marketing, and recommendation systems. Choosing the right segmentation method depends on business requirements and data distribution.

---

## 🔹 Author

Jagatha Nandini
