# Phase 4A: Bucketing and Segmentation in PySpark

## Overview
This project focuses on converting continuous numerical data into meaningful categories using different segmentation techniques in PySpark.

---

## Technologies Used
- PySpark
- DataFrame API
- Window functions
- Spark SQL functions

---

## Objective
To understand and implement multiple methods of customer segmentation and compare their effectiveness.

---

## Dataset
Sample dataset with:
- customer_id
- total_spend

---

## Segmentation Methods

### 1. Conditional Logic
Used when() to assign segments:
- Gold: total_spend > 10000
- Silver: 5000 <= total_spend <= 10000
- Bronze: total_spend < 5000

---

### 2. SQL CASE Logic
Implemented similar logic using SQL CASE statements.

---

### 3. Quantile-Based Segmentation
Used approxQuantile() to divide customers into:
- Bottom 33%: Bronze
- Middle 33%: Silver
- Top 33%: Gold

---

### 4. Window-Based Ranking
Used percent_rank() to assign percentile ranking to customers.

---

## Segment Analysis
Grouped customers by segment and counted distribution.

---

## Comparison of Methods

| Method        | Type         | Advantage              | Limitation            |
|--------------|--------------|------------------------|------------------------|
| Conditional  | Business     | Simple and clear       | Static thresholds      |
| Quantile     | Data-driven  | Balanced distribution  | Less intuitive         |
| Window       | Analytical   | Ranking capability     | Not category-focused   |

---

## Key Learnings

- Segmentation simplifies complex numerical data
- Business rules are easy to apply but may become outdated
- Quantile-based methods adapt to data distribution
- Window functions are useful for ranking analysis

---

## Reflection

### Why segmentation?
To simplify analysis and support decision-making.

### Business vs Technical Segmentation
- Business: Fixed rules
- Technical: Data-driven methods

### When do fixed thresholds fail?
When data distribution changes or becomes skewed.

### Best Approach
A combination of business rules and data-driven methods works best in real-world scenarios.

---

## Conclusion
This phase demonstrates multiple approaches to segmentation and highlights their importance in business analytics.
