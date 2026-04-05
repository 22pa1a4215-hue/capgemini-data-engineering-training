 # Phase 3A – Data Quality & Cleaning Challenge

## Overview
In this task, I worked with intentionally messy data to understand the importance of data cleaning in real-world data engineering. The focus was on identifying data quality issues, applying cleaning techniques, and validating the results before performing analysis.

---

## Objective
- Identify data quality issues such as nulls, duplicates, and invalid values  
- Clean and prepare the dataset for processing  
- Validate the cleaning steps  
- Perform basic aggregation on cleaned data  

---

## Dataset Used

```python
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)
```

---

## Work Completed

### Data Quality Checks
- Identified null values in key columns  
- Detected duplicate records  
- Found invalid values such as negative age  

### Data Cleaning
- Removed rows with null primary keys  
- Handled missing values appropriately  
- Eliminated duplicate records  
- Filtered out invalid data (e.g., negative age)  

### Validation
- Compared row counts before and after cleaning  
- Ensured only valid and consistent data remained  

### Aggregation
- Calculated the number of customers per city using cleaned data  

---

## Key Learnings
- Real-world datasets often contain incomplete and inconsistent data  
- Data cleaning is essential before any transformation or analysis  
- Even small data issues can significantly impact results  
- Validation is necessary to ensure correctness  

---

## Challenges
- Handling multiple data issues in a single dataset  
- Deciding the right approach for missing values  
- Ensuring no important data was lost during cleaning  

---

## Outcome
This task helped me understand how critical data quality is in data engineering. I gained practical experience in cleaning datasets, validating transformations, and preparing data for further processing.

---

## Reflection
- Skipping data cleaning can lead to incorrect insights and unreliable results  
- Duplicate and invalid records can significantly distort aggregations  
- Poor data quality can negatively impact business decisions  
- A structured data cleaning checklist is essential for consistent workflows  

---
