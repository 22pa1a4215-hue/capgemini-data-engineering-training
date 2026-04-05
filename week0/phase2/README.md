
# SQL to PySpark – Phase 2 (Bridge Pack)

## Overview
This phase was focused on moving beyond basic SQL queries and applying the same logic using PySpark. The goal was to understand how typical SQL operations translate into distributed data processing with Spark.

I worked with sample datasets and performed end-to-end transformations including cleaning, joining, and aggregating data.

---

## Learning Highlights

During this phase, I practiced:

- Combining datasets using joins  
- Performing aggregations such as SUM, AVG, and COUNT  
- Grouping data to generate insights  
- Sorting results based on business needs
- Handling missing or incomplete data in PySpark  

---

## Data Cleaning

Before running any transformations, I ensured the datasets were clean and usable by removing records with missing customer IDs:

```python
customers = customers.dropna(subset=["customer_id"])
orders = orders.dropna(subset=["customer_id"])
```

---

## Practical Problems Solved

I implemented solutions for common real-world scenarios, including:

- Calculating total spending for each customer  
- Identifying the top 3 customers by total purchase value  
- Finding customers who never placed an order  
- Computing total revenue generated from each city  
- Measuring average order value per customer  
- Detecting customers with multiple orders  
- Ranking customers based on their spending  

---

## Approach Followed

To strengthen my understanding, I followed a consistent workflow:

- Started by writing each solution in SQL  
- Recreated the same logic using PySpark DataFrames  
- Ran both versions and compared outputs  
- Focused on understanding the transformation process rather than just syntax  

---

## Project Structure

- `phase2_problem_statement.pdf` – List of all problem statements  
- `phase2_sql_queries.sql` – SQL-based solutions  
- `pyspark_phase2_notebook.py` – PySpark implementations  
- `outputs/` – Screenshots of results  

---

## Outcome

By completing this phase, I was able to:

- Develop a clear understanding of how data transformations work in practice  
- Confidently convert SQL queries into PySpark code  
- Improve my ability to work with joins and aggregations in a distributed environment  
