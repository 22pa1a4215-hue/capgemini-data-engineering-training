# SQL to PySpark – Phase 3: ETL Pipeline Practice

## Overview
This project focuses on building end-to-end ETL pipelines using PySpark. It helps transition from SQL-based querying to scalable data engineering workflows.

---

## Objective
- Read and process data from multiple sources  
- Clean and transform datasets  
- Convert SQL logic into PySpark  
- Build a reusable ETL pipeline  

---

## ETL Flow
- **Extract**: Load data from CSV, JSON, and Parquet files  
- **Transform**: Handle nulls, filter records, join, and aggregate  
- **Load**: Produce final datasets for reporting  

---

## Tasks

### Data Ingestion
- Read CSV files from `/samples/`  
- Inspect schema using `show()` and `printSchema()`  
- Handle missing values using `dropna()` or `fillna()`  
- Filter invalid records  
- Read JSON and Parquet files  

### Business Exercises
- Calculate daily sales from sales data  
- Compute city-wise revenue from customer data  
- Identify repeat customers (> 2 orders)  
- Find highest spending customers per city  
- Build a final reporting table  

---



## Final Challenge
- Write each task in SQL  
- Convert to PySpark  
- Combine into a single ETL pipeline  

---


## Outcome
- Understanding of ETL workflows  
- Ability to translate SQL into PySpark  
- Experience building data pipelines  
