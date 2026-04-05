# Phase 4: Business Pipeline and Analytics

## Overview
This project builds an end-to-end data pipeline using PySpark. It focuses on transforming raw transactional data into structured business insights through cleaning, aggregation, and reporting.

---

## Technologies Used
- PySpark
- DataFrame API
- Spark SQL functions

---

## Dataset
Sample data was created within the environment.

### Orders Data
- order_id
- customer_id
- order_date
- amount
- city

### Customers Data
- customer_id
- customer_name

---

## Data Cleaning
The following steps were performed:
- Removed null values in customer_id
- Removed duplicate rows
- Filtered out negative amounts
- Verified schema and data types

---

## Tasks Performed

### 1. Daily Sales
Calculated total sales per day using groupBy on order_date.

### 2. City-wise Revenue
Aggregated total revenue for each city.

### 3. Top 5 Customers
Identified top customers based on total spending.

### 4. Repeat Customers
Filtered customers who placed more than one order.

### 5. Customer Segmentation
Customers were categorized based on spending:
- Gold: total_spend > 10000
- Silver: 5000 <= total_spend <= 10000
- Bronze: total_spend < 5000

### 6. Final Reporting Table
Combined all insights into a single table with:
- customer_name
- city
- total_spend
- order_count
- segment

### 7. Output
Due to environment limitations, output is displayed using:
final_df.show(truncate=False)

---

## Key Insights
- High-value customers contribute most to revenue
- Repeat customers indicate retention
- City-wise analysis helps identify regional trends

---

## Conclusion
This phase demonstrates how to build a structured data pipeline in PySpark and generate meaningful business insights from raw data.
