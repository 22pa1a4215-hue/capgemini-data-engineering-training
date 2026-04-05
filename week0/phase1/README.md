# Databricks Week 0 – Foundation

## Phase 1: SQL to PySpark – Foundational Exercises

### Overview
In this phase, I explored how SQL operations can be seamlessly translated into PySpark DataFrame transformations.  
The goal was to build a strong foundation in handling data programmatically while retaining SQL logic.


### Key Exercises Implemented
- Display all customer records  
- Filter customers located in **Chennai**  
- Filter customers with **age > 25**  
- Select columns: `customer_name`, `city`  
- Count customers per city  


### SQL → PySpark Reference Table

| SQL Statement         | PySpark Equivalent                  |
|----------------------|------------------------------------|
| SELECT *           |  df.show()                        |
| WHERE <condition>   | df.filter(<condition>)         |
| SELECT col1, col2  |  df.select("col1", "col2")       |
| GROUP BY col       |  df.groupBy("col").count()        |


### Learning Takeaways
1. PySpark DataFrames mirror SQL concepts but allow programmatic flexibility.  
2. Filtering, selection, and aggregation can be performed efficiently on large datasets.  
3. Mapping SQL logic to PySpark code strengthens both SQL skills and Spark programming skills.

### Notes
 All code snippets and exercises are included in this folder.This serves as a foundation for building more complex pipelines in future phases.
