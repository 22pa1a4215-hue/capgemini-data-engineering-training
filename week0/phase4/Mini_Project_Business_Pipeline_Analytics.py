from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

orders_data = [
    (1, 101, "2024-01-01", 500, "Hyderabad"),
    (2, 102, "2024-01-01", 700, "Delhi"),
    (3, 101, "2024-01-02", 300, "Hyderabad"),
    (4, 103, "2024-01-02", 1500, "Mumbai"),
    (5, 102, "2024-01-03", 2000, "Delhi"),
    (6, 104, "2024-01-03", -100, "Chennai"),  # invalid
    (7, None, "2024-01-04", 800, "Bangalore") # null
]

orders_df = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "order_date", "amount", "city"]
)

customers_data = [
    (101, "Amit"),
    (102, "Rahul"),
    (103, "Sneha"),
    (104, "Priya")
]

customers_df = spark.createDataFrame(
    customers_data,
    ["customer_id", "customer_name"]
)

orders_df.show()
customers_df.show()

# data cleaning
from pyspark.sql.functions import col

orders_df = orders_df.dropna(subset=["customer_id"])
orders_df = orders_df.dropDuplicates()
orders_df = orders_df.filter(col("amount") > 0)

# Task 1
from pyspark.sql.functions import sum

daily_sales = orders_df.groupBy("order_date") \
    .agg(sum("amount").alias("total_sales"))

daily_sales.show()

# Task 2
city_revenue = orders_df.groupBy("city") \
    .agg(sum("amount").alias("total_revenue"))

city_revenue.show()

# Task 3
customer_spend = orders_df.groupBy("customer_id") \
    .agg(sum("amount").alias("total_spend"))

top_customers = customer_spend.orderBy(col("total_spend").desc()).limit(5)

top_customers.show()

# Task4
from pyspark.sql.functions import count

repeat_customers = orders_df.groupBy("customer_id") \
    .agg(count("order_id").alias("order_count")) \
    .filter(col("order_count") > 1)

repeat_customers.show()

# Task5
from pyspark.sql.functions import when

customer_segmentation = customer_spend.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when(col("total_spend") >= 5000, "Silver")
    .otherwise("Bronze")
)

customer_segmentation.show()

# Task 6

# Join customer names
final_df = customer_segmentation.join(customers_df, "customer_id", "left")

# Order count
order_counts = orders_df.groupBy("customer_id") \
    .agg(count("order_id").alias("order_count"))

final_df = final_df.join(order_counts, "customer_id", "left")

# City
city_df = orders_df.select("customer_id", "city").dropDuplicates(["customer_id"])

final_df = final_df.join(city_df, "customer_id", "left")

final_df = final_df.select(
    "customer_name",
    "city",
    "total_spend",
    "order_count",
    "segment"
)

final_df.show()

# Task 7

# Saving to file is restricted in Spark Playground
# So displaying final output instead
final_df.show(truncate=False)