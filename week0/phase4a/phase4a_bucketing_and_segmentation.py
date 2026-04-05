from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (101, 2000),
    (102, 7000),
    (103, 15000),
    (104, 4000),
    (105, 12000),
    (106, 3000),
    (107, 8000),
    (108, 600),
    (109, 20000),
    (110, 9500)
]

df = spark.createDataFrame(data, ["customer_id", "total_spend"])

df.show()

# Task1: Conditional Segmentation
from pyspark.sql.functions import when, col

seg_df = df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

seg_df.show()

# Task 2: Count Customers per Segment
segment_count = seg_df.groupBy("segment").count()
segment_count.show()

# Task 3: Quantile-Based Segmentation
quantiles = df.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

quantile_df = df.withColumn(
    "segment",
    when(col("total_spend") <= q1, "Bronze")
    .when((col("total_spend") > q1) & (col("total_spend") <= q2), "Silver")
    .otherwise("Gold")
)

quantile_df.show()

#Task 4: Window-Based Ranking
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

window = Window.orderBy("total_spend")

rank_df = df.withColumn(
    "rank_pct",
    percent_rank().over(window)
)

rank_df.show()

# Task 5: Compare Methods
print("Conditional Segmentation")
seg_df.show()

print("Quantile Segmentation")
quantile_df.show()

print("Ranking")
rank_df.show()

df.show()