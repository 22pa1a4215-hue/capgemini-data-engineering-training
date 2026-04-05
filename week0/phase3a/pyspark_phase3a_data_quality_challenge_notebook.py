from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count



spark = SparkSession.builder \
    .appName("DataCleaningChallenge") \
    .getOrCreate()

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5),
(6, "Teena", "Hyderabad", 27),
(7, "Greeshma", None, 29),
(8, "Teju", "Chennai", None)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

# 1.Identify data issues

# nulls
df.filter(
    col("customer_id").isNull() |
    col("name").isNull() |
    col("city").isNull() |
    col("age").isNull()
).show()

#duplicates
df.groupBy(df.columns).count().filter(col("count") > 1).show()

#invalid values
df.filter(col("age") <= 0).show()

# 2.Clean data 

# count before cleaning
print("Before cleaning:", df.count())

# remove null keys
df_clean = df.filter(df.customer_id.isNotNull())

# handle missing values
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown",
    "age": 0
})

#remove duplicates
df_clean = df_clean.dropDuplicates()

#filter invalid age
df_clean = df_clean.filter(df_clean.age > 0)

# 3.validate cleaning
print("After cleaning:", df_clean.count())
df_clean.show()

# 4.aggregation (customers per city)
df_grouped = df_clean.groupBy("city").count()
df_grouped.show()