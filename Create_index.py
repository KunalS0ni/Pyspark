from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id,dense_rank


spark = SparkSession.builder.appName('Create Index Df').getOrCreate()

# Assuming df is your DataFrame
df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"])

df.show()


# Define window specification
w = Window.orderBy(monotonically_increasing_id())

# Add index
df = df.withColumn("index", row_number().over(w) - 1)

df.show()