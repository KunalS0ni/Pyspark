import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

spark = SparkSession.builder.appName("pyspark 101 ").getOrCreate()
print(spark.version)

df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"])

# Define window specification
w = Window.orderBy(monotonically_increasing_id())

# Add index
df = df.withColumn("index", row_number().over(w) -1)

df.show() 