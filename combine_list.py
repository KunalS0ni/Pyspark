from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Combining list').getOrCreate()

# Define your lists
list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]

rdd = spark.sparkContext.parallelize(list(zip(list1, list2)))
df = rdd.toDF(["Column1", "Column2"])

# Show the DataFrame
df.show()
