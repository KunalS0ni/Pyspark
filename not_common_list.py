from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Not commont in list').getOrCreate()

sc = spark.sparkContext

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]

# Convert lists to RDD
rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)

# Perform subtract operation
result_rdd_A = rdd_A.subtract(rdd_B)
result_rdd_B = rdd_B.subtract(rdd_A)

# Union the two RDDs
result_rdd = result_rdd_A.union(result_rdd_B)

# Collect result
result_list = result_rdd.collect()

print(result_list)