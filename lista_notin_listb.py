from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Lista not in listb').getOrCreate()

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]


sc = spark.sparkContext

# Convert lists to RDD
rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)

# Perform subtract operation
result_rdd = rdd_A.subtract(rdd_B)

# Collect result
result_list = result_rdd.collect()
print(result_list)