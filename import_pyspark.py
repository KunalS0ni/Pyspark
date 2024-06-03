import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Import Pyspark and spark version').getOrCreate()

print(spark.version)