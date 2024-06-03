from pyspark.sql import Row,SparkSession

spark = SparkSession.builder.appName('Unique Count').getOrCreate()
# Sample data
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

# create DataFrame
df = spark.createDataFrame(data)

df.groupBy("job").count().show()

# show DataFrame
df.show()