from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName('2 most frequent').getOrCreate()

sc = spark.sparkContext

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

# show DataFrame
df.show()
# Get the top 2 most frequent jobs
top_2_jobs = df.groupBy('job').count().orderBy('count', ascending=False).limit(2).select('job').rdd.flatMap(lambda x: x).collect()

print(top_2_jobs)

# Replace all but the top 2 most frequent jobs with 'Other'
df = df.withColumn('job', when(col('job').isin(top_2_jobs), col('job')).otherwise('Other'))

# show DataFrame
df.show()