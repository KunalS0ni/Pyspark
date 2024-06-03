from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Change column name').getOrCreate()

sc = spark.sparkContext

# suppose you have the following DataFrame
df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["col1", "col2", "col3"])

# old column names
old_names = ["col1", "col2", "col3"]

# new column names
new_names = ["new_col1", "new_col2", "new_col3"]

df.show()

# renaming
for old_name, new_name in zip(old_names, new_names):
    df = df.withColumnRenamed(old_name, new_name)

df.show()