from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('drop na null values').getOrCreate()

sc = spark.sparkContext

df = spark.createDataFrame([
("A", 1, None),
("B", None, "123" ),
("B", 3, "456"),
("D", None, None),
], ["Name", "Value", "id"])

df.show()

df_2 = df.dropna(subset=['id','value'])

df_2.show()