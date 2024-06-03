from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('min-max-percent').getOrCreate()

sc = spark.sparkContext

# Create a sample DataFrame
data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41), ("J", 86)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Calculate percentiles
quantiles = df.approxQuantile("Age", [0.0, 0.25, 0.5, 0.75, 1.0], 0.01)

print("Min: ", quantiles[0])
print("25th percentile: ", quantiles[1])
print("Median: ", quantiles[2])
print("75th percentile: ", quantiles[3])
print("Max: ", quantiles[4])