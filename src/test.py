from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TP MultiSource")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "driver")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

print("=" * 50)
print("Spark fonctionne !")
print("Version :", spark.version)
print("=" * 50)

df = spark.range(10)

df.show()

print("Nombre de lignes :", df.count())

spark.stop()