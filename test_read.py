import sys

sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.9-src.zip")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestRead") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("=" * 50)
print("Spark version :", spark.version)
print("=" * 50)

customers = spark.read.csv(
    "/workspace/data/raw/customers/customers.csv",
    header=True,
    inferSchema=True
)

print("CUSTOMERS")
customers.printSchema()
customers.show(5)

orders = spark.read.csv(
    "/workspace/data/raw/orders/orders.csv",
    header=True,
    inferSchema=True
)

print("ORDERS")
orders.printSchema()
orders.show(5)

products = spark.read.csv(
    "/workspace/data/raw/products/products.csv",
    header=True,
    inferSchema=True
)

print("PRODUCTS")
products.printSchema()
products.show(5)

reviews = spark.read \
    .option("multiLine", True) \
    .json("/workspace/data/raw/reviews/reviews.json")

print("REVIEWS")
reviews.printSchema()
reviews.show(5, truncate=False)

delivery = spark.read \
    .option("multiLine", True) \
    .json("/workspace/data/raw/delivery_events/delivery_events.json")
    
print("DELIVERY EVENTS")
delivery.printSchema()
delivery.show(5)

spark.stop()
