from pyspark.sql.functions import col

def quality_customers(df):
    print("\n=== QUALITE CUSTOMERS ===")

    print("Customer_id null :", df.filter(col("customer_id").isNull()).count())
    print("Email null :", df.filter(col("email").isNull()).count())
    print("Téléphone invalide :", df.filter(~col("phone").rlike("^\\+33[67][0-9]{8}$")).count())

def quality_orders(df):
    print("\n=== QUALITE ORDERS ===")

    print("Order_id null :", df.filter(col("order_id").isNull()).count())
    print("Customer_id null :", df.filter(col("customer_id").isNull()).count())
    print("Montant négatif :", df.filter(col("total_amount") < 0).count())

def quality_order_items(df):
    print("\n=== QUALITE ORDER ITEMS ===")

    print("Order_id null :", df.filter(col("order_id").isNull()).count())
    print("Product_id null :", df.filter(col("product_id").isNull()).count())
    print("Quantité <= 0 :", df.filter(col("quantity") <= 0).count())
    print("Prix unitaire <= 0 :", df.filter(col("unit_price") <= 0).count())


def quality_products(df):
    print("\n=== QUALITE PRODUCTS ===")

    print("Product_id null :", df.filter(col("product_id").isNull()).count())
    print("Prix <= 0 :", df.filter(col("current_price") <= 0).count())

def quality_reviews(df):
    print("\n=== QUALITE REVIEWS ===")

    print("Rating hors intervalle :", df.filter((col("rating") < 1) | (col("rating") > 5)).count())

def quality_delivery_events(df):
    print("\n=== QUALITE DELIVERY EVENTS ===")

    print("Timestamp null :", df.filter(col("event_timestamp").isNull()).count())

def quality_silver_data(silver):

    quality_customers(silver["customers"])
    quality_orders(silver["orders"])
    quality_products(silver["products"])
    quality_order_items(silver["order_items"])
    quality_reviews(silver["reviews"])
    quality_delivery_events(silver["delivery_events"])

