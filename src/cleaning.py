from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    upper,
    lower,
    initcap,
    concat,
    coalesce,
    lpad,
    regexp_replace,
    regexp_extract,
    struct,
    try_to_date,
    try_to_timestamp,
    abs as spark_abs,
)


# ============================================================
# OUTILS DE NORMALISATION
# ============================================================

def normalize_customer_id(column):
    """
    Exemples :
    C001       -> C001
    cust-001   -> C001
    CUST-1     -> C001
    """
    normalized = upper(trim(column))
    numeric_part = regexp_extract(normalized, r"(\d+)", 1)

    return when(
        numeric_part != "",
        concat(lit("C"), lpad(numeric_part, 3, "0"))
    ).otherwise(None)


def normalize_order_id(column):
    """
    Exemples :
    ORD-1      -> ORD-0001
    order-25   -> ORD-0025
    36         -> ORD-0036
    """
    normalized = upper(trim(column))
    numeric_part = regexp_extract(normalized, r"(\d+)", 1)

    return when(
        numeric_part != "",
        concat(lit("ORD-"), lpad(numeric_part, 4, "0"))
    ).otherwise(None)


def normalize_product_id(column):
    """
    Exemples :
    p1         -> P001
    PROD-12    -> P012
    P999       -> P999
    """
    normalized = upper(trim(column))
    numeric_part = regexp_extract(normalized, r"(\d+)", 1)

    return when(
        numeric_part != "",
        concat(lit("P"), lpad(numeric_part, 3, "0"))
    ).otherwise(None)


def normalize_boolean(column):
    value = lower(trim(column.cast("string")))

    return (
        when(value.isin("true", "yes", "oui", "1", "active", "verified"), True)
        .when(value.isin("false", "no", "non", "0", "inactive"), False)
        .otherwise(None)
    )


def normalize_country(column):
    value = lower(trim(column))

    return (
        when(value.isin("fr", "france", "french republic"), "FRANCE")
        .when(value.isNull() | (value == ""), None)
        .otherwise(upper(trim(column)))
    )


# ============================================================
# CUSTOMERS
# ============================================================

def clean_customers(df: DataFrame) -> DataFrame:
    phone_clean = regexp_replace(
        trim(col("phone")),
        r"[^0-9+]",
        ""
    )

    return (
        df
        .withColumn(
            "customer_id",
            normalize_customer_id(col("customer_id"))
        )
        .withColumn(
            "first_name",
            initcap(trim(col("first_name")))
        )
        .withColumn(
            "last_name",
            initcap(trim(col("last_name")))
        )
        .withColumn(
            "email",
            lower(trim(col("email")))
        )
        .withColumn(
            "city",
            initcap(trim(col("city")))
        )
        .withColumn(
            "country",
            normalize_country(col("country"))
        )
        .withColumn(
            "phone",
            phone_clean
        )
        .withColumn(
            "phone",
            when(
                col("phone").startswith("0033"),
                regexp_replace(col("phone"), r"^0033", "+33")
            ).otherwise(col("phone"))
        )
        .withColumn(
            "phone",
            when(
                col("phone").rlike(r"^0[67][0-9]{8}$"),
                concat(
                    lit("+33"),
                    regexp_replace(col("phone"), r"^0", "")
                )
            ).otherwise(col("phone"))
        )
        .withColumn(
            "phone",
            when(
                col("phone").rlike(r"^\+33[67][0-9]{8}$"),
                col("phone")
            ).otherwise(None)
        )
        .withColumn(
            "birth_date",
            coalesce(
                try_to_date(col("birth_date"), "yyyy-MM-dd"),
                try_to_date(col("birth_date"), "yyyy/MM/dd"),
                try_to_date(col("birth_date"), "dd/MM/yyyy")
            )
        )
        .withColumn(
            "created_at",
            coalesce(
                try_to_timestamp(
                    col("created_at"),
                    lit("yyyy-MM-dd HH:mm:ss")
                ),
                try_to_timestamp(
                    col("created_at"),
                    lit("yyyy/MM/dd HH:mm:ss")
                ),
                try_to_timestamp(
                    col("created_at"),
                    lit("yyyy-MM-dd'T'HH:mm:ss")
                )
            )
        )
    )


# ============================================================
# ORDERS
# ============================================================

def clean_orders(df: DataFrame) -> DataFrame:
    normalized_status = lower(trim(col("status")))
    normalized_payment = lower(trim(col("payment_method")))

    return (
        df
        .withColumn(
            "order_id",
            normalize_order_id(col("order_id"))
        )
        .withColumn(
            "customer_id",
            normalize_customer_id(col("customer_id"))
        )
        .withColumn(
            "order_date",
            coalesce(
                try_to_date(col("order_date"), "yyyy-MM-dd"),
                try_to_date(col("order_date"), "yyyy/MM/dd"),
                try_to_date(col("order_date"), "dd/MM/yyyy")
            )
        )
        .withColumn(
            "status",
            when(
                normalized_status.isin("paid", "payée", "payee"),
                "PAID"
            )
            .when(
                normalized_status.isin(
                    "completed",
                    "complete",
                    "terminée",
                    "terminee"
                ),
                "COMPLETED"
            )
            .when(
                normalized_status.isin(
                    "cancelled",
                    "canceled",
                    "annulée",
                    "annulee"
                ),
                "CANCELLED"
            )
            .when(
                normalized_status.isin("pending", "en attente"),
                "PENDING"
            )
            .otherwise(upper(trim(col("status"))))
        )
        .withColumn(
            "payment_method",
            when(
                normalized_payment.isin(
                    "credit card",
                    "card",
                    "cb",
                    "carte bancaire"
                ),
                "CARD"
            )
            .when(
                normalized_payment.isin(
                    "bank transfer",
                    "wire",
                    "virement"
                ),
                "WIRE"
            )
            .when(normalized_payment == "paypal", "PAYPAL")
            .otherwise(upper(trim(col("payment_method"))))
        )
        .withColumn(
            "currency",
            when(
                col("currency").isNull()
                | (trim(col("currency")) == ""),
                "EUR"
            ).otherwise(upper(trim(col("currency"))))
        )
        .withColumn(
            "total_amount",
            spark_abs(col("total_amount").cast("decimal(10,2)"))
        )
    )


# ============================================================
# PRODUCTS
# ============================================================

def clean_products(df: DataFrame) -> DataFrame:
    category = lower(
        regexp_replace(
            trim(col("category")),
            r"[-_]+",
            " "
        )
    )

    return (
        df
        .withColumn(
            "product_id",
            normalize_product_id(col("product_id"))
        )
        .withColumn(
            "product_name",
            initcap(trim(col("product_name")))
        )
        .withColumn(
            "category",
            when(category.isin("high tech", "hightech"), "High-tech")
            .when(category == "electronique", "Electronique")
            .when(category == "informatique", "Informatique")
            .otherwise(initcap(category))
        )
        .withColumn(
            "brand",
            initcap(trim(col("brand")))
        )
        .withColumn(
            "current_price",
            col("current_price").cast("decimal(10,2)")
        )
        .withColumn(
            "active",
            coalesce(normalize_boolean(col("active")), lit(False))
        )
    )


# ============================================================
# ORDER ITEMS
# ============================================================

def clean_order_items(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "order_id",
            normalize_order_id(col("order_id"))
        )
        .withColumn(
            "product_id",
            normalize_product_id(col("product_id"))
        )
        .withColumn(
            "quantity",
            col("quantity").cast("integer")
        )
        .withColumn(
            "unit_price",
            spark_abs(col("unit_price").cast("decimal(10,2)"))
        )
        .withColumn(
            "discount",
            col("discount").cast("decimal(5,2)")
        )
        .withColumn(
            "discount",
            when(col("discount") < 0, lit(0))
            .when(
                (col("discount") > 1) & (col("discount") <= 100),
                col("discount") / 100
            )
            .otherwise(col("discount"))
            .cast("decimal(5,2)")
        )
    )


# ============================================================
# REVIEWS
# ============================================================

def clean_reviews(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("_id", trim(col("_id").cast("string")))
        .withColumn(
            "customerId",
            normalize_customer_id(col("customerId"))
        )
        .withColumn(
            "orderId",
            normalize_order_id(col("orderId"))
        )
        .withColumn(
            "productId",
            normalize_product_id(col("productId"))
        )
        .withColumn("rating", col("rating").cast("integer"))
        .withColumn(
            "reviewDate",
            coalesce(
                try_to_date(col("reviewDate"), "yyyy-MM-dd"),
                try_to_date(col("reviewDate"), "dd/MM/yyyy"),
                try_to_date(col("reviewDate"), "yyyy/MM/dd"),
                try_to_date(
                    try_to_timestamp(
                        col("reviewDate"),
                        lit("yyyy-MM-dd'T'HH:mm:ss")
                    )
                )
            )
        )
        .withColumn(
            "verifiedPurchase",
            normalize_boolean(col("verifiedPurchase"))
        )
        .withColumn("comment", trim(col("comment")))
    )


# ============================================================
# DELIVERY EVENTS
# ============================================================

def clean_delivery_events(df: DataFrame) -> DataFrame:
    raw_event_type = upper(trim(col("event_type")))

    return (
        df
        .withColumn("event_id", upper(trim(col("event_id"))))
        .withColumn(
            "order_id",
            normalize_order_id(col("order_id"))
        )
        .withColumn(
            "event_type",
            when(
                raw_event_type.isin(
                    "ORDER_CREATED",
                    "PREPARING",
                    "SHIPPED",
                    "IN_TRANSIT",
                    "OUT_FOR_DELIVERY",
                    "DELIVERED",
                    "DELIVERY_FAILED",
                    "CANCELLED"
                ),
                raw_event_type
            ).otherwise("UNKNOWN")
        )
        .withColumn(
            "event_timestamp",
            coalesce(
                try_to_timestamp(
                    col("event_timestamp"),
                    lit("yyyy-MM-dd'T'HH:mm:ss")
                ),
                try_to_timestamp(
                    col("event_timestamp"),
                    lit("yyyy-MM-dd HH:mm:ss")
                ),
                try_to_timestamp(
                    col("event_timestamp"),
                    lit("yyyy/MM/dd HH:mm:ss")
                ),
                try_to_timestamp(
                    col("event_timestamp"),
                    lit("dd/MM/yyyy HH:mm:ss")
                )
            )
        )
        .withColumn(
            "location",
            struct(
                initcap(trim(col("location.city"))).alias("city"),
                normalize_country(
                    col("location.country")
                ).alias("country")
            )
        )
        .withColumn(
            "carrier",
            struct(
                upper(trim(col("carrier.id"))).alias("id"),
                initcap(trim(col("carrier.name"))).alias("name")
            )
        )
    )


# ============================================================
# RÈGLES DE VALIDATION ET REJETS
# ============================================================

def split_customers(df: DataFrame):
    invalid_condition = (
        col("customer_id").isNull()
        | col("email").isNull()
        | ~col("email").rlike(
            r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )
    )

    rejected = (
        df
        .filter(invalid_condition)
        .withColumn(
            "rejection_reason",
            when(
                col("customer_id").isNull(),
                "CUSTOMER_ID_INVALID"
            )
            .when(col("email").isNull(), "EMAIL_MISSING")
            .otherwise("EMAIL_INVALID")
        )
    )

    valid = df.filter(~invalid_condition)
    return valid, rejected


def split_orders(df: DataFrame):
    invalid_condition = (
        col("order_id").isNull()
        | col("customer_id").isNull()
        | col("total_amount").isNull()
    )

    rejected = (
        df
        .filter(invalid_condition)
        .withColumn(
            "rejection_reason",
            when(col("order_id").isNull(), "ORDER_ID_INVALID")
            .when(
                col("customer_id").isNull(),
                "CUSTOMER_ID_INVALID"
            )
            .otherwise("TOTAL_AMOUNT_INVALID")
        )
    )

    valid = df.filter(~invalid_condition)
    return valid, rejected


def split_products(df: DataFrame):
    invalid_condition = (
        col("product_id").isNull()
        | col("product_name").isNull()
        | col("current_price").isNull()
        | (col("current_price") <= 0)
    )

    rejected = (
        df
        .filter(invalid_condition)
        .withColumn(
            "rejection_reason",
            when(col("product_id").isNull(), "PRODUCT_ID_INVALID")
            .when(
                col("product_name").isNull(),
                "PRODUCT_NAME_MISSING"
            )
            .when(
                col("current_price").isNull(),
                "CURRENT_PRICE_INVALID"
            )
            .otherwise("CURRENT_PRICE_NOT_POSITIVE")
        )
    )

    valid = df.filter(~invalid_condition)
    return valid, rejected


def split_order_items(df: DataFrame):
    invalid_condition = (
        col("order_id").isNull()
        | col("product_id").isNull()
        | col("quantity").isNull()
        | (col("quantity") <= 0)
        | col("unit_price").isNull()
        | (col("unit_price") == 0)
        | col("discount").isNull()
        | (col("discount") < 0)
        | (col("discount") > 1)
    )

    rejected = (
        df
        .filter(invalid_condition)
        .withColumn(
            "rejection_reason",
            when(col("order_id").isNull(), "ORDER_ID_INVALID")
            .when(col("product_id").isNull(), "PRODUCT_ID_INVALID")
            .when(col("quantity").isNull(), "QUANTITY_INVALID")
            .when(
                col("quantity") <= 0,
                "QUANTITY_NOT_POSITIVE"
            )
            .when(col("unit_price").isNull(), "UNIT_PRICE_INVALID")
            .when(col("unit_price") == 0, "UNIT_PRICE_ZERO")
            .when(col("discount").isNull(), "DISCOUNT_INVALID")
            .otherwise("DISCOUNT_OUT_OF_RANGE")
        )
    )

    valid = df.filter(~invalid_condition)
    return valid, rejected


def split_reviews(df: DataFrame):
    invalid_condition = (
        col("_id").isNull()
        | col("productId").isNull()
        | col("rating").isNull()
        | ~col("rating").between(1, 5)
    )

    rejected = (
        df
        .filter(invalid_condition)
        .withColumn(
            "rejection_reason",
            when(col("_id").isNull(), "REVIEW_ID_INVALID")
            .when(col("productId").isNull(), "PRODUCT_ID_INVALID")
            .when(col("rating").isNull(), "RATING_INVALID")
            .otherwise("RATING_OUT_OF_RANGE")
        )
    )

    valid = df.filter(~invalid_condition)
    return valid, rejected


def split_delivery_events(df: DataFrame):
    invalid_condition = (
        col("event_id").isNull()
        | col("order_id").isNull()
        | col("event_timestamp").isNull()
        | (col("event_type") == "UNKNOWN")
    )

    rejected = (
        df
        .filter(invalid_condition)
        .withColumn(
            "rejection_reason",
            when(col("event_id").isNull(), "EVENT_ID_INVALID")
            .when(col("order_id").isNull(), "ORDER_ID_INVALID")
            .when(
                col("event_timestamp").isNull(),
                "EVENT_TIMESTAMP_INVALID"
            )
            .otherwise("EVENT_TYPE_UNKNOWN")
        )
    )

    valid = df.filter(~invalid_condition)
    return valid, rejected


# ============================================================
# PIPELINE SILVER COMPATIBLE AVEC LE MAIN ACTUEL
# ============================================================

def clean_bronze_data(bronze):
    """
    Version sans séparation des rejets.
    """
    return {
        "customers": clean_customers(bronze["customers"]),
        "orders": clean_orders(bronze["orders"]),
        "products": clean_products(bronze["products"]),
        "order_items": clean_order_items(bronze["order_items"]),
        "reviews": clean_reviews(bronze["reviews"]),
        "delivery_events": clean_delivery_events(
            bronze["delivery_events"]
        ),
    }


# ============================================================
# PIPELINE SILVER AVEC REJETS
# ============================================================

def clean_bronze_data_with_rejects(bronze):
    """
    Nettoie les données Bronze, sépare les lignes valides
    des lignes rejetées, puis retourne deux dictionnaires :

    - silver : données valides
    - rejected : données invalides avec rejection_reason
    """

    cleaned_customers = clean_customers(
        bronze["customers"]
    )

    cleaned_orders = clean_orders(
        bronze["orders"]
    )

    cleaned_products = clean_products(
        bronze["products"]
    )

    cleaned_order_items = clean_order_items(
        bronze["order_items"]
    )

    cleaned_reviews = clean_reviews(
        bronze["reviews"]
    )

    cleaned_delivery_events = clean_delivery_events(
        bronze["delivery_events"]
    )

    customers, rejected_customers = split_customers(
        cleaned_customers
    )

    orders, rejected_orders = split_orders(
        cleaned_orders
    )

    products, rejected_products = split_products(
        cleaned_products
    )

    order_items, rejected_order_items = split_order_items(
        cleaned_order_items
    )

    reviews, rejected_reviews = split_reviews(
        cleaned_reviews
    )

    delivery_events, rejected_delivery_events = (
        split_delivery_events(
            cleaned_delivery_events
        )
    )

    silver = {
        "customers": customers,
        "orders": orders,
        "products": products,
        "order_items": order_items,
        "reviews": reviews,
        "delivery_events": delivery_events,
    }

    rejected = {
        "customers": rejected_customers,
        "orders": rejected_orders,
        "products": rejected_products,
        "order_items": rejected_order_items,
        "reviews": rejected_reviews,
        "delivery_events": rejected_delivery_events,
    }

    return silver, rejected