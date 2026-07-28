from typing import Dict

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    collect_set,
    concat_ws,
    count,
    countDistinct,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    sum as spark_sum,
    when,
)


# ==========================================================
# Ventes par produit
# ==========================================================

def build_sales_by_product(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Calcule les quantités vendues et le chiffre d'affaires
    net par produit.
    """

    order_items = silver["order_items"]
    products = silver["products"]

    return (
        order_items
        .join(
            products,
            on="product_id",
            how="left",
        )
        .withColumn(
            "product_name",
            coalesce(
                col("product_name"),
                lit("Produit supprimé"),
            ),
        )
        .withColumn(
            "category",
            coalesce(
                col("category"),
                lit("Inconnue"),
            ),
        )
        .withColumn(
            "brand",
            coalesce(
                col("brand"),
                lit("Inconnue"),
            ),
        )
        .withColumn(
            "line_total",
            (
                col("quantity")
                * col("unit_price")
                * (lit(1) - col("discount"))
            ).cast("decimal(14,2)"),
        )
        .groupBy(
            "product_id",
            "product_name",
            "category",
            "brand",
        )
        .agg(
            spark_sum("quantity").alias(
                "quantity_sold"
            ),
            spark_sum("line_total")
            .cast("decimal(14,2)")
            .alias("revenue"),
        )
        .orderBy(
            col("revenue").desc_nulls_last()
        )
    )


# ==========================================================
# Commandes par client
# ==========================================================

def build_customer_orders(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Produit une synthèse des commandes et dépenses par client.

    La jointure gauche permet de conserver les commandes dont
    le client correspondant est introuvable.
    """

    orders = silver["orders"].alias("o")
    customers = silver["customers"].alias("c")

    return (
        orders
        .join(
            customers,
            col("o.customer_id")
            == col("c.customer_id"),
            how="left",
        )
        .withColumn(
            "full_name",
            when(
                col("c.customer_id").isNotNull(),
                concat_ws(
                    " ",
                    col("c.first_name"),
                    col("c.last_name"),
                ),
            ).otherwise(
                lit("Client inconnu")
            ),
        )
        .withColumn(
            "email",
            coalesce(
                col("c.email"),
                lit("Inconnu"),
            ),
        )
        .withColumn(
            "city",
            coalesce(
                col("c.city"),
                lit("Inconnue"),
            ),
        )
        .withColumn(
            "country",
            coalesce(
                col("c.country"),
                lit("Inconnu"),
            ),
        )
        .withColumn(
            "customer_found",
            col("c.customer_id").isNotNull(),
        )
        .groupBy(
            col("o.customer_id").alias(
                "customer_id"
            ),
            "full_name",
            "email",
            "city",
            "country",
            "customer_found",
        )
        .agg(
            countDistinct("o.order_id").alias(
                "number_of_orders"
            ),
            spark_sum("o.total_amount")
            .cast("decimal(14,2)")
            .alias("total_spent"),
        )
        .withColumn(
            "total_spent",
            coalesce(
                col("total_spent"),
                lit(0).cast("decimal(14,2)"),
            ),
        )
        .orderBy(
            col("total_spent").desc()
        )
    )


# ==========================================================
# Notes par produit
# ==========================================================

def build_product_ratings(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Calcule les statistiques d'avis par produit.
    """

    reviews = silver["reviews"].alias("r")
    products = silver["products"].alias("p")

    return (
        reviews
        .join(
            products,
            col("r.productId")
            == col("p.product_id"),
            how="left",
        )
        .withColumn(
            "product_name",
            coalesce(
                col("p.product_name"),
                lit("Produit supprimé"),
            ),
        )
        .withColumn(
            "category",
            coalesce(
                col("p.category"),
                lit("Inconnue"),
            ),
        )
        .withColumn(
            "brand",
            coalesce(
                col("p.brand"),
                lit("Inconnue"),
            ),
        )
        .withColumn(
            "product_found",
            col("p.product_id").isNotNull(),
        )
        .groupBy(
            col("r.productId").alias(
                "product_id"
            ),
            "product_name",
            "category",
            "brand",
            "product_found",
        )
        .agg(
            avg("r.rating")
            .cast("decimal(3,2)")
            .alias("average_rating"),
            count("r._id").alias(
                "number_of_reviews"
            ),
            spark_sum(
                when(
                    col("r.verifiedPurchase")
                    == lit(True),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("verified_reviews"),
        )
        .orderBy(
            col("average_rating")
            .desc_nulls_last()
        )
    )


# ==========================================================
# Statut de livraison par commande
# ==========================================================

def build_delivery_status(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Joint les commandes au dernier événement de livraison.

    Cette vue conserve également les commandes ne possédant
    aucun événement de livraison.
    """

    orders = silver["orders"].alias("o")

    delivery_summary = (
        build_delivery_summary(silver)
        .alias("d")
    )

    return (
        orders
        .join(
            delivery_summary,
            col("o.order_id")
            == col("d.order_id"),
            how="left",
        )
        .select(
            col("o.order_id").alias(
                "order_id"
            ),
            col("o.customer_id").alias(
                "customer_id"
            ),
            col("o.order_date").alias(
                "order_date"
            ),
            coalesce(
                col("d.last_delivery_status"),
                lit("AUCUN ÉVÉNEMENT"),
            ).alias("delivery_status"),
            col("d.first_event_timestamp"),
            col("d.last_event_timestamp"),
            coalesce(
                col("d.number_of_events"),
                lit(0),
            ).alias("number_of_events"),
            col("d.shipping_date"),
            col("d.delivery_date"),
            coalesce(
                col("d.last_city"),
                lit("Inconnue"),
            ).alias("last_known_city"),
            coalesce(
                col("d.last_country"),
                lit("Inconnu"),
            ).alias("last_known_country"),
            coalesce(
                col("d.carrier_id"),
                lit("Inconnu"),
            ).alias("carrier_id"),
            coalesce(
                col("d.carrier_name"),
                lit("Inconnu"),
            ).alias("carrier_name"),
            col("d.order_id")
            .isNotNull()
            .alias("delivery_event_found"),
        )
        .orderBy(
            col("last_event_timestamp")
            .desc_nulls_last()
        )
    )


# ==========================================================
# Agrégation des lignes de commande
# ==========================================================

def build_order_items_summary(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Calcule les montants agrégés par commande.
    """

    order_items = silver["order_items"]

    return (
        order_items
        .withColumn(
            "gross_line_amount",
            (
                col("quantity")
                * col("unit_price")
            ).cast("decimal(14,2)"),
        )
        .withColumn(
            "discount_amount",
            (
                col("quantity")
                * col("unit_price")
                * col("discount")
            ).cast("decimal(14,2)"),
        )
        .withColumn(
            "net_line_amount",
            (
                col("quantity")
                * col("unit_price")
                * (lit(1) - col("discount"))
            ).cast("decimal(14,2)"),
        )
        .groupBy("order_id")
        .agg(
            countDistinct("product_id").alias(
                "number_of_products"
            ),
            spark_sum("quantity").alias(
                "total_quantity"
            ),
            spark_sum("gross_line_amount")
            .cast("decimal(14,2)")
            .alias("gross_amount"),
            spark_sum("discount_amount")
            .cast("decimal(14,2)")
            .alias("discount_amount"),
            spark_sum("net_line_amount")
            .cast("decimal(14,2)")
            .alias("net_amount"),
        )
    )


# ==========================================================
# Agrégation des produits par commande
# ==========================================================

def build_order_products_summary(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Produit les listes des produits, catégories et marques
    présentes dans chaque commande.
    """

    order_items = silver["order_items"]
    products = silver["products"]

    return (
        order_items
        .join(
            products,
            on="product_id",
            how="left",
        )
        .withColumn(
            "product_name",
            coalesce(
                col("product_name"),
                lit("Produit supprimé"),
            ),
        )
        .withColumn(
            "category",
            coalesce(
                col("category"),
                lit("Inconnue"),
            ),
        )
        .withColumn(
            "brand",
            coalesce(
                col("brand"),
                lit("Inconnue"),
            ),
        )
        .groupBy("order_id")
        .agg(
            concat_ws(
                ", ",
                collect_set("product_name"),
            ).alias("product_names"),
            concat_ws(
                ", ",
                collect_set("category"),
            ).alias("categories"),
            concat_ws(
                ", ",
                collect_set("brand"),
            ).alias("brands"),
            countDistinct("category").alias(
                "number_of_categories"
            ),
        )
    )


# ==========================================================
# Agrégation des avis par commande
# ==========================================================

def build_order_reviews_summary(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Calcule les statistiques des avis par commande.

    La jointure utilise orderId, car un avis MongoDB possède
    directement l'identifiant de la commande.
    """

    reviews = silver["reviews"]

    return (
        reviews
        .groupBy(
            col("orderId").alias("order_id")
        )
        .agg(
            avg("rating")
            .cast("decimal(3,2)")
            .alias("average_rating"),
            count("_id").alias(
                "number_of_reviews"
            ),
            spark_sum(
                when(
                    col("verifiedPurchase")
                    == lit(True),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("verified_reviews"),
        )
    )


# ==========================================================
# Synthèse des événements de livraison
# ==========================================================

def build_delivery_summary(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Construit une ligne de synthèse par commande.

    Le dernier événement est sélectionné avec une fonction
    de fenêtrage ordonnée par event_timestamp.
    """

    delivery_events = silver[
        "delivery_events"
    ]

    last_event_window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            col(
                "event_timestamp"
            ).desc_nulls_last(),
            col("event_id").desc_nulls_last(),
        )
    )

    ranked_events = (
        delivery_events
        .withColumn(
            "_event_rank",
            row_number().over(
                last_event_window
            ),
        )
    )

    last_event = (
        ranked_events
        .filter(
            col("_event_rank") == 1
        )
        .select(
            "order_id",
            col("event_timestamp").alias(
                "last_event_timestamp"
            ),
            col("event_type").alias(
                "last_delivery_status"
            ),
            col("location.city").alias(
                "last_city"
            ),
            col("location.country").alias(
                "last_country"
            ),
            col("carrier.id").alias(
                "carrier_id"
            ),
            col("carrier.name").alias(
                "carrier_name"
            ),
        )
    )

    event_aggregates = (
        delivery_events
        .groupBy("order_id")
        .agg(
            spark_min(
                "event_timestamp"
            ).alias(
                "first_event_timestamp"
            ),
            count("*").alias(
                "number_of_events"
            ),
            spark_min(
                when(
                    col("event_type").isin(
                        "SHIPPED",
                        "IN_TRANSIT",
                        "OUT_FOR_DELIVERY",
                    ),
                    col("event_timestamp"),
                )
            ).alias("shipping_date"),
            spark_min(
                when(
                    col("event_type")
                    == "DELIVERED",
                    col("event_timestamp"),
                )
            ).alias("delivery_date"),
        )
    )

    return (
        event_aggregates
        .join(
            last_event,
            on="order_id",
            how="left",
        )
    )


# ==========================================================
# Vue consolidée Customer Order 360
# ==========================================================

def build_customer_order_360(
    silver: Dict[str, DataFrame],
) -> DataFrame:
    """
    Construit une première vue Gold contenant une ligne
    par commande.
    """

    orders = silver["orders"].alias("o")
    customers = silver["customers"].alias("c")

    order_items_summary = (
        build_order_items_summary(silver)
        .alias("oi")
    )

    order_products_summary = (
        build_order_products_summary(silver)
        .alias("op")
    )

    order_reviews_summary = (
        build_order_reviews_summary(silver)
        .alias("rev")
    )

    delivery_summary = (
        build_delivery_summary(silver)
        .alias("ds")
    )

    joined = (
        orders
        .join(
            customers,
            col("o.customer_id")
            == col("c.customer_id"),
            how="left",
        )
        .join(
            order_items_summary,
            col("o.order_id")
            == col("oi.order_id"),
            how="left",
        )
        .join(
            order_products_summary,
            col("o.order_id")
            == col("op.order_id"),
            how="left",
        )
        .join(
            order_reviews_summary,
            col("o.order_id")
            == col("rev.order_id"),
            how="left",
        )
        .join(
            delivery_summary,
            col("o.order_id")
            == col("ds.order_id"),
            how="left",
        )
    )

    return (
        joined
        .select(
            col("o.order_id").alias(
                "order_id"
            ),
            col("o.customer_id").alias(
                "customer_id"
            ),
            col("o.order_date").alias(
                "order_date"
            ),
            col("o.status").alias(
                "order_status"
            ),
            col("o.total_amount").alias(
                "order_total_amount"
            ),
            col("c.first_name"),
            col("c.last_name"),
            col("c.email"),
            col("c.city").alias(
                "customer_city"
            ),
            col("c.country").alias(
                "customer_country"
            ),
            col("c.customer_id")
            .isNotNull()
            .alias("customer_found"),
            coalesce(
                col("oi.number_of_products"),
                lit(0),
            ).alias("number_of_products"),
            coalesce(
                col("oi.total_quantity"),
                lit(0),
            ).alias("total_quantity"),
            coalesce(
                col("oi.gross_amount"),
                lit(0).cast(
                    "decimal(14,2)"
                ),
            ).alias("gross_amount"),
            coalesce(
                col("oi.discount_amount"),
                lit(0).cast(
                    "decimal(14,2)"
                ),
            ).alias("discount_amount"),
            coalesce(
                col("oi.net_amount"),
                lit(0).cast(
                    "decimal(14,2)"
                ),
            ).alias("net_amount"),
            coalesce(
                col("op.product_names"),
                lit("Aucun produit"),
            ).alias("product_names"),
            coalesce(
                col("op.categories"),
                lit("Inconnue"),
            ).alias("categories"),
            coalesce(
                col("op.brands"),
                lit("Inconnue"),
            ).alias("brands"),
            coalesce(
                col("op.number_of_categories"),
                lit(0),
            ).alias("number_of_categories"),
            col("rev.average_rating"),
            coalesce(
                col("rev.number_of_reviews"),
                lit(0),
            ).alias("number_of_reviews"),
            coalesce(
                col("rev.verified_reviews"),
                lit(0),
            ).alias("verified_reviews"),
            col("ds.first_event_timestamp"),
            col("ds.last_event_timestamp"),
            coalesce(
                col("ds.number_of_events"),
                lit(0),
            ).alias("number_of_events"),
            coalesce(
                col("ds.last_delivery_status"),
                lit("AUCUN ÉVÉNEMENT"),
            ).alias("last_delivery_status"),
            col("ds.shipping_date"),
            col("ds.delivery_date"),
            coalesce(
                col("ds.last_city"),
                lit("Inconnue"),
            ).alias("last_city"),
            coalesce(
                col("ds.last_country"),
                lit("Inconnu"),
            ).alias("last_country"),
            coalesce(
                col("ds.carrier_id"),
                lit("Inconnu"),
            ).alias("carrier_id"),
            coalesce(
                col("ds.carrier_name"),
                lit("Inconnu"),
            ).alias("carrier_name"),
            col("ds.order_id")
            .isNotNull()
            .alias("delivery_found"),
        )
    )


# ==========================================================
# Transformation Silver vers Gold
# ==========================================================

def transform_silver_to_gold(
    silver: Dict[str, DataFrame],
) -> Dict[str, DataFrame]:
    """
    Construit les DataFrames de la couche Gold.
    """

    return {
        "customer_order_360":
            build_customer_order_360(
                silver
            ),
        "sales_by_product":
            build_sales_by_product(
                silver
            ),
        "customer_orders":
            build_customer_orders(
                silver
            ),
        "product_ratings":
            build_product_ratings(
                silver
            ),
        "delivery_status":
            build_delivery_status(
                silver
            ),
    }