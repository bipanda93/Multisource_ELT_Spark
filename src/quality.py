from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


VALID_STATUSES = [
    "CREATED",
    "PAID",
    "PENDING",
    "PREPARING",
    "SHIPPED",
    "COMPLETED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
    "UNKNOWN",
]


# ============================================================
# CONTROLES QUALITE SILVER
# ============================================================

def quality_customers(df: DataFrame) -> None:
    """
    Affiche les contrôles qualité du DataFrame customers Silver.
    """

    print("\n=== QUALITE CUSTOMERS ===")

    null_customer_ids = (
        df.filter(
            col("customer_id").isNull()
        )
        .count()
    )

    null_emails = (
        df.filter(
            col("email").isNull()
        )
        .count()
    )

    invalid_phones = (
        df.filter(
            col("phone").isNull()
            | ~col("phone").rlike(
                r"^\+33[67][0-9]{8}$"
            )
        )
        .count()
    )

    print("Customer_id null :", null_customer_ids)
    print("Email null :", null_emails)
    print("Téléphone invalide :", invalid_phones)


def quality_orders(df: DataFrame) -> None:
    """
    Affiche les contrôles qualité du DataFrame orders Silver.
    """

    print("\n=== QUALITE ORDERS ===")

    null_order_ids = (
        df.filter(
            col("order_id").isNull()
        )
        .count()
    )

    null_customer_ids = (
        df.filter(
            col("customer_id").isNull()
        )
        .count()
    )

    invalid_amounts = (
        df.filter(
            col("total_amount").isNull()
            | (col("total_amount") < 0)
        )
        .count()
    )

    print("Order_id null :", null_order_ids)
    print("Customer_id null :", null_customer_ids)
    print("Montant invalide :", invalid_amounts)


def quality_order_items(df: DataFrame) -> None:
    """
    Affiche les contrôles qualité du DataFrame order_items Silver.
    """

    print("\n=== QUALITE ORDER ITEMS ===")

    null_order_ids = (
        df.filter(
            col("order_id").isNull()
        )
        .count()
    )

    null_product_ids = (
        df.filter(
            col("product_id").isNull()
        )
        .count()
    )

    invalid_quantities = (
        df.filter(
            col("quantity").isNull()
            | (col("quantity") <= 0)
        )
        .count()
    )

    invalid_unit_prices = (
        df.filter(
            col("unit_price").isNull()
            | (col("unit_price") <= 0)
        )
        .count()
    )

    invalid_discounts = (
        df.filter(
            col("discount").isNull()
            | (col("discount") < 0)
            | (col("discount") > 1)
        )
        .count()
    )

    print("Order_id null :", null_order_ids)
    print("Product_id null :", null_product_ids)
    print("Quantité <= 0 :", invalid_quantities)
    print("Prix unitaire <= 0 :", invalid_unit_prices)
    print("Remise hors intervalle [0, 1] :", invalid_discounts)


def quality_products(df: DataFrame) -> None:
    """
    Affiche les contrôles qualité du DataFrame products Silver.
    """

    print("\n=== QUALITE PRODUCTS ===")

    null_product_ids = (
        df.filter(
            col("product_id").isNull()
        )
        .count()
    )

    null_product_names = (
        df.filter(
            col("product_name").isNull()
        )
        .count()
    )

    invalid_prices = (
        df.filter(
            col("current_price").isNull()
            | (col("current_price") <= 0)
        )
        .count()
    )

    print("Product_id null :", null_product_ids)
    print("Product_name null :", null_product_names)
    print("Prix <= 0 :", invalid_prices)


def quality_reviews(df: DataFrame) -> None:
    """
    Affiche les contrôles qualité du DataFrame reviews Silver.
    """

    print("\n=== QUALITE REVIEWS ===")

    null_review_ids = (
        df.filter(
            col("_id").isNull()
        )
        .count()
    )

    null_order_ids = (
        df.filter(
            col("orderId").isNull()
        )
        .count()
    )

    invalid_ratings = (
        df.filter(
            col("rating").isNull()
            | (col("rating") < 1)
            | (col("rating") > 5)
        )
        .count()
    )

    print("Review_id null :", null_review_ids)
    print("Order_id null :", null_order_ids)
    print("Rating hors intervalle [1, 5] :", invalid_ratings)


def quality_delivery_events(df: DataFrame) -> None:
    """
    Affiche les contrôles qualité du DataFrame delivery_events Silver.
    """

    print("\n=== QUALITE DELIVERY EVENTS ===")

    null_event_ids = (
        df.filter(
            col("event_id").isNull()
        )
        .count()
    )

    null_order_ids = (
        df.filter(
            col("order_id").isNull()
        )
        .count()
    )

    null_timestamps = (
        df.filter(
            col("event_timestamp").isNull()
        )
        .count()
    )

    unknown_event_types = (
        df.filter(
            col("event_type").isNull()
            | (col("event_type") == "UNKNOWN")
        )
        .count()
    )

    print("Event_id null :", null_event_ids)
    print("Order_id null :", null_order_ids)
    print("Timestamp null :", null_timestamps)
    print("Event_type inconnu :", unknown_event_types)


def quality_silver_data(
    silver: dict[str, DataFrame],
) -> None:
    """
    Lance l'ensemble des contrôles qualité sur la couche Silver.
    """

    required_dataframes = {
        "customers",
        "orders",
        "products",
        "order_items",
        "reviews",
        "delivery_events",
    }

    missing_dataframes = (
        required_dataframes - set(silver.keys())
    )

    if missing_dataframes:
        raise KeyError(
            "DataFrames Silver manquants : "
            f"{sorted(missing_dataframes)}"
        )

    quality_customers(
        silver["customers"]
    )

    quality_orders(
        silver["orders"]
    )

    quality_products(
        silver["products"]
    )

    quality_order_items(
        silver["order_items"]
    )

    quality_reviews(
        silver["reviews"]
    )

    quality_delivery_events(
        silver["delivery_events"]
    )


# ============================================================
# ETAPE 35 - VALIDATION FINALE DU DATAFRAME GOLD
# ============================================================

def build_validation_results(
    customer_order_360: DataFrame,
) -> DataFrame:
    """
    Construit le DataFrame validation_results.

    Le résultat contient :
        - controle
        - nombre_anomalies
        - statut

    Le statut vaut :
        - OK si aucune anomalie n'est détectée ;
        - ERREUR si au moins une anomalie est détectée.
    """

    spark = customer_order_360.sparkSession

    validations: List[
        Tuple[str, int, str]
    ] = []

    def add_result(
        control_name: str,
        anomaly_count: int,
    ) -> None:
        """
        Ajoute un résultat de contrôle à la liste.
        """

        status = (
            "OK"
            if anomaly_count == 0
            else "ERREUR"
        )

        validations.append(
            (
                control_name,
                int(anomaly_count),
                status,
            )
        )

    # --------------------------------------------------------
    # 1. Vérification de l'unicité de order_id
    # --------------------------------------------------------

    duplicate_orders = (
        customer_order_360
        .groupBy("order_id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    add_result(
        "Unicité des commandes",
        duplicate_orders,
    )

    # --------------------------------------------------------
    # 2. Vérification des identifiants obligatoires
    # --------------------------------------------------------

    null_order_ids = (
        customer_order_360
        .filter(
            col("order_id").isNull()
        )
        .count()
    )

    add_result(
        "Order_id manquants",
        null_order_ids,
    )

    null_customer_ids = (
        customer_order_360
        .filter(
            col("customer_id").isNull()
        )
        .count()
    )

    add_result(
        "Customer_id manquants",
        null_customer_ids,
    )

    # --------------------------------------------------------
    # 3. Vérification des montants
    # --------------------------------------------------------

    negative_amounts = (
        customer_order_360
        .filter(
            col("order_total_amount").isNull()
            | (col("order_total_amount") < 0)
            | (col("gross_amount") < 0)
            | (col("discount_amount") < 0)
            | (col("net_amount") < 0)
        )
        .count()
    )

    add_result(
        "Montants négatifs ou absents",
        negative_amounts,
    )

    inconsistent_amounts = (
        customer_order_360
        .filter(
            col("gross_amount").isNotNull()
            & col("discount_amount").isNotNull()
            & col("net_amount").isNotNull()
            & (
                col("net_amount")
                != (
                    col("gross_amount")
                    - col("discount_amount")
                )
            )
        )
        .count()
    )

    add_result(
        "Incohérence des montants calculés",
        inconsistent_amounts,
    )

    # --------------------------------------------------------
    # 4. Vérification des quantités
    # --------------------------------------------------------

    invalid_quantities = (
        customer_order_360
        .filter(
            (col("number_of_products") < 0)
            | (col("total_quantity") < 0)
            | (col("number_of_categories") < 0)
            | (col("number_of_reviews") < 0)
            | (col("verified_reviews") < 0)
            | (col("number_of_events") < 0)
        )
        .count()
    )

    add_result(
        "Quantités ou compteurs négatifs",
        invalid_quantities,
    )

    # --------------------------------------------------------
    # 5. Vérification des notes
    # --------------------------------------------------------

    invalid_ratings = (
        customer_order_360
        .filter(
            col("average_rating").isNotNull()
            & (
                (col("average_rating") < 1)
                | (col("average_rating") > 5)
            )
        )
        .count()
    )

    add_result(
        "Notes moyennes hors intervalle",
        invalid_ratings,
    )

    # --------------------------------------------------------
    # 6. Vérification du score qualité
    # --------------------------------------------------------

    if (
        "data_quality_score"
        in customer_order_360.columns
    ):
        missing_quality_score_column = 0

        invalid_quality_scores = (
            customer_order_360
            .filter(
                col(
                    "data_quality_score"
                ).isNull()
                | (
                    col(
                        "data_quality_score"
                    ) < 0
                )
                | (
                    col(
                        "data_quality_score"
                    ) > 100
                )
            )
            .count()
        )
    else:
        missing_quality_score_column = 1
        invalid_quality_scores = 0

    add_result(
        "Colonne data_quality_score absente",
        missing_quality_score_column,
    )

    add_result(
        "Scores qualité hors intervalle",
        invalid_quality_scores,
    )

    # --------------------------------------------------------
    # 7. Vérification des dates
    # --------------------------------------------------------

    invalid_delivery_dates = (
        customer_order_360
        .filter(
            col("delivery_date").isNotNull()
            & col("order_date").isNotNull()
            & (
                col("delivery_date").cast("date")
                < col("order_date").cast("date")
            )
        )
        .count()
    )

    add_result(
        "Livraison antérieure à la commande",
        invalid_delivery_dates,
    )

    invalid_shipping_dates = (
        customer_order_360
        .filter(
            col("shipping_date").isNotNull()
            & col("order_date").isNotNull()
            & (
                col("shipping_date").cast("date")
                < col("order_date").cast("date")
            )
        )
        .count()
    )

    add_result(
        "Expédition antérieure à la commande",
        invalid_shipping_dates,
    )

    delivery_before_shipping = (
        customer_order_360
        .filter(
            col("delivery_date").isNotNull()
            & col("shipping_date").isNotNull()
            & (
                col("delivery_date")
                < col("shipping_date")
            )
        )
        .count()
    )

    add_result(
        "Livraison antérieure à l'expédition",
        delivery_before_shipping,
    )

    invalid_event_dates = (
        customer_order_360
        .filter(
            col(
                "first_event_timestamp"
            ).isNotNull()
            & col(
                "last_event_timestamp"
            ).isNotNull()
            & (
                col(
                    "last_event_timestamp"
                )
                < col(
                    "first_event_timestamp"
                )
            )
        )
        .count()
    )

    add_result(
        "Dernier événement antérieur au premier",
        invalid_event_dates,
    )

    # --------------------------------------------------------
    # 8. Vérification des statuts
    # --------------------------------------------------------

    invalid_statuses = (
        customer_order_360
        .filter(
            col("order_status").isNull()
            | ~col("order_status").isin(
                VALID_STATUSES
            )
        )
        .count()
    )

    add_result(
        "Statuts de commande non normalisés",
        invalid_statuses,
    )

    # --------------------------------------------------------
    # 9. Cohérence commande annulée / livraison
    # --------------------------------------------------------

    cancelled_and_delivered = (
        customer_order_360
        .filter(
            (
                col("order_status")
                == "CANCELLED"
            )
            & (
                col("last_delivery_status")
                == "DELIVERED"
            )
        )
        .count()
    )

    add_result(
        "Commandes annulées marquées livrées",
        cancelled_and_delivered,
    )

    # --------------------------------------------------------
    # 10. Cohérence livraison / date
    # --------------------------------------------------------

    delivered_without_date = (
        customer_order_360
        .filter(
            (
                (
                    col("order_status")
                    == "DELIVERED"
                )
                | (
                    col(
                        "last_delivery_status"
                    )
                    == "DELIVERED"
                )
            )
            & col(
                "delivery_date"
            ).isNull()
        )
        .count()
    )

    add_result(
        "Commandes livrées sans date",
        delivered_without_date,
    )

    delivery_date_without_delivery = (
        customer_order_360
        .filter(
            col("delivery_date").isNotNull()
            & (
                col("last_delivery_status")
                != "DELIVERED"
            )
        )
        .count()
    )

    add_result(
        "Date de livraison sans statut DELIVERED",
        delivery_date_without_delivery,
    )

    # --------------------------------------------------------
    # 11. Cohérence des indicateurs de présence
    # --------------------------------------------------------

    customer_flag_inconsistency = (
        customer_order_360
        .filter(
            (
                col("customer_found")
                == False
            )
            & (
                col("first_name").isNotNull()
                | col("last_name").isNotNull()
                | col("email").isNotNull()
            )
        )
        .count()
    )

    add_result(
        "Incohérence de l'indicateur customer_found",
        customer_flag_inconsistency,
    )

    delivery_flag_inconsistency = (
        customer_order_360
        .filter(
            (
                col("delivery_found")
                == False
            )
            & (
                col(
                    "first_event_timestamp"
                ).isNotNull()
                | col(
                    "last_event_timestamp"
                ).isNotNull()
                | (
                    col(
                        "number_of_events"
                    ) > 0
                )
            )
        )
        .count()
    )

    add_result(
        "Incohérence de l'indicateur delivery_found",
        delivery_flag_inconsistency,
    )

    # --------------------------------------------------------
    # Création du DataFrame final
    # --------------------------------------------------------

    validation_results = (
        spark.createDataFrame(
            validations,
            schema=[
                "controle",
                "nombre_anomalies",
                "statut",
            ],
        )
    )

    return validation_results