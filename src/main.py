from extract import (
    load_config,
    create_spark_session,
    print_spark_info,
    load_relational_data,
    load_reviews,
    load_delivery_events,
    compare_json_schema,
)

from cleaning import clean_bronze_data_with_rejects
from quality import quality_silver_data
from transformation import transform_silver_to_gold
from load import save_gold_data


def main():
    """
    Exécute le pipeline ETL complet.

    Bronze :
        - PostgreSQL
        - MongoDB
        - fichiers JSON

    Silver :
        - nettoyage
        - séparation des données valides et rejetées
        - contrôles qualité
        - sauvegarde des rejets

    Gold :
        - transformations métier
        - sauvegarde des résultats
    """

    config = load_config("config/application.conf")

    spark = None

    try:
        # ==========================
        # Initialisation de Spark
        # ==========================

        spark = create_spark_session(config)
        spark.sparkContext.setLogLevel("WARN")

        print_spark_info(spark)

        # ==========================
        # BRONZE - PostgreSQL
        # ==========================

        print("\n=== CHARGEMENT POSTGRESQL ===")

        relational = load_relational_data(
            spark=spark,
            config=config,
        )

        # ==========================
        # BRONZE - MongoDB
        # ==========================

        print("\n=== CHARGEMENT MONGODB ===")

        reviews = load_reviews(
            spark=spark,
            config=config,
        )

        print("\n=== REVIEWS MONGODB ===")
        reviews.printSchema()

        print(
            "Nombre de partitions :",
            reviews.rdd.getNumPartitions(),
        )

        review_count = reviews.count()
        print("Nombre de reviews :", review_count)

        # ==========================
        # BRONZE - JSON
        # ==========================

        print("\n=== CHARGEMENT DELIVERY EVENTS ===")

        delivery_events = load_delivery_events(
            spark=spark,
            config=config,
        )

        print("\n=== DELIVERY EVENTS (BRONZE) ===")
        delivery_events.printSchema()
        delivery_events.show(
            5,
            truncate=False,
        )

        print("\n=== COMPARAISON DU SCHEMA JSON ===")

        compare_json_schema(
            spark=spark,
            config=config,
        )

        # ==========================
        # Contrôle Bronze
        # ==========================

        print("\n=== RESUME BRONZE ===")

        bronze_counts = {
            "Customers": relational["customers"].count(),
            "Orders": relational["orders"].count(),
            "Order Items": relational["order_items"].count(),
            "Products": relational["products"].count(),
            "Reviews": review_count,
            "Delivery Events": delivery_events.count(),
        }

        for source_name, row_count in bronze_counts.items():
            print(f"{source_name}: {row_count}")

        # ==========================
        # Construction Bronze
        # ==========================

        bronze = {
            "customers": relational["customers"],
            "orders": relational["orders"],
            "order_items": relational["order_items"],
            "products": relational["products"],
            "reviews": reviews,
            "delivery_events": delivery_events,
        }

        # ==========================
        # SILVER - Nettoyage
        # ==========================

        print("\n=== DEBUT DU NETTOYAGE SILVER ===")

        silver, rejected = clean_bronze_data_with_rejects(
            bronze
        )

        print("\n=== SILVER - NETTOYAGE TERMINE ===")

        for name, dataframe in silver.items():
            print(f"\n=== {name.upper()} (SILVER) ===")

            dataframe.printSchema()
            dataframe.show(
                5,
                truncate=False,
            )

        # ==========================
        # Résumé des rejets
        # ==========================

        print("\n=== RESUME DES REJETS SILVER ===")

        rejected_counts = {}

        for name, dataframe in rejected.items():
            rejected_count = dataframe.count()
            rejected_counts[name] = rejected_count

            print(
                f"{name}: "
                f"{rejected_count} ligne(s) rejetée(s)"
            )

            if rejected_count > 0:
                dataframe.groupBy(
                    "rejection_reason"
                ).count().show(
                    truncate=False
                )

        # ==========================
        # Sauvegarde des rejets
        # ==========================

        print("\n=== SAUVEGARDE DES REJETS ===")

        for name, dataframe in rejected.items():
            output_path = f"data/rejected/{name}"

            dataframe.write.mode(
                "overwrite"
            ).parquet(
                output_path
            )

            print(
                f"Rejets {name} sauvegardés dans : "
                f"{output_path}"
            )

        # ==========================
        # Contrôle qualité Silver
        # ==========================

        print("\n=== CONTROLE QUALITE SILVER ===")

        quality_silver_data(silver)

        # ==========================
        # GOLD - Transformations
        # ==========================

        print("\n=== CONSTRUCTION GOLD ===")

        gold = transform_silver_to_gold(silver)

        print("\n=== GOLD ===")

        for name, dataframe in gold.items():
            print(f"\n=== {name.upper()} (GOLD) ===")

            dataframe.printSchema()
            dataframe.show(
                5,
                truncate=False,
            )

        # ==========================
        # LOAD - Sauvegarde Gold
        # ==========================

        print("\n=== SAUVEGARDE GOLD ===")

        save_gold_data(gold)

        # ==========================
        # Résumé final
        # ==========================

        print("\n=== RESUME FINAL ===")

        print("Données Bronze :")
        for name, count in bronze_counts.items():
            print(f"  - {name}: {count}")

        print("Rejets Silver :")
        for name, count in rejected_counts.items():
            print(f"  - {name}: {count}")

        print("\n=== PIPELINE TERMINE AVEC SUCCES ===")

    except Exception as error:
        print("\n=== ECHEC DU PIPELINE ===")
        print(f"Type d'erreur : {type(error).__name__}")
        print(f"Message : {error}")

        raise

    finally:
        if spark is not None:
            print("\n=== ARRET DE SPARK ===")
            spark.stop()


if __name__ == "__main__":
    main()