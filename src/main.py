from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sql_runner import run_analytical_queries

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

from quality import (
    quality_silver_data,
    build_validation_results,
)

from transformation import transform_silver_to_gold

from load import save_gold_data


# ============================================================
# CONFIGURATION POSTGRESQL
# ============================================================

jdbc_url = (
    "jdbc:postgresql://postgres:5432/"
    "tp_multisource"
)

connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

def count_dataframes(
    dataframes: Dict[str, DataFrame],
) -> Dict[str, int]:
    """
    Retourne le nombre de lignes de chaque DataFrame.
    """

    return {
        name: dataframe.count()
        for name, dataframe in dataframes.items()
    }


def display_dataframes(
    dataframes: Dict[str, DataFrame],
    layer_name: str,
    number_of_rows: int = 5,
) -> None:
    """
    Affiche le schéma et un aperçu de chaque DataFrame.
    """

    for name, dataframe in dataframes.items():
        print(
            f"\n=== {name.upper()} "
            f"({layer_name.upper()}) ==="
        )

        dataframe.printSchema()

        dataframe.show(
            number_of_rows,
            truncate=False,
        )


def prepare_rejected_dataframe(
    dataframe: DataFrame,
    source_name: str,
) -> DataFrame:
    """
    Standardise un DataFrame de rejet.

    Colonnes techniques produites :
        - source ;
        - rejection_reason ;
        - rejection_timestamp ;
        - original_data.
    """

    technical_columns = {
        "source",
        "rejection_reason",
        "rejection_timestamp",
        "original_data",
    }

    original_columns = [
        F.col(column_name)
        for column_name in dataframe.columns
        if column_name not in technical_columns
    ]

    result = dataframe

    if "rejection_reason" not in result.columns:
        result = result.withColumn(
            "rejection_reason",
            F.lit(
                "Motif de rejet non renseigné"
            ),
        )
    else:
        result = result.withColumn(
            "rejection_reason",
            F.when(
                F.col(
                    "rejection_reason"
                ).isNull()
                | (
                    F.trim(
                        F.col(
                            "rejection_reason"
                        ).cast("string")
                    )
                    == ""
                ),
                F.lit(
                    "Motif de rejet non renseigné"
                ),
            ).otherwise(
                F.col(
                    "rejection_reason"
                ).cast("string")
            ),
        )

    result = (
        result
        .withColumn(
            "source",
            F.lit(source_name),
        )
        .withColumn(
            "rejection_timestamp",
            F.current_timestamp(),
        )
    )

    if original_columns:
        result = result.withColumn(
            "original_data",
            F.to_json(
                F.struct(
                    *original_columns
                )
            ),
        )
    else:
        result = result.withColumn(
            "original_data",
            F.lit("{}"),
        )

    first_columns = [
        "source",
        "rejection_reason",
        "rejection_timestamp",
        "original_data",
    ]

    remaining_columns = [
        column_name
        for column_name in result.columns
        if column_name not in first_columns
    ]

    return result.select(
        *first_columns,
        *remaining_columns,
    )


def save_rejected_data(
    rejected: Dict[str, DataFrame],
    base_output_path: str = (
        "data/output/rejects"
    ),
) -> Dict[str, int]:
    """
    Sauvegarde les données rejetées au format Parquet.

    Retourne le nombre de rejets par source.
    """

    rejected_counts: Dict[str, int] = {}

    for source_name, dataframe in rejected.items():
        standardized_dataframe = (
            prepare_rejected_dataframe(
                dataframe=dataframe,
                source_name=source_name,
            )
        )

        rejected_count = (
            standardized_dataframe.count()
        )

        rejected_counts[
            source_name
        ] = rejected_count

        output_path = (
            f"{base_output_path}/"
            f"{source_name}"
        )

        (
            standardized_dataframe.write
            .mode("overwrite")
            .parquet(output_path)
        )

        print(
            f"Rejets {source_name} sauvegardés "
            f"dans : {output_path}"
        )

        print(
            f"Nombre de rejets sauvegardés : "
            f"{rejected_count}"
        )

    return rejected_counts


def display_rejected_summary(
    rejected: Dict[str, DataFrame],
) -> None:
    """
    Affiche le nombre de rejets et leur répartition
    par motif.
    """

    print(
        "\n=== RÉSUMÉ DES REJETS SILVER ==="
    )

    for source_name, dataframe in rejected.items():
        rejected_count = dataframe.count()

        print(
            f"\n{source_name}: "
            f"{rejected_count} ligne(s) rejetée(s)"
        )

        if (
            rejected_count > 0
            and "rejection_reason"
            in dataframe.columns
        ):
            (
                dataframe
                .groupBy(
                    "rejection_reason"
                )
                .count()
                .orderBy(
                    F.col("count").desc()
                )
                .show(
                    truncate=False
                )
            )


def save_sql_results(
    sql_results: Dict[str, DataFrame],
    base_output_path: str = (
        "data/output/sql_results"
    ),
) -> Dict[str, int]:
    """
    Sauvegarde les résultats des requêtes SQL analytiques
    au format Parquet.

    Retourne le nombre de lignes de chaque résultat SQL.
    """

    sql_result_counts: Dict[str, int] = {}

    for query_name, dataframe in sql_results.items():
        output_path = (
            f"{base_output_path}/"
            f"{query_name}"
        )

        row_count = dataframe.count()

        sql_result_counts[
            query_name
        ] = row_count

        (
            dataframe.write
            .mode("overwrite")
            .parquet(output_path)
        )

        print(
            f"Résultat SQL sauvegardé : "
            f"{output_path}"
        )

        print(
            f"Nombre de lignes SQL sauvegardées : "
            f"{row_count}"
        )

    return sql_result_counts


def validate_gold_dataframe(
    customer_order_360: DataFrame,
    output_path: str = (
        "data/output/validation_results"
    ),
) -> DataFrame:
    """
    Lance les validations finales du DataFrame
    customer_order_360 et sauvegarde les résultats.
    """

    print(
        "\n=== VALIDATION DU RÉSULTAT FINAL ==="
    )

    print(
        "Colonnes customer_order_360 :",
        customer_order_360.columns,
    )

    validation_results = (
        build_validation_results(
            customer_order_360
        )
    )

    print(
        "\n=== RÉSULTATS DES VALIDATIONS ==="
    )

    (
        validation_results
        .orderBy(
            F.col("statut").desc(),
            F.col("controle"),
        )
        .show(
            truncate=False
        )
    )

    (
        validation_results.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(
        "\nRésultats des validations "
        "sauvegardés dans : "
        f"{output_path}"
    )

    controls_in_error = (
        validation_results
        .filter(
            F.col("statut") == "ERREUR"
        )
    )

    validation_error_count = (
        controls_in_error.count()
    )

    print(
        "Nombre de contrôles en erreur : "
        f"{validation_error_count}"
    )

    if validation_error_count > 0:
        print(
            "\n=== CONTRÔLES EN ERREUR ==="
        )

        controls_in_error.show(
            truncate=False
        )
    else:
        print(
            "\nTous les contrôles sont OK."
        )

    return validation_results


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def main() -> None:
    """
    Exécute le pipeline ETL multisource complet.

    Bronze :
        - PostgreSQL ;
        - MongoDB ;
        - fichiers JSON.

    SQL analytique :
        - lecture des fichiers .sql ;
        - exécution sur PostgreSQL ;
        - affichage des résultats ;
        - sauvegarde en Parquet.

    Silver :
        - nettoyage ;
        - standardisation ;
        - séparation des données valides et rejetées ;
        - contrôles qualité ;
        - sauvegarde des rejets.

    Gold :
        - transformations métier ;
        - construction de customer_order_360 ;
        - validations finales ;
        - sauvegarde des résultats.
    """

    config = load_config(
        "config/application.conf"
    )

    spark = None

    try:
        # ====================================================
        # 1. INITIALISATION DE SPARK
        # ====================================================

        print(
            "\n=== INITIALISATION DE SPARK ==="
        )

        spark = create_spark_session(
            config
        )

        spark.sparkContext.setLogLevel(
            "WARN"
        )

        print_spark_info(
            spark
        )

        # ====================================================
        # 2. BRONZE - POSTGRESQL
        # ====================================================

        print(
            "\n=== CHARGEMENT POSTGRESQL ==="
        )

        relational = load_relational_data(
            spark=spark,
            config=config,
        )

        required_relational_sources = {
            "customers",
            "orders",
            "order_items",
            "products",
        }

        missing_relational_sources = (
            required_relational_sources
            - set(relational.keys())
        )

        if missing_relational_sources:
            raise KeyError(
                "Sources PostgreSQL manquantes : "
                f"{sorted(missing_relational_sources)}"
            )

        # ====================================================
        # 3. REQUÊTES SQL ANALYTIQUES
        # ====================================================

        print(
            "\n========================================"
        )

        print(
            "=== REQUÊTES SQL ANALYTIQUES =========="
        )

        print(
            "========================================"
        )

        sql_results = run_analytical_queries(
            spark=spark,
            sql_directory=(
                "sql/analytical_queries"
            ),
            jdbc_url=jdbc_url,
            connection_properties=(
                connection_properties
            ),
        )

        print(
            f"\nNombre de requêtes SQL exécutées : "
            f"{len(sql_results)}"
        )

        print(
            "\n=== SAUVEGARDE DES RÉSULTATS SQL ==="
        )

        sql_result_counts = save_sql_results(
            sql_results=sql_results,
            base_output_path=(
                "data/output/sql_results"
            ),
        )

        # ====================================================
        # 4. BRONZE - MONGODB
        # ====================================================

        print(
            "\n=== CHARGEMENT MONGODB ==="
        )

        reviews = load_reviews(
            spark=spark,
            config=config,
        )

        print(
            "\n=== REVIEWS MONGODB ==="
        )

        reviews.printSchema()

        print(
            "Nombre de partitions :",
            reviews.rdd.getNumPartitions(),
        )

        review_count = reviews.count()

        print(
            "Nombre de reviews :",
            review_count,
        )

        reviews.show(
            5,
            truncate=False,
        )

        # ====================================================
        # 5. BRONZE - FICHIERS JSON
        # ====================================================

        print(
            "\n=== CHARGEMENT DELIVERY EVENTS ==="
        )

        delivery_events = (
            load_delivery_events(
                spark=spark,
                config=config,
            )
        )

        print(
            "\n=== DELIVERY EVENTS (BRONZE) ==="
        )

        delivery_events.printSchema()

        delivery_events.show(
            5,
            truncate=False,
        )

        print(
            "\n=== COMPARAISON DU SCHÉMA JSON ==="
        )

        compare_json_schema(
            spark=spark,
            config=config,
        )

        # ====================================================
        # 6. CONSTRUCTION DE LA COUCHE BRONZE
        # ====================================================

        bronze = {
            "customers": (
                relational["customers"]
            ),
            "orders": (
                relational["orders"]
            ),
            "order_items": (
                relational["order_items"]
            ),
            "products": (
                relational["products"]
            ),
            "reviews": reviews,
            "delivery_events": (
                delivery_events
            ),
        }

        print(
            "\n=== RÉSUMÉ BRONZE ==="
        )

        bronze_counts = {
            "Customers": (
                bronze["customers"].count()
            ),
            "Orders": (
                bronze["orders"].count()
            ),
            "Order Items": (
                bronze["order_items"].count()
            ),
            "Products": (
                bronze["products"].count()
            ),
            "Reviews": review_count,
            "Delivery Events": (
                bronze[
                    "delivery_events"
                ].count()
            ),
        }

        for source_name, row_count in (
            bronze_counts.items()
        ):
            print(
                f"{source_name}: {row_count}"
            )

        # ====================================================
        # 7. SILVER - NETTOYAGE ET REJETS
        # ====================================================

        print(
            "\n=== DÉBUT DU NETTOYAGE SILVER ==="
        )

        silver, rejected = (
            clean_bronze_data_with_rejects(
                bronze
            )
        )

        print(
            "\n=== SILVER - NETTOYAGE TERMINÉ ==="
        )

        required_silver_sources = {
            "customers",
            "orders",
            "order_items",
            "products",
            "reviews",
            "delivery_events",
        }

        missing_silver_sources = (
            required_silver_sources
            - set(silver.keys())
        )

        if missing_silver_sources:
            raise KeyError(
                "DataFrames Silver manquants : "
                f"{sorted(missing_silver_sources)}"
            )

        display_dataframes(
            dataframes=silver,
            layer_name="Silver",
            number_of_rows=5,
        )

        # ====================================================
        # 8. RÉSUMÉ DES REJETS
        # ====================================================

        display_rejected_summary(
            rejected
        )

        # ====================================================
        # 9. SAUVEGARDE DES REJETS
        # ====================================================

        print(
            "\n=== SAUVEGARDE DES REJETS ==="
        )

        rejected_counts = (
            save_rejected_data(
                rejected=rejected,
                base_output_path=(
                    "data/output/rejects"
                ),
            )
        )

        # ====================================================
        # 10. CONTRÔLES QUALITÉ SILVER
        # ====================================================

        print(
            "\n=== CONTRÔLE QUALITÉ SILVER ==="
        )

        quality_silver_data(
            silver
        )

        # ====================================================
        # 11. GOLD - TRANSFORMATIONS
        # ====================================================

        print(
            "\n=== CONSTRUCTION GOLD ==="
        )

        gold = transform_silver_to_gold(
            silver
        )

        if not isinstance(gold, dict):
            raise TypeError(
                "transform_silver_to_gold doit "
                "retourner un dictionnaire de "
                "DataFrames."
            )

        print(
            "\nDataFrames Gold disponibles :",
            list(gold.keys()),
        )

        display_dataframes(
            dataframes=gold,
            layer_name="Gold",
            number_of_rows=5,
        )

        # ====================================================
        # 12. VALIDATION FINALE
        # ====================================================

        required_gold_dataframe = (
            "customer_order_360"
        )

        if required_gold_dataframe not in gold:
            raise KeyError(
                "Le DataFrame "
                "'customer_order_360' est absent "
                "du dictionnaire Gold. "
                "DataFrames disponibles : "
                f"{list(gold.keys())}"
            )

        customer_order_360 = gold[
            required_gold_dataframe
        ]

        validation_results = (
            validate_gold_dataframe(
                customer_order_360=(
                    customer_order_360
                ),
                output_path=(
                    "data/output/"
                    "validation_results"
                ),
            )
        )

        # ====================================================
        # 13. SAUVEGARDE GOLD
        # ====================================================

        print(
            "\n=== SAUVEGARDE GOLD ==="
        )

        save_gold_data(
            gold
        )

        # ====================================================
        # 14. RÉSUMÉ FINAL
        # ====================================================

        print(
            "\n=== RÉSUMÉ FINAL ==="
        )

        print(
            "\nDonnées Bronze :"
        )

        for name, row_count in (
            bronze_counts.items()
        ):
            print(
                f"  - {name}: {row_count}"
            )

        print(
            "\nRésultats SQL analytiques :"
        )

        for name, row_count in (
            sql_result_counts.items()
        ):
            print(
                f"  - {name}: {row_count}"
            )

        print(
            "\nDonnées Silver valides :"
        )

        silver_counts = count_dataframes(
            silver
        )

        for name, row_count in (
            silver_counts.items()
        ):
            print(
                f"  - {name}: {row_count}"
            )

        print(
            "\nRejets Silver :"
        )

        for name, row_count in (
            rejected_counts.items()
        ):
            print(
                f"  - {name}: {row_count}"
            )

        gold_counts = count_dataframes(
            gold
        )

        print(
            "\nDonnées Gold :"
        )

        for name, row_count in (
            gold_counts.items()
        ):
            print(
                f"  - {name}: {row_count}"
            )

        validation_error_count = (
            validation_results
            .filter(
                F.col("statut")
                == "ERREUR"
            )
            .count()
        )

        print(
            "\nContrôles finaux en erreur : "
            f"{validation_error_count}"
        )

        print(
            "\n=== PIPELINE TERMINÉ "
            "AVEC SUCCÈS ==="
        )

    except Exception as error:
        print(
            "\n=== ÉCHEC DU PIPELINE ==="
        )

        print(
            "Type d'erreur : "
            f"{type(error).__name__}"
        )

        print(
            f"Message : {error}"
        )

        raise

    finally:
        if spark is not None:
            print(
                "\n=== ARRÊT DE SPARK ==="
            )

            spark.stop()


if __name__ == "__main__":
    main()