from pathlib import Path
from typing import Dict

from pyhocon import ConfigFactory
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, count, when, first, max, min, row_number
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


# ==========================================================
# Configuration
# ==========================================================

def load_config(path: str):
    """
    Charge le fichier de configuration HOCON.

    Parameters
    ----------
    path:
        Chemin vers le fichier application.conf.
    """
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(
            f"Fichier de configuration introuvable : {path}"
        )

    return ConfigFactory.parse_file(path)


# ==========================================================
# Spark
# ==========================================================

def create_spark_session(config) -> SparkSession:
    """
    Crée et configure la session Spark.

    Les dépendances JDBC et MongoDB sont normalement fournies
    par spark-submit avec l'option --packages.
    """

    spark = (
        SparkSession.builder
        .appName(config.get("spark.app_name"))
        .master(config.get("spark.master"))
        .config(
            "spark.sql.shuffle.partitions",
            str(config.get("spark.shuffle_partitions")),
        )
        .config(
            "spark.default.parallelism",
            str(config.get("spark.default_parallelism")),
        )
        .config(
            "spark.jars.ivy",
            "/tmp/ivy-cache",
        )
        .config(
            "spark.mongodb.read.connection.uri",
            config.get("mongodb.uri"),
        )
        .config(
            "spark.mongodb.write.connection.uri",
            config.get("mongodb.uri"),
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        config.get("spark.log_level")
    )

    return spark


def print_spark_info(spark: SparkSession) -> None:
    """
    Affiche les informations principales de la session Spark.
    """

    print("\n=== INFORMATIONS SPARK ===")
    print(
        f"Application Name: "
        f"{spark.sparkContext.appName}"
    )
    print(
        f"Master: "
        f"{spark.sparkContext.master}"
    )
    print(
        f"Spark Version: "
        f"{spark.version}"
    )
    print(
        f"Default Parallelism: "
        f"{spark.sparkContext.defaultParallelism}"
    )


# ==========================================================
# PostgreSQL
# ==========================================================

def load_jdbc_table(
    spark: SparkSession,
    config,
    table_name: str,
) -> DataFrame:
    """
    Charge une table PostgreSQL avec JDBC.
    """

    jdbc_conf = config.get("jdbc")

    table_config_key = f"{table_name}_table"

    if table_config_key not in jdbc_conf:
        raise KeyError(
            f"Configuration absente : "
            f"jdbc.{table_config_key}"
        )

    table = jdbc_conf.get(table_config_key)

    print(
        f"Chargement PostgreSQL : "
        f"{table_name} ({table})"
    )

    return (
        spark.read
        .format("jdbc")
        .option(
            "url",
            jdbc_conf.get("url"),
        )
        .option(
            "driver",
            jdbc_conf.get("driver"),
        )
        .option(
            "dbtable",
            table,
        )
        .option(
            "user",
            jdbc_conf.get("user"),
        )
        .option(
            "password",
            jdbc_conf.get("password"),
        )
        .load()
    )


def load_relational_data(
    spark: SparkSession,
    config,
) -> Dict[str, DataFrame]:
    """
    Charge toutes les tables relationnelles Bronze.
    """

    table_names = [
        "customers",
        "orders",
        "order_items",
        "products",
    ]

    tables = {}

    for table_name in table_names:
        tables[table_name] = load_jdbc_table(
            spark=spark,
            config=config,
            table_name=table_name,
        )

    return tables


# ==========================================================
# MongoDB
# ==========================================================

def load_reviews(
    spark: SparkSession,
    config,
) -> DataFrame:
    """
    Charge la collection MongoDB contenant les avis.
    """

    mongo_conf = config.get("mongodb")

    uri = mongo_conf.get("uri")
    database = mongo_conf.get("database")
    collection = mongo_conf.get("collection")

    print(
        f"Chargement MongoDB : "
        f"{database}.{collection}"
    )

    return (
        spark.read
        .format("mongodb")
        .option(
            "connection.uri",
            uri,
        )
        .option(
            "database",
            database,
        )
        .option(
            "collection",
            collection,
        )
        .load()
    )


# ==========================================================
# Contrôle des valeurs nulles
# ==========================================================

def print_null_counts(df: DataFrame) -> None:
    """
    Affiche le nombre de valeurs nulles ou de chaînes vides
    pour chaque colonne simple du DataFrame.
    """

    if not df.columns:
        print(
            "Le DataFrame ne contient aucune colonne."
        )
        return

    expressions = []

    for column_name in df.columns:
        data_type = df.schema[column_name].dataType

        if isinstance(data_type, StringType):
            condition = (
                col(column_name).isNull()
                | (col(column_name) == "")
            )
        else:
            condition = col(column_name).isNull()

        expressions.append(
            count(
                when(
                    condition,
                    column_name,
                )
            ).alias(column_name)
        )

    df.select(expressions).show(
        truncate=False
    )


# ==========================================================
# JSON Delivery Events
# ==========================================================

def delivery_events_schema() -> StructType:
    """
    Retourne le schéma attendu pour les événements de livraison.
    """

    return StructType(
        [
            StructField(
                "event_id",
                StringType(),
                True,
            ),
            StructField(
                "order_id",
                StringType(),
                True,
            ),
            StructField(
                "event_type",
                StringType(),
                True,
            ),
            StructField(
                "event_timestamp",
                StringType(),
                True,
            ),
            StructField(
                "location",
                StructType(
                    [
                        StructField(
                            "city",
                            StringType(),
                            True,
                        ),
                        StructField(
                            "country",
                            StringType(),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                "carrier",
                StructType(
                    [
                        StructField(
                            "id",
                            StringType(),
                            True,
                        ),
                        StructField(
                            "name",
                            StringType(),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )


def get_delivery_events_path(config) -> str:
    """
    Récupère et valide le chemin du fichier JSON.
    """

    path = config.get("paths.delivery_events")

    if not path:
        raise ValueError(
            "La configuration "
            "'paths.delivery_events' est absente."
        )

    local_path = Path(path)

    if not local_path.exists():
        raise FileNotFoundError(
            f"Fichier JSON introuvable : {path}"
        )

    if local_path.is_file() and local_path.stat().st_size == 0:
        raise ValueError(
            f"Le fichier JSON est vide : {path}"
        )

    return path


def load_delivery_events(
    spark: SparkSession,
    config,
) -> DataFrame:
    """
    Charge les événements de livraison avec un schéma manuel.

    Le fichier est un tableau JSON multi-lignes. L'option
    multiLine doit donc être activée.
    """

    path = get_delivery_events_path(config)
    schema = delivery_events_schema()

    print(
        f"Chargement JSON delivery events : "
        f"{path}"
    )

    return (
        spark.read
        .option(
            "multiLine",
            "true",
        )
        .schema(schema)
        .json(path)
    )


def compare_json_schema(
    spark: SparkSession,
    config,
) -> None:
    """
    Compare le schéma inféré par Spark au schéma manuel attendu.
    """

    path = get_delivery_events_path(config)
    expected_schema = delivery_events_schema()

    print(
        f"Chemin utilisé pour la comparaison : "
        f"{path}"
    )

    inferred_df = (
        spark.read
        .option(
            "multiLine",
            "true",
        )
        .json(path)
    )

    manual_df = (
        spark.read
        .option(
            "multiLine",
            "true",
        )
        .schema(expected_schema)
        .json(path)
    )

    print("\n=== SCHEMA INFÉRÉ ===")
    inferred_df.printSchema()

    print("\n=== SCHEMA MANUEL ===")
    manual_df.printSchema()

    inferred_schema = inferred_df.schema

    if inferred_schema == expected_schema:
        print(
            "\nLe schéma inféré correspond exactement "
            "au schéma manuel."
        )
        return

    print(
        "\nLe schéma inféré diffère du schéma manuel."
    )

    inferred_fields = {
        field.name: field
        for field in inferred_schema.fields
    }

    expected_fields = {
        field.name: field
        for field in expected_schema.fields
    }

    missing_fields = (
        expected_fields.keys()
        - inferred_fields.keys()
    )

    unexpected_fields = (
        inferred_fields.keys()
        - expected_fields.keys()
    )

    common_fields = (
        expected_fields.keys()
        & inferred_fields.keys()
    )

    if missing_fields:
        print(
            "Champs absents du schéma inféré : "
            + ", ".join(sorted(missing_fields))
        )

    if unexpected_fields:
        print(
            "Champs supplémentaires : "
            + ", ".join(sorted(unexpected_fields))
        )

    for field_name in sorted(common_fields):
        inferred_type = (
            inferred_fields[field_name]
            .dataType
            .simpleString()
        )

        expected_type = (
            expected_fields[field_name]
            .dataType
            .simpleString()
        )

        if inferred_type != expected_type:
            print(
                f"Type différent pour '{field_name}' : "
                f"inféré={inferred_type}, "
                f"manuel={expected_type}"
            )

def build_delivery_summary(
    delivery_events: DataFrame,
) -> DataFrame:
    """
    Construit une ligne récapitulative par commande à partir
    des événements de livraison nettoyés.
    """

    order_window_desc = (
        Window
        .partitionBy("order_id")
        .orderBy(
            col("event_timestamp").desc_nulls_last(),
            col("event_id").desc(),
        )
    )

    events_ranked = (
        delivery_events
        .withColumn(
            "_event_rank",
            row_number().over(order_window_desc),
        )
    )

    last_event = (
        events_ranked
        .filter(col("_event_rank") == 1)
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
            min("event_timestamp").alias(
                "first_event_timestamp"
            ),
            count("*").alias(
                "number_of_events"
            ),
            min(
                when(
                    col("event_type").isin(
                        "SHIPPED",
                        "IN_TRANSIT",
                        "OUT_FOR_DELIVERY",
                    ),
                    col("event_timestamp"),
                )
            ).alias("shipping_date"),
            min(
                when(
                    col("event_type") == "DELIVERED",
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