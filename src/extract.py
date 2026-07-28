from pathlib import Path
from typing import Dict

from pyhocon import ConfigFactory
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


# ==========================================================
# CONFIGURATION
# ==========================================================

def load_config(path: str):
    """
    Charge le fichier de configuration HOCON.
    """

    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(
            f"Fichier de configuration introuvable : {path}"
        )

    return ConfigFactory.parse_file(path)


# ==========================================================
# SPARK
# ==========================================================

def create_spark_session(config) -> SparkSession:
    """
    Crée la session Spark configurée pour PostgreSQL et MongoDB.

    Les connecteurs sont fournis dans la commande spark-submit
    avec l'option --packages.
    """

    spark = (
        SparkSession.builder
        .appName(config.get("spark.app_name"))
        .master(config.get("spark.master"))
        .config(
            "spark.jars.ivy",
            "/tmp/ivy-cache",
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(config.get("spark.shuffle_partitions")),
        )
        .config(
            "spark.default.parallelism",
            str(config.get("spark.default_parallelism")),
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
    Affiche les informations principales de Spark.
    """

    print("\n=== INFORMATIONS SPARK ===")
    print(f"Application Name: {spark.sparkContext.appName}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"Spark Version: {spark.version}")
    print(
        f"Default Parallelism: "
        f"{spark.sparkContext.defaultParallelism}"
    )


# ==========================================================
# POSTGRESQL
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

    table_key = f"{table_name}_table"
    table = jdbc_conf.get(table_key)

    if not table:
        raise ValueError(
            f"Configuration absente : jdbc.{table_key}"
        )

    print(
        f"Chargement PostgreSQL : "
        f"{table_name} -> {table}"
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
    Charge toutes les tables PostgreSQL utilisées
    dans la couche Bronze.
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
# MONGODB
# ==========================================================

def load_reviews(
    spark: SparkSession,
    config,
) -> DataFrame:
    """
    Charge la collection MongoDB contenant les reviews.
    """

    mongo_conf = config.get("mongodb")

    uri = mongo_conf.get("uri")
    database = mongo_conf.get("database")
    collection = mongo_conf.get("collection")

    if not uri:
        raise ValueError(
            "Configuration absente : mongodb.uri"
        )

    if not database:
        raise ValueError(
            "Configuration absente : mongodb.database"
        )

    if not collection:
        raise ValueError(
            "Configuration absente : mongodb.collection"
        )

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
# CONTRÔLE DES VALEURS NULLES
# ==========================================================

def print_null_counts(df: DataFrame) -> None:
    """
    Affiche le nombre de valeurs nulles par colonne.

    Pour les colonnes de type chaîne, les chaînes vides
    sont également comptées.
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
# SCHÉMA JSON DELIVERY EVENTS
# ==========================================================

def delivery_events_schema() -> StructType:
    """
    Retourne le schéma manuel attendu pour les événements
    de livraison.
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
    Récupère et valide le chemin des événements de livraison.

    Le chemin configuré peut désigner :
    - un fichier JSON précis ;
    - un dossier contenant un ou plusieurs fichiers JSON.

    Lorsqu'il s'agit d'un dossier, un motif "*.json" est renvoyé
    afin que Spark ne tente pas de lire d'autres types de fichiers.
    """

    path = config.get("paths.delivery_events")

    if not path:
        raise ValueError(
            "Configuration absente : paths.delivery_events"
        )

    json_path = Path(path)

    if not json_path.exists():
        raise FileNotFoundError(
            f"Chemin JSON introuvable : {path}"
        )

    # Cas 1 : le chemin désigne directement un fichier JSON.
    if json_path.is_file():
        if json_path.suffix.lower() != ".json":
            raise ValueError(
                f"Le fichier n'a pas l'extension .json : {path}"
            )

        if json_path.stat().st_size == 0:
            raise ValueError(
                f"Le fichier JSON est vide : {path}"
            )

        return str(json_path)

    # Cas 2 : le chemin désigne un dossier.
    if json_path.is_dir():
        json_files = sorted(
            file
            for file in json_path.glob("*.json")
            if file.is_file() and file.stat().st_size > 0
        )

        if not json_files:
            raise FileNotFoundError(
                f"Aucun fichier JSON non vide trouvé dans : {path}"
            )

        print(
            f"{len(json_files)} fichier(s) JSON trouvé(s) "
            f"dans : {path}"
        )

        # Spark comprend les chemins avec caractères génériques.
        return str(json_path / "*.json")

    raise ValueError(
        f"Le chemin n'est ni un fichier ni un dossier : {path}"
    )


# ==========================================================
# JSON DELIVERY EVENTS
# ==========================================================

def load_delivery_events(
    spark: SparkSession,
    config,
) -> DataFrame:
    """
    Charge les événements de livraison avec le schéma manuel.

    Les fichiers contiennent des tableaux JSON multi-lignes
    commençant par "[". L'option multiLine est donc obligatoire.
    """

    path = get_delivery_events_path(config)
    schema = delivery_events_schema()

    print(
        f"Chargement JSON delivery events : {path}"
    )

    dataframe = (
        spark.read
        .option(
            "multiLine",
            "true",
        )
        .schema(schema)
        .json(path)
    )

    return dataframe


def compare_json_schema(
    spark: SparkSession,
    config,
) -> None:
    """
    Compare le schéma inféré par Spark avec le schéma manuel.
    """

    path = get_delivery_events_path(config)
    expected_schema = delivery_events_schema()

    print(
        f"Chemin JSON utilisé : {path}"
    )

    inferred_dataframe = (
        spark.read
        .option(
            "multiLine",
            "true",
        )
        .json(path)
    )

    manual_dataframe = (
        spark.read
        .option(
            "multiLine",
            "true",
        )
        .schema(expected_schema)
        .json(path)
    )

    print("\n=== SCHEMA INFÉRÉ ===")
    inferred_dataframe.printSchema()

    print("\n=== SCHEMA MANUEL ===")
    manual_dataframe.printSchema()

    inferred_schema = inferred_dataframe.schema

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
            "Champs supplémentaires dans le schéma inféré : "
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

