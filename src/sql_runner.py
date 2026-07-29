from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession


def read_sql_file(sql_path: Path) -> str:
    """
    Lit le contenu d'un fichier SQL.
    """

    if not sql_path.exists():
        raise FileNotFoundError(
            f"Fichier SQL introuvable : {sql_path}"
        )

    return sql_path.read_text(
        encoding="utf-8"
    ).strip()


def normalize_sql_query(query: str) -> str:
    """
    Retire le point-virgule final pour éviter certains problèmes
    lors de l'exécution de la requête via JDBC.
    """

    return query.rstrip().rstrip(";")


def execute_postgresql_query(
    spark: SparkSession,
    query: str,
    jdbc_url: str,
    connection_properties: Dict[str, str],
) -> DataFrame:
    """
    Exécute une requête PostgreSQL avec Spark JDBC.

    La requête est utilisée comme sous-requête JDBC.
    """

    normalized_query = normalize_sql_query(query)

    dbtable = f"""
        (
            {normalized_query}
        ) AS analytical_result
    """

    return (
        spark.read
        .jdbc(
            url=jdbc_url,
            table=dbtable,
            properties=connection_properties,
        )
    )


def run_analytical_queries(
    spark: SparkSession,
    sql_directory: str,
    jdbc_url: str,
    connection_properties: Dict[str, str],
) -> Dict[str, DataFrame]:
    """
    Exécute tous les fichiers SQL présents dans le dossier
    analytical_queries.
    """

    directory = Path(sql_directory)

    if not directory.exists():
        raise FileNotFoundError(
            f"Dossier SQL introuvable : {directory}"
        )

    sql_files = sorted(
        directory.glob("*.sql")
    )

    if not sql_files:
        print(
            f"Aucune requête SQL trouvée dans : "
            f"{directory}"
        )
        return {}

    results: Dict[str, DataFrame] = {}

    for sql_file in sql_files:
        query_name = sql_file.stem

        print(
            f"\n--- Exécution SQL : "
            f"{sql_file.name} ---"
        )

        try:
            query = read_sql_file(
                sql_file
            )

            result = execute_postgresql_query(
                spark=spark,
                query=query,
                jdbc_url=jdbc_url,
                connection_properties=(
                    connection_properties
                ),
            )

            results[query_name] = result

            result.show(
                20,
                truncate=False,
            )

        except Exception as error:
            print(
                f"Erreur pendant l'exécution de "
                f"{sql_file.name} : {error}"
            )

    return results