from pathlib import Path


def save_gold_data(gold, output_dir="data/gold"):

    """
    Sauvegarde tous les DataFrames Gold au format Parquet.
    """

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for name, df in gold.items():

        destination = str(output_path / name)

        (
            df.write
            .mode("overwrite")
            .parquet(destination)
        )

        print(f"{name} sauvegardé dans : {destination}")