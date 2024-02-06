import shutil

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main(spark: SparkSession):
    clean = spark.read.option("header", True).parquet(
        "src/data/exo2/clean")

    aggregated = agg_and_count(clean, "departement", "nb_people")

    aggregated.write.mode("overwrite").csv("src/data/exo2/aggregate")
    shutil.rmtree('src/data/exo2/clean')


def agg_and_count(df, field, count_col_name="count"):
    return df.groupBy(df[field]).count().withColumnRenamed("count", count_col_name)
