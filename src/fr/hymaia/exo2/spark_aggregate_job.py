from pyspark.sql import SparkSession
from .aggregate import main as aggregate


def main():
    agg_job = (SparkSession.builder
                 .appName("aggregate")
                 .master("local[*]")  # Signifie "utilise tous les threads"
                 .getOrCreate())

    aggregate.main(agg_job)
