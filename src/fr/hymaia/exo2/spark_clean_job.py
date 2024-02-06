from pyspark.sql import SparkSession
from .clean import main as clean


def main():
    clean_job = (SparkSession.builder
             .appName("clean")
             .master("local[*]")  # Signifie "utilise tous les threads"
             .getOrCreate())

    clean.main(clean_job)