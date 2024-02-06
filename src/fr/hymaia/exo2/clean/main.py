import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main(spark: SparkSession):

    city = spark.read.option("header", True).csv(
        "src/resources/exo2/city_zipcode.csv")
    clients = spark.read.option("header", True).csv(
        "src/resources/exo2/clients_bdd.csv")

    clients = filterMinorsOut(clients)
    final = joinOnZip(clients, city)

    final = addDepartement(final)

    final.write.mode("overwrite").parquet("src/data/exo2/clean")


def filterMinorsOut(df):
    return df.where(df["age"] >= 18)


def joinOnZip(base, tojoin):
    return base.join(tojoin, on="zip")


def addDepartement(df):
    departement_col = (f.when(df["zip"][0:2] != "20", df["zip"][0:2])
                       .when(df["zip"] <= 20190, "2A")
                       .otherwise("2B"))

    return df.withColumn("departement", departement_col)

