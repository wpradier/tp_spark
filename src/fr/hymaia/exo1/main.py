import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.builder
             .appName("wordcount")
             .master("local[*]") # Signifie "utilise tous les threads"
             .getOrCreate())
    file = spark.read.option("header", True).csv("src/resources/exo1/data.csv")
    count = wordcount(file, "text")
    count.show()
    count.write.partitionBy("count").mode("overwrite").parquet("src/data/exo1/output")



def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()


if __name__ == '__main__':
    main()
