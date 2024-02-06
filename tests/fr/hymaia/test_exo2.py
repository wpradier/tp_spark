from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.clean.main import filterMinorsOut, joinOnZip, addDepartement
from src.fr.hymaia.exo2.aggregate.main import agg_and_count
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_filterminors(self):
        input = spark.createDataFrame([
            Row(age=17),
            Row(age=1),
            Row(age=18),
            Row(age=77)
        ])

        expected = spark.createDataFrame([
            Row(age=18),
            Row(age=77)
        ])

        actual = filterMinorsOut(input)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_joinonzip(self):
        base_input = spark.createDataFrame([
            Row(zip="75012", arr="12"),
            Row(zip="75013", arr="13"),
            Row(zip="75014", arr="14")
        ])
        tojoin_input = spark.createDataFrame([
            Row(zip="75012", text="coucou"),
            Row(zip="75013", text="salut"),
            Row(zip="75015", text="bonsoir")
        ])

        expected = spark.createDataFrame([
            Row(zip="75012", arr="12", text="coucou"),
            Row(zip="75013", arr="13", text="salut"),
        ])

        actual = joinOnZip(base_input, tojoin_input)
        self.assertCountEqual(actual.collect(), expected.collect())

    def test_adddepartement(self):
        input = spark.createDataFrame([
            Row(zip="75012"),
            Row(zip="92600"),
            Row(zip="20100"),
            Row(zip="20190"),
            Row(zip="20191"),
            Row(zip="20350"),
        ])

        expected = spark.createDataFrame([
            Row(zip="75012", departement="75"),
            Row(zip="92600", departement="92"),
            Row(zip="20100", departement="2A"),
            Row(zip="20190", departement="2A"),
            Row(zip="20191", departement="2B"),
            Row(zip="20350", departement="2B"),
        ])

        actual = addDepartement(input)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_aggandcount(self):
        input = spark.createDataFrame([
            Row(departement="75"),
            Row(departement="75"),
            Row(departement="2A"),
            Row(departement="2A"),
            Row(departement="2A"),
            Row(departement="01"),
        ])

        expected = spark.createDataFrame([
            Row(departement="75", total=2),
            Row(departement="2A", total=3),
            Row(departement="01", total=1),
        ])

        actual = agg_and_count(input, "departement", "total")

        self.assertCountEqual(actual.collect(), expected.collect())