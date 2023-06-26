from pyspark.sql import SparkSession
from pyspark import SparkConf

from appSpark.DataReader import DataReader
from appSpark.DataTransformer import DataTransformer
from appSpark.DataWriter import DataWriter

import logging

def create_spark_session(app_name="appSpark"):
    conf = SparkConf()
    conf.set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:./log4j.properties")
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark

if __name__ == "__main__":
    spark = create_spark_session()
    logger = logging.getLogger("appLogger")

    logger.info("Informacja - wygenerowane przeez użytkownika")
    logger.error("Błąd - wygenerowane przeez użytkownika")
    logger.warning("Ostrzeżenie - wygenerowane przeez użytkownika")

    data_reader = DataReader(spark)
    df = data_reader.read_csv("../wine.csv")
    df.show()

    df = DataTransformer.cast_to_int(df, "Alcohol")
    df = DataTransformer.agg_sum(df, "Wine", "Alcohol")
    df.show()

    DataWriter.write_csv(df, "../wine_sum.csv")

    logger.warning("Ostrzeżenie - do widzenia!")


