import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pandas
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os
import time   

def spark_session(app_name: str) -> SparkSession:
    """
    Helper to manage the `SparkContext` and keep all of our
    configuration params in one place. See below comments for details:
        |_ https://github.com/bitnami/bitnami-docker-spark/issues/18#issuecomment-700628676
        |_ https://github.com/leriel/pyspark-easy-start/blob/master/read_file.py
    """

    conf = SparkConf()

    conf.setAll(
        [
            ("spark.master", "spark://spark-master:7077"),
            # ("spark.driver.host", "172.23.0.2"),
            ("spark.submit.deployMode", "client"),
            ("spark.driver.bindAddress", "0.0.0.0"),
            ("spark.app.name", app_name),
            ("spark.executor.memory", "2G"),
            ("spark.dynamicAllocation.enabled", "false")
        ]
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()

def pandas_read_csv():
    print(os.getcwd())
    salesDfPandas = pandas.read_csv('plugins/data/salesRecord.csv')
    return salesDfPandas


def spark_dataframe():
    spark = spark_session(app_name = "pyspark-notebook")
    salesDfPandas = pandas_read_csv()
    # print(salesDfPandas)
    salesDfSpark=spark.createDataFrame(salesDfPandas)

    salesDfSpark = salesDfSpark.select([F.col(col).alias(col.replace(' ', '_')) for col in salesDfSpark.columns])
    print("Sales Dataframe created with schema : ")
    salesDfSpark.printSchema()

    # Write Dataframe into HDFS
    # Repartition it by "Country" column before storing as parquet files in Hadoop
    csvName = "salesRecord"
    epochNow = int(time.time())
    salesDfSpark.write.option("header",True) \
            .partitionBy("Country") \
            .mode("overwrite") \
            .parquet("hdfs://hadoop-namenode:9000/sales/{}_{}.parquet".format(csvName,epochNow))
    print("Sales Dataframe stored in Hadoop.")

    # salesDfSpark.show()
    spark.stop()
