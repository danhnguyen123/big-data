import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


import pyspark
from pyspark.sql import SparkSession

# spark = SparkSession.\
#         builder.\
#         appName("pyspark-notebook").\
#         master("spark://spark-master:7077").\
#         config("spark.executor.memory", "4G").\
#         getOrCreate()

def get_spark_context(app_name: str) -> SparkSession:
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
            # ("spark.submit.deployMode", "client"),
            # ("spark.driver.bindAddress", "0.0.0.0"),
            ("spark.app.name", app_name),
            ("spark.dynamicAllocation.enabled", "false"),
            ("spark.executor.memory", "4G"),
        ]
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()