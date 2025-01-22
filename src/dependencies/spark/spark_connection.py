import os
from pyspark.sql import SparkSession
from ..logging import logging
from ...config.enviroment_conf import set_spark_config_environment, set_spark_config_database


def start_spark(app_name=None):
    """Start Spark session, get Spark logger and load config files.
    :param app_name: Name of Spark app.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # SET config environment - Apache-Spark
    data_env_config = set_spark_config_environment()

    # SET config environment - Database
    data_db_config = set_spark_config_database()

    # get Spark session factory
    spark_builder = (
        SparkSession
        .builder
        .master(os.getenv("BOOTSTRAP_SPARK_CLUSTER"))
        .appName(app_name))

    # add other config params > ENVIRONMENT
    if data_env_config is not None:
        for key, val in data_env_config.items():
            spark_builder.config(key, val)

    # add other config params > DATABASE

    if data_db_config is not None:
        for key, val in data_db_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_session = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_session)

    return spark_session, spark_logger
