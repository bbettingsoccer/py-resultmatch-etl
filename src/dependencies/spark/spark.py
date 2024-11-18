from pyspark import SparkFiles, SparkConf
import pyspark as spark
from pyspark.sql import SparkSession
import __main__
from os import environ, listdir, path
import json
from ..logging import logging


def start_spark(app_name=None, master='local[*]', jar_packages=None,
                files=None, spark_config=None):
    """Start Spark session, get Spark logger and load config files.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """
    conf = SparkConf().set("spark.eventLog.enabled", "true")

    # detect execution environment
    if spark_config is None:
        spark_config = {}
    if files is None:
        files = []
    if jar_packages is None:
        jar_packages = []
    flag_repl = not (hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        print("AQUI EH NOIS")
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master("spark://127.0.0.1:7077")
            .appName(app_name)
            .config(conf=conf))
    else:
        # get Spark session factory
        print("AQUI AGORA ")
        spark_builder = (
            SparkSession
            .builder
            .master("spark://127.0.0.1:7077")
            .appName(app_name)
            .config(conf=conf))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_session = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_session)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_session, spark_logger, config_dict
