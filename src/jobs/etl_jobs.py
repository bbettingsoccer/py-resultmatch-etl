import os
from src.config.match_constants import MatchConstants
from src.dependencies.spark.spark_connection import start_spark
from src.dao.operationimpl_dao import OperationImplDAO


def execute_job(championship: str, match_date: str):
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log = start_spark(app_name=MatchConstants.NAME_JOB)

    # log that main ETL job is starting
    log.info('etl_job is up-and-running')

    # EXTRACT :: DATA -> SCHEMAS MONGODB
    schedule_df, scrapy_df = extract_data(spark=spark, championship=championship, match_date=match_date)
    # TRANSFORM :: DATAFRAME -> JOIN METHOD TWO COLLECTION
    joined_df = transform_data(schedule_df, scrapy_df)
    # LOAD :: DATAFRAME -> SCHEMAS MONGODB
    load_data(spark, joined_df, championship)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()


def extract_data(spark, championship: str, match_date: str):
    """Load data from MongoDB .
        :param match_date:
        :param spark: Spark session object.
        :param championship: Championship-Collection type parameter.
        :return: Spark DataFrame.
    """

    # (1) - EXTRACT FIRST SCHEMA  -> OPERATION_DATA_DB :  <CHAMPIONSHIP_>_sche - SCHEDULES MATCH
    collection_name = championship + MatchConstants.DOMAIN_OPERATION_DATA_SCHEDULE

    pipeline = [
        {"$match": {"match_date": {"$eq": match_date}}}
    ]
    operation = OperationImplDAO(spark, os.getenv("DB_NAME_READ_1"), MatchConstants.SPARK_READ_DB, collection_name)
    schedule_df = operation.get_collection(pipeline)

    print("<<< EXTRACT schedule_df >>>>")
    schedule_df.show()

    # (2) - EXTRACT SECOND SCHEMA -> SCRAPY_DB : <CHAMPIONSHIP_>_chmp - SCRAPY CHAMPIONSHIP
    collection_name = championship + MatchConstants.DOMAIN_SCRAPY_CHAMPIONSHIP

    operation = OperationImplDAO(spark, os.getenv("DB_NAME_READ_2"), MatchConstants.SPARK_READ_DB, collection_name)
    scrapy_df = operation.get_collection(None)

    print("<<< EXTRACT  scrapy_df >>>>")
    scrapy_df.show()

    return schedule_df, scrapy_df


def transform_data(schedule_df, scrapy_df):
    """Transform original dataset.
    :param schedule_df: Input DataFrame.
    :param scrapy_df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    # join_df = schedule_df.join(scrapy_df,(schedule_df.teamA == scrapy_df.team_A) & (schedule_df.teamB == scrapy_df.team_B),"inner")
    join_condition = scrapy_df['teamA'] == schedule_df['team_A']
    joined_df = scrapy_df.join(schedule_df, join_condition, 'leftsemi')

    print("<<< transform_data join >>>>")
    joined_df.show()
    return joined_df


def load_data(spark, join_dt, championship: str):
    # (1) - LOAD DATAFRAME TRANSFORM -> <CHAMPIONSHIP>_mtch
    collection_name = championship + MatchConstants.DOMAIN_OPERATION_DATA_MATCH

    operation = OperationImplDAO(spark, os.getenv("DB_NAME_WRITE_1"), MatchConstants.SPARK_WRITE_DB, collection_name)
    operation.save_collection(join_dt)
