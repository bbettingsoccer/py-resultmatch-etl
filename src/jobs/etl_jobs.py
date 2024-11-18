from src.dependencies.config.enviroment_conf import env_check
from src.dependencies.spark.spark import start_spark
from src.service.match_schedule_service import MatchScheduleService
import asyncio
import time

async def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(app_name='my_etl_ricardo', files=None)

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = await extract_data(spark)
    # data_transformed = transform_data(data, config['steps_per_floor'])
    # load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()

    return None


async def extract_data(spark):
    # 1- EXTRACT MATCH_SCHEDULE FOR CHAMPIONSHIP
    match_schedule = MatchScheduleService("LaLiga_chp")
    matchesL = await match_schedule.getMatchScheduleByDateCurrent()

    print("RESULT ", matchesL)


async def transform_data(df, steps_per_floor_):
    pass


async def load_data(df):
    pass


# entry point for PySpark ETL application
if __name__ == '__main__':
    env_check()
    asyncio.run(main())
