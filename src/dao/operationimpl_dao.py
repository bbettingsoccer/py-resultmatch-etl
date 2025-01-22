import os
from abc import ABC
from .operation_dao import OperationDAO
import json
from ..config.enviroment_conf import get_database_conf


class OperationImplDAO(OperationDAO, ABC):
    instance_collection = None

    def __init__(self, spark, connection_db, type_operation, entity_name):
        self.spark = spark
        self.connection = get_database_conf(db_name=connection_db,
                                            type_operation=type_operation,
                                            entity_name=entity_name)

    def get_collection(self, pipeline):
        schedule_df = None

        try:
            if pipeline is not None:
                schedule_df = (self.spark.read.format("mongo")
                               .option("uri", self.connection.db_connection)
                               .option("database", self.connection.db_name)
                               .option("collection", self.connection.entity)
                               .option("pipeline", json.dumps(pipeline))
                               .load())
            if pipeline is None:
                schedule_df = (self.spark.read.format("mongo")
                               .option("uri", self.connection.db_connection)
                               .option("database", self.connection.db_name)
                               .option("collection", self.connection.entity)
                               .load())
            return schedule_df
        except Exception as e:
            print("[Error :: DAO] - get_collection > ", e)
            return None


    def save_collection(self, join_dt):
        try:
            df = (join_dt.write.format("mongo")
                  .option("uri", self.connection.db_connection)
                  .option("database", self.connection.db_name)
                  .option("collection", self.connection.entity)
                  .option("replaceDocument", "false")
                  .mode("append")
                  .save())
        except Exception as e:
            print("[Error :: DAO] - save_collection > ", e)
            return None
