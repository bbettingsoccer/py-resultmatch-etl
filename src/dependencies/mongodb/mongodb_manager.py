import os
import motor.motor_asyncio
import asyncio

from src.commons.match_constants import MatchConstants


class MongoManager:
    __instance = None

    def __init__(self, mongo_schema: str):
        if MongoManager.__instance != None:
            print("This class is a singleton")
            raise Exception("This class is a singleton!")
        else:
            MongoManager.__instance = self.getConnection(mongo_schema)

    @staticmethod
    def getInstance(mongo_schema: str):
        if MongoManager.__instance is None:
            MongoManager(mongo_schema)
        return MongoManager.__instance

    def getConnection(self, mongo_schema: str):

        connURL = os.environ["DB_URL"]
        DB_NAME = None
        print(" mongo_schema -- ", mongo_schema)
        match mongo_schema:
            case MatchConstants.MONGO_SCHEMA_SCRAPY:
                DB_NAME = os.environ["DB_NAME_SCRAPY"]
            case MatchConstants.MONGO_SCHEMA_MATCH_CHAMPIONSHIP:
                DB_NAME = os.environ["DB_NAME_MATCH_CHAMPIONSHIP"]

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            client = motor.motor_asyncio.AsyncIOMotorClient(connURL, serverSelectionTimeoutMS=5000, io_loop=loop)
            client.get_io_loop = asyncio.get_running_loop
            database = client[DB_NAME]
            return database
        except Exception:
            print("Unable to connect to the server.")
            return None

