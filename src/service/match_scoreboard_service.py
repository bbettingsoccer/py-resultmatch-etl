from json_encoder.json import json_encoder

from src.commons.match_constants import MatchConstants
from src.dependencies.mongodb.dao.operationimpl_dao import OperationImplDAO
from src.model.match_scoreboard_model import MatchScoreboardModel


class MatchScoreboardService:

    def __init__(self, collection: str):
        self.collection = OperationImplDAO(collection, MatchConstants.MONGO_SCHEMA_MATCH_CHAMPIONSHIP)

    async def getMatchesByMatch(self, search: str, values: []):
        print("[MatchScoreboardService][getMatchesByMatch] - START")

        currentsL = []
        filter = []

        filter = {"$and": [{MatchScoreboardModel.config.match_A: {"$eq": values[0]}},
                           {MatchScoreboardModel.config.match_B: {"$eq": values[1]}},
                           {MatchScoreboardModel.config.date_match: {"$eq": values[2]}}]}
        try:
            currents = await self.collection.find_condition(filter)
            if currents:
                for currented in currents:
                    currentsL.append(MatchScoreboardModel.data_helper(currented))
                return currentsL
        except Exception as e:
            print("[MatchScoreboardService][getMatchesByMatch] - ERROR :: ", filter, e)
            return None

    async def save(self, data: MatchScoreboardModel):
        print("[MatchScoreboardService][save] - START")
        try:
            jsonObj = json_encoder(data)
            await self.collection.save(jsonObj)
            return True
        except Exception as e:
            print("[MatchScoreboardService][save] - ERROR :: ", e)
            raise

    async def update(self, id, data: MatchScoreboardModel):
        print("[MatchScoreboardService][Update] - START")
        try:
            await self.collection.update_one(id, data)
            return True
        except Exception as e:
            print("[MatchScoreboardService][Update] - ERROR :: ", e)
            return False