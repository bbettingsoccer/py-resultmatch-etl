from datetime import date
from src.commons.match_constants import MatchConstants
from src.dependencies.mongodb.dao.operationimpl_dao import OperationImplDAO
from src.model.match_schedule_model import MatchScheduleModel


class MatchScheduleService:

    def __init__(self, collection: str):
        self.collection = OperationImplDAO(collection, MatchConstants.MONGO_SCHEMA_MATCH_CHAMPIONSHIP)

    async def getMatchScheduleByDateCurrent(self):
        print("[MatchScheduleService][getMatchScheduleByDatetime] - START")
        values = str(date.today())
        currentsL = []
        filter = {MatchScheduleModel.config.date_match: {"$eq": values}}
        try:
            currents = await self.collection.find_condition(filter)
            if currents:
                for currented in currents:
                    print(" ------------ currented ---------", currented)
                    currentsL.append(MatchScheduleModel.data_helper(currented))
                return currentsL
        except Exception as e:
            print("[MatchScheduleService][getMatchScheduleByDatetime] - ERROR :: ", filter, e)
            return None