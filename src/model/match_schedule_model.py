from pydantic import BaseModel, Field, constr, create_model
from _datetime import date, time


class MatchScheduleModel(BaseModel):
    championship: constr(strict=True) = Field(...)
    match_A: constr(strict=True) = Field(...)
    match_B: constr(strict=True) = Field(...)
    local_match: constr(strict=True) = Field(...)
    date_match: date | None = None
    time_match: str | None = None
    status: constr(strict=True) = Field(...)

    class config:
        championship = "championship"
        match_A = "match_A"
        match_B = "match_B"
        local_match = "local_match"
        date_match = "date_match"
        time_match = "time_match"
        status = "status"

    @classmethod
    def as_optional(cls):
        annonations = cls.__fields__
        OptionalModel = create_model(
            f"Optional{cls.__name__}",
            __base__=MatchScheduleModel,
            **{
                k: (v.annotation, None) for k, v in MatchScheduleModel.model_fields.items()
            })
        return OptionalModel

    def ResponseModel(data, message):
        return {
            "data": [data],
            "code": 200,
            "message": message,
        }

    def ErrorResponseModel(error, code, message):
        return {"error": error, "code": code, "message": message}

    @staticmethod
    def data_helper(matches) -> dict:
        return {
            "_id": str(matches['_id']),
            "championship": str(matches["championship"]),
            "match_A": str(matches["match_A"]),
            "match_B": str(matches["match_B"]),
            "local_match": str(matches["local_match"]),
            "date_match": matches["date_match"],
            "time_match": matches["time_match"],
            "status": matches["status"]
        }
