from dataclasses import dataclass


@dataclass
class InfoConnectionDatabaseDTO:
    acronym: str
    db_name: str
    db_connection: str
    entity: str

    def __init__(self, acronym, db_name, db_connection, entity):
        super().__init__()
        self.acronym = acronym
        self.db_name = db_name
        self.db_connection = db_connection
        self.entity = entity
