import os
from dotenv import load_dotenv, find_dotenv
import json

from src.config.match_constants import MatchConstants
from src.dto.info_connection_db_dto import InfoConnectionDatabaseDTO
from pathlib import Path


def get_path_file(folder1: str, folder2: str, file: str) -> Path:
    root_dir = Path(__file__).parent.parent
    if folder2 is None:
        path_complete = os.path.join(root_dir, folder1, file)
    else:
        path_complete = os.path.join(root_dir, folder1, folder2, file)
    return path_complete


def env_check():
    env_file = None

    if os.environ['ENVIRONMENT_TYPE'] == 'DEV':
        path_file = get_path_file(folder1="env", folder2=None, file="dev.env")
        print("ENV path_file ", path_file)
        env_file = find_dotenv(path_file)
        load_dotenv(env_file)

    elif os.environ['ENVIRONMENT_TYPE'] == 'PRO':
        path_file = get_path_file(folder1="env", folder2=None, file="pro.env")
        env_file = find_dotenv(path_file)
        load_dotenv(env_file)
    load_dotenv(env_file)


def set_spark_config_environment() -> dict:
    data_dict = None
    path_file = get_path_file(folder1="env", folder2="config", file="spark_config_env.json")

    with open(path_file, 'r') as f:
        json_string = f.read()
        if len(json_string) > 0:
            data_dict = json.loads(json_string)
    return data_dict


def set_spark_config_database() -> dict:
    data_dict_comp = {}
    template_data = None
    item_read = None
    item_write = None
    path_file = get_path_file(folder1="env", folder2="config", file="spark_config_db.json")

    with open(path_file, 'r') as f:
        json_string = f.read()
        if len(json_string) > 0:
            data = json.loads(json_string)
            connection_type = data[MatchConstants.SPARK_TYPE_DB]
            # TEMPLATE DATABASE
            match connection_type:
                case MatchConstants.DB_MONGODB:
                    path_file = get_path_file(folder1="env", folder2="config", file="mongo_template.json")
                    with open(path_file, 'r') as f:
                        json_template = f.read()
                        template_data = json.loads(json_template)
                case MatchConstants.DB_ORACLE:
                    pass
            for key, val in data.items():
                if key == MatchConstants.SPARK_READ_DB:
                    for v in val:
                        if item_read is None:
                            item_read = os.getenv('DB_URL') + v
                        else:
                            item_read = item_read + "," + os.getenv('DB_URL') + v
                    data_dict_comp[template_data[MatchConstants.SPARK_READ_DB]] = item_read
                elif key == MatchConstants.SPARK_WRITE_DB:
                    for v in val:
                        if item_write is None:
                            item_write = os.getenv('DB_URL') + v
                        else:
                            item_write = item_write + "," + os.getenv('DB_URL') + v
                    data_dict_comp[template_data[MatchConstants.SPARK_WRITE_DB]] = item_write
                elif key == MatchConstants.SPARK_JARS_DB:
                    data_dict_comp[template_data[MatchConstants.SPARK_JARS_DB]] = val
    return data_dict_comp


def get_database_conf(db_name: str, type_operation: str, entity_name: str) -> InfoConnectionDatabaseDTO:
    acronym_v = []
    connection_v = []
    db_con_v = []
    connection_db_dto = None
    path_file = get_path_file(folder1="env", folder2="config", file="spark_config_db.json")
    with open(path_file, 'r') as f:
        json_string = f.read()
        data = json.loads(json_string)
        for key, val in data.items():
            if key == MatchConstants.ACRONYM_DB:
                for key, val in val.items():
                    acronym_d = {"acronym": key, "db_name": val}
                    acronym_v.append(acronym_d)
            if key == type_operation:
                for v in val:
                    connection = os.getenv("DB_URL") + v
                    connection_d = {"db_name": v, "db_connection": connection}
                    connection_v.append(connection_d)
        for ac in acronym_v:
            for cn in connection_v:
                if ac["db_name"] == cn["db_name"]:
                    db_connection_entity = cn["db_connection"] + "." + entity_name
                    db_con_d = {"acronym": ac["acronym"], "db_name": ac["db_name"],
                                "db_connection": db_connection_entity}
                    db_con_v.append(db_con_d)

        for db in db_con_v:
            if db["db_name"] == db_name:
                connection_db_dto = InfoConnectionDatabaseDTO(acronym=db["acronym"],
                                                              db_name=db["db_name"],
                                                              db_connection=db["db_connection"],
                                                              entity=entity_name)
    return connection_db_dto
