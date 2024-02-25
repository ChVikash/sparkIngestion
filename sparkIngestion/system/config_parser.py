
import os 
import json 
from typing import Optional, Dict, List, Tuple
from sparkIngestion.system.custom_exceptions import NoConfigFoundError 


def parse_config(src_system: str, schema_name: str, table_name: str) -> dict:
    """
    This function parses the json config and returns it as a dictionary.
    @Args:
        -src_system(str) : The name of the source system 
        -schem_name(str) : The name of the schema to be read 
        -table_name(str) : The name of the table for which we have to read config
    @Returns:
        -dict_config(dict) : JSON Config parsed as a dictionary
    @Raises: 
        -NoConfigFoundError : When souce config is missing or either 
                                table/database name is incorrect
    """
    config_base_path = "Personal\etlConfigs\source_config"
    file_name = f'{table_name}.json' 
    config_path = os.path.join(config_base_path, src_system, schema_name, file_name)
    if not os.path.exists(config_path):
        raise NoConfigFoundError()
    with open(config_path) as F :
        dict_config = json.loads(F.read())
    return dict_config
