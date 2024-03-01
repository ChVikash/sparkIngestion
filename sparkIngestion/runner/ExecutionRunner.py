from dataclasses import dataclass
from typing import Callable, List, Union

from sparkIngestion.core.SparkIngestion import SparkIngestionR2B

Queue = []

load_func_map = {
    "parquet" : SparkIngestionR2B.get_latest_files,
    "delta" : SparkIngestionR2B.get_latest_dataframe
}

function_map ={
    "extract_dataframe" : SparkIngestionR2B.extract_dataframe ,
    "read_max_file" : SparkIngestionR2B.read_max_file,
    "read_all_files_after_checkpoint" : SparkIngestionR2B.read_all_files_after_checkpoint,
    "return_src_dataframe" : SparkIngestionR2B.return_src_dataframe
}

@dataclass 
class ExecutionRunner(object):
    
    @classmethod
    def create_execution_extracts(params):
        src_system = params["src_system"]
        src_database = params["src_database"]
        src_table = params["src_table"]
        src_format = params["src_format"]
        if src_format.lower() in ["parquet", "delta"]: 
            src_path = params["src_path"]
        
        
        ####enable real boolean here 
        if params["read_max"].lower() == "true":
            list_of_files = load_func_map(src_path)
        
        else: 
            list_of_files = load_func_map(src_path, src_table, params["checkpoint_info"])
        
        list_of_files.sort()
           
        ls_df = []
        for file in list_of_files:
            ls_df.append(function_map["extract_dataframe"](
                params["src_format"],
                file,
                params["read_options"]
            ))
            
        
        
        
        
        
        
            
            
        
        
        