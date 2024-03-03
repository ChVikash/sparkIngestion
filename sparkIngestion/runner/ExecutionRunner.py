from dataclasses import dataclass
from typing import Callable, List, Union
from spark.sql import DataFrame
from pyspark.sql import SparkSession, DataFrame
from sparkIngestion.core.SparkIngestion import SparkIngestionR2B
from sparkIngestion.system.custom_exceptions import DbNotFoundException


load_func_map = {
    "parquet" : SparkIngestionR2B.get_latest_files,
    "delta" : SparkIngestionR2B.get_latest_dataframe
}

function_map ={
    "extract_dataframe" : SparkIngestionR2B.extract_dataframe ,
    "create_table" : SparkIngestionR2B.create_table,
    "match_existing_schema" : SparkIngestionR2B.match_existing_schema, 
    "write_dataframe" : SparkIngestionR2B.write_dataframe 
}


spark = SparkSession.builder.getOrCreate()


## IDEA : Executions context to be passed



@dataclass 
class ExecutionRunner(object):
    
    @classmethod
    def reader_exection(params: dict):
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
            ls_df.append((file,
                function_map["extract_dataframe"](
                params["src_format"],
                file,
                params["read_options"]
                )
            ))
    
    @classmethod
    def transformer_exection(ls_df: List[tuple, DataFrame]) -> List[DataFrame] :
        
        ###
        # TO BE implemeted the tranformation logic for R2B
        ###
        
        return ls_df
    
    
    @classmethod 
    def loader_execution(ls_df: List[tuple, DataFrame], params: dict):
        tgt_catalog = params["tgt_catalog"]
        tgt_schmea = params["tgt_schmea"]
        tgt_table = params["tgt_table"]
        
        db_exists =spark\
                    .sql('show schemas')\
                    .filter(F.col('databaseName') == f"{tgt_schmea}").count()
        
        if db_exists == 0:
            raise DbNotFoundException(tgt_catalog)
            # cls.create_database(env,db_name.lower())

        # checking if table already exists, If it does the schema of input dataframe is matched
        # with existing else it uses new df schema to create a new table  
        # however if OverwriteSchema is given it skips schema checks 
        table_exists = spark.sql(f'''show tables 
                                     from 
                                     `{tgt_catalog}`.`{tgt_schmea}`''')\
                                .filter(F.col('tableName') == f'{tgt_table}').count()
        
        dtypes = ls_df[0][1].dtypes
        if table_exists == 0 :
            function_map["create_table"](tgt_catalog, tgt_schmea, tgt_table, dtypes)
        
        
        if params["tgt_table_type"].lower() != "managed":
            ## implement external table function 
            raise NotImplementedError
        for file_name, df in ls_df :
            function_map["write_dataframe"](
                df,
                tgt_catalog,
                tgt_schmea,
                tgt_schmea,
                params["loads"]["load_params"]
            )
            
            ##write logs logic here
            
            
    
        
        
            
        
        
        
        
        
        
            
            
        
        
        