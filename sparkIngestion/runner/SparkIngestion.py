import os  
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from typing import Optional, Dict, List, Tuple
from sparkIngestion.core.BaseIngestion import BaseIngestion

from sparkIngestion.system.custom_exceptions import ColumnAddedException,\
                                       ColumnDroppedException,\
                                       ColumnMismatchException, \
                                       DbNotFoundException
                                    

spark = SparkSession.builder.getOrCreate()

class SparkIngestion(BaseIngestion):
    """
    This class contains methods related to Ingestion. All the methods present 
    in this class are decorated with @classemethod decorator. Hence, all these methods can be
    used through the class object itself. 

    -code_block::python
        >>from path-to-file.pre_processing improt LWIngestion
        >>
    """
    # There can be parsed json objects and load params and extract params are passed separately       
    @classmethod
    def extract_dataframe(cls, 
                          database_name : str, 
                          table_name : str, 
                          params: Optional[Dict[str, str]] = {}
                          ) -> DataFrame:
        """
        This function returns the data present at the location given  as a spark dataframe.
        It assumes that we have the access to this location
        @Agrs: 
            -database_name(str) : The name of the database 
            -table_name(str) : The name of the table to be created 
            -params(Dict[str,str]) : it has the options that we want to enable on the dataframe
        
        @Returns: 
            -df(DataFrame) : spark.sql.DataFrame containing the data present at the given path
        
        @Raises:
            -Analysis Exception : If the path doesn't exist 
        """
        #by default delta format is considered 
        base_path = "abfss://raw@corporatelakehousedata.dfs.core.windows.net/saptableraw/"
        tbl_path = os.path.join(base_path, database_name, table_name)
        src_format = params.get("format", "delta")

        df = spark.read.format(src_format)

        for key,value in params["options"].items() : 
            df = df.option(key, value)
        df = spark.read.format(src_format)        
        df = df.load(tbl_path)
        return df
    
    @classmethod
    def create_table(cls,
                     env : str,
                     catalog : str, 
                     db_name: str, 
                     tb_name: str, 
                     schema: List[Tuple[str,str]]
                     )->int :
        """
        This function creates a table inside the given database of the cataloag 
        corporate-data-{env} if it's not already present in catalog yet. 

        @Args :
            -env(str) : string representing the environment 
            -catalog(str) : The name of the catalog
            -db_name(str) : The name of the database 
            -tb_name(str) : The name of the table to be created 
            -schema(List[Tuple[str,str]]) : list of column names and their data types 
        @Returns : 
            -count(int) : it return the count of the final dataframe  
        """
        schema_str = ""
        #creating a schema string that represents columns and their datatypes 
        for key,val in schema : 
            if  "/" in key : 
                schema_str += f"`{key}` {val} ,\n"
            else :
                schema_str += f"{key} {val} ,\n"
        schema_str = schema_str[:-2]
        schema_name = f"cd-sap-{db_name.lower()}-bronze"
        #creating a SQL string for table creation 
        sql_str = f"""CREATE TABLE IF NOT EXISTS 
                    `{schema_name}`.`{db_name}_{tb_name}` \n( {schema_str})"""
        #executing SQL strings 
        spark.sql(f"USE CATALOG `{catalog}`")
        spark.sql(sql_str)
        return

    @classmethod
    def match_existing_schema(cls, 
                              env : str,
                              catalog : str, 
                              db_name: str, 
                              tb_name: str, 
                              schema: List[Tuple[str,str]]
                              )->None :
        """
        This function matches the schema of the table present in DBR with the schema coming in from 
        source
        @Args:
            -env(str) : string representing the environment 
            -db_name(str) : The name of the database 
            -tb_name(str) : The name of the table to be created 
            -schema(List[Tuple[str,str]]) : list of column names and their data types 
        @Returns:
            -None
        @Raises:
            -ColumnDroppedException : Exception raised when a column is not present in source but was
                                      present in databricks table from previous loads 
            -ColumnAddedException   : Exception raised when a column is  present in source but was not
                                      present in databricks table from previous loads 
            -ColumnMismatchException: Exception raised when a column datatype present in source and 
                                      present in databricks table as well but wither datatype or column 
                                      name is different
        """
        spark.sql(f"USE CATALOG `{catalog}`") 
        cd_df = spark.sql(f"SELECT * FROM `{db_name}`.`{tb_name}`")
        cd_schema = cd_df.dtypes 
        if (len(cd_schema) < len(schema)):
            raise ColumnDroppedException 
        elif((len(cd_schema) > len(schema))):
            raise ColumnAddedException
        for col1, col2 in zip(cd_schema, schema):
            if col1[0] == col2[0] and col1[1] == col2[1] : 
                raise ColumnMismatchException(f"DBR_Col: {col1[0]}, SRC_Col: {col2[0]}")
        return None

    @classmethod
    def write_dataframe(cls, 
                        env: str, 
                        df: DataFrame, 
                        schema: List[Tuple[str,str]],
                        catalog : str ,
                        db_name: str, 
                        tb_name: str,
                        params: Optional[Dict[str, str]] = {}
                        ) ->None:
        """
        #This function take a spark dataframe and writes the dataframe as a Managed table in catalog.
        It checks whether the table was already present or not, If it is then it matches the schema 
        if the {"overwriteSchema" : "true"} is not present 
        
        @Args:
            -df(DataFrame) : A spark DataFrame that is to be written 
            -schema(List[Tuple[str,str]]) : list of column names and their data types
            -db_name(str) : The name of the database 
            -tb_name(str) : The name of the table to be created 
            -params(Optional[Dict[str, str]])  : it has the options that we want to enable on the dataframe
                                                 to be written 

        @Raises:
            -ColumnMismatchException
        """
        spark.sql(f"USE CATALOG `{catalog}`") 
        mode = params["meta"].get("mode", "Overwrite")
        format = params["meta"].get("format", "delta")

        #checking if database already present
        db_exists =spark\
                    .sql('show schemas')\
                    .filter(F.col('databaseName') == f"cd-sap-{db_name}-bronze").count()
        
        if db_exists == 0:
            raise DbNotFoundException(catalog)
            # cls.create_database(env,db_name.lower())

        # checking if table already exists, If it does the schema of input dataframe is matched
        # with existing else it uses new df schema to create a new table  
        # however if OverwriteSchema is given it skips schema checks 
        if db_name == "default" : 
            schema_name = "default"
        else :
            schema_name = f"cd-sap-{db_name}-bronze"
        table_exists = spark.sql(f'''show tables 
                                     from 
                                     `corporate-data-dev`.`{schema_name}`''')\
                                .filter(F.col('tableName') == f'{db_name}_{tb_name}').count()
        
        
        if table_exists == 0 :
            cls.create_table(env, catalog, db_name, tb_name, schema)
        df = spark.write.format(format)
        df = df.mode(mode)
        df.saveAsTable(f"`{catalog}`.`{schema_name}`.{db_name}_{tb_name}")
        return 