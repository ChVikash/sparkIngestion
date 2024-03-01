import os  
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from typing import Optional, Dict, List, Tuple
from sparkIngestion.core.BaseIngestion import BaseIngestion

from sparkIngestion.system.custom_exceptions import ColumnAddedException,\
                                       ColumnDroppedException,\
                                       ColumnMismatchException, \
                                       DbNotFoundException, \
                                       IllegalTablePathException
                                    

spark = SparkSession.builder.getOrCreate()

class SparkIngestionR2B(BaseIngestion):
    """
    This class contains methods related to Ingestion. All the methods present 
    in this class are decorated with @classemethod decorator. Hence, all these methods can be
    used through the class object itself. 

    -code_block::python
        >>from path-to-file.pre_processing improt SparkIngestionR2B
        >>
    """
    
    @classmethod
    def get_latest_dataframe(cls, src_path:str )-> str :
        """
        This function reads the latest files that loaded after the latest loaded file found in ingestion logs for sap bw hana.  
        @Args : 
            -table_name(str) : table name as a string

        @Returns: 
            -file_path(str): most recent file's path that was loaded most recently. 
        """
        file_path = dbutils.fs.ls(src_path)
        if not file_path:
            raise IllegalTablePathException
        file_path = [src_path]
        return file_path
    
    
    @classmethod
    def get_latest_files(cls, src_path:str )-> str :
        """
        This function reads the latest files that loaded after the latest loaded file found in ingestion logs for sap bw hana.  
        @Args : 
            -table_name(str) : table name as a string

        @Returns: 
            -file_path(str): most recent file's path that was loaded most recently. 
        """
        files = dbutils.fs.ls(src_path)
        file_path = [max([file_path.path for file_path in files])]
        
        if not file_path:
            raise IllegalTablePathException
        return file_path
    
    
    @classmethod
    def get_latest_files(cls, src_path: str , table_name: str, checkpoint_info: dict )-> List[str] :
        """
        This function reads the latest files that loaded after the latest loaded file found in ingestion logs for sap bw hana.  
        @Args : 
            -src_path(str) : path of the source
            -table_name(str) : table name as a string

        @Returns: 
            -files(List[str]): sorted list of file path(s) of the files that came after the previous loaded files. 
        """
        metadata_catalog = checkpoint_info["catalog"]
        metadata_schema = checkpoint_info["schema"]
        metadata_table = checkpoint_info["table"]
        metadata_file_field = checkpoint_info["file_field"]
        metadata_dbr_table_field = checkpoint_info["table_field"]
        files = dbutils.fs.ls(src_path)
        mpf = spark.sql(f'''
                        SELECT 
                            max({metadata_file_field}) as mpf
                        FROM 
                            {metadata_catalog}.{metadata_schema}.{metadata_table}
                        WHERE 
                            {metadata_dbr_table_field} =  "{table_name.upper()}" 
                        ''')
        mpf = mpf.filter(mpf["mpf"].isNotNull())
        if mpf.count() > 0 :
            mpf.display()
            mpf = mpf.collect()[0]['mpf']
        else : 
            mpf = f"{table_name.lower()}_2023-01-01T01:01:01.1644818Z"
        files = [file_path.path for file_path in files if file_path.name > mpf ]
        return files 
    
    
    # There can be parsed json objects and load params and extract params are passed separately       
    @classmethod
    def extract_dataframe(cls,
                          src_format: str, 
                          src_path: str,
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
        df = spark.read.format(src_format)
        for key,value in params["options"].items() : 
            df = df.option(key, value)    
        df = df.load(src_path)
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
        #TO-BE IMPLEMENTED 
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
                    .filter(F.col('databaseName') == f"{schema}").count()
        
        if db_exists == 0:
            raise DbNotFoundException(catalog)
            # cls.create_database(env,db_name.lower())

        # checking if table already exists, If it does the schema of input dataframe is matched
        # with existing else it uses new df schema to create a new table  
        # however if OverwriteSchema is given it skips schema checks 
        if db_name == "default" : 
            schema_name = "default"
        else :
            schema_name = f"{schema}"
        table_exists = spark.sql(f'''show tables 
                                     from 
                                     `{catalog}`.`{schema_name}`''')\
                                .filter(F.col('tableName') == f'{db_name}_{tb_name}').count()
        
        
        if table_exists == 0 :
            cls.create_table(env, catalog, db_name, tb_name, schema)
        df = spark.write.format(format)
        df = df.mode(mode)
        df.saveAsTable(f"`{catalog}`.`{schema_name}`.{db_name}_{tb_name}")
        return 