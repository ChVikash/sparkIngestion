from sparkIngestion.core.SparkIngestion import SparkIngestionR2B



load_func_map = {
    "parquet" : SparkIngestionR2B.get_latest_files,
    "delta" : SparkIngestionR2B.get_latest_dataframe
}


function_map ={
    "read_max_file" : SparkIngestionR2B.read_max_file,
    "read_all_files_after_checkpoint" : SparkIngestionR2B.read_all_files_after_checkpoint,
    "return_src_dataframe" : SparkIngestionR2B.return_src_dataframe
}