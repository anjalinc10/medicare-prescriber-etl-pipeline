import sys
import os
import logging
import time
from subprocess import Popen, PIPE

import src.config as cfg
from src.create_objects import get_spark_session
from src.validations import validate_spark_session_object, validate_df_by_count, validate_df_by_top10_records, validate_df_by_schema
from src.prescriber_data_ingestion import load_data_file
from src.prescriber_data_preprocessing import clean_city_data, clean_prescriber_data
from src.prescriber_data_transformation import get_city_reporting_data, get_top5_prescriber_by_state
from src.prescriber_data_extraction import extract_files
from src.prescriber_data_persist import data_persist_hive, data_persist_postgre

filename = f"logs/prescriber_pipeline_{time.strftime('%d%m%Y-%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
    handlers=[
        logging.FileHandler(filename=filename, mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)


def main():
    try:
        logging.info("======================================================================")
        logging.info("Prescriber DataPipeline execution started")
        # Get Spark Object
        spark = get_spark_session(cfg.exec_env, cfg.appName)

        # Validate Spark Object
        # Set up Error Handling Mechanism
        # Set up Logging Configuration Mechanism
        validate_spark_session_object(spark)

        # --- Initiate Data Ingestion ---
        # Load the City Data File
        file_dir=cfg.staging_city
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=cfg.header
           inferSchema=cfg.inferSchema

        df_city = load_data_file(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                                 inferSchema=inferSchema)

        # Load the Prescriber Fact Data File
        file_dir=cfg.staging_presc
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=cfg.header
           inferSchema=cfg.inferSchema
        
        df_prescriber_fact = load_data_file(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                                            inferSchema=inferSchema)

        # Validate city Dimension & Prescriber Fact dataframes
        validate_df_by_count(df_city, 'df_city')
        validate_df_by_top10_records(df_city, 'df_city')

        validate_df_by_count(df_prescriber_fact, 'df_prescriber_fact')
        validate_df_by_top10_records(df_prescriber_fact, 'df_prescriber_fact')

        # --- Initiate Data Preprocessing ---
        # Perform data cleaning operations
        #   1. Clean city data
        df_city_clean = clean_city_data(df_city)
        #   2. Clean prescriber data
        df_prescriber_clean = clean_prescriber_data(df_prescriber_fact)
        #   3. Validate
        validate_df_by_top10_records(df_city_clean, 'df_city_clean')
        validate_df_by_top10_records(df_prescriber_clean, 'df_prescriber_clean')
        validate_df_by_schema(df_prescriber_clean, 'df_prescriber_clean')

        # --- Initiate Data Transformations ---
        # Apply all the transformations as per business logic
        df_city_final = get_city_reporting_data(df_city_clean, df_prescriber_clean)
        df_presc_final = get_top5_prescriber_by_state(df_prescriber_clean)

        # Validate final DataFrames after transformation
        validate_df_by_top10_records(df_city_final, 'df_city_final')
        validate_df_by_schema(df_city_final, 'df_city_final')
        validate_df_by_top10_records(df_presc_final, 'df_presc_final')
        validate_df_by_schema(df_presc_final, 'df_presc_final')

        # -- Initiate Data Extraction --
        CITY_PATH=cfg.output_city
        extract_files(df_city_final,'json',CITY_PATH,1,False,'bzip2')
     
        PRESC_PATH=cfg.output_presc
        extract_files(df_presc_final,'orc',PRESC_PATH,2,False,'snappy')

        ### -- Initiate Data Persist --
        # Persist data to Hive
        data_persist_hive(spark=spark, df=df_city_final , dfName='df_city_final' , partitionBy='delivery_date', mode='append')        
        data_persist_hive(spark=spark, df=df_presc_final, dfName='df_presc_final', partitionBy='delivery_date', mode='append')

        # Persist data at Postgre
        data_persist_postgre(spark=spark, df=df_city_final, dfName='df_city_final', url="jdbc:postgresql://localhost:6432/prescriber", 
                             driver="org.postgresql.Driver", dbtable='df_city_final', mode="append", user=cfg.postgres_username, password=cfg.postgres_password)
        data_persist_postgre(spark=spark, df=df_presc_final, dfName='df_presc_final', url="jdbc:postgresql://localhost:6432/prescriber", 
                             driver="org.postgresql.Driver", dbtable='df_presc_final', mode="append", user=cfg.postgres_username, password=cfg.postgres_password)

        logging.info("Prescriber DataPipeline execution completed")
    except Exception as exp:
        logging.error(f"Error occurred in the main() method. Please check the Stack Trace to locate the respective module and fix it. {str(exp)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Call main function here
    main()
