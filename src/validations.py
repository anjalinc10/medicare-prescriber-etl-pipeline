import logging
import pandas

logger = logging.getLogger(__name__)


def validate_spark_session_object(spark):
    try:
        logger.info("Validating SparkSession object by printing current date using SparkSession Object")
        date_df = spark.sql("select current_date()")
        logger.info(f"Current date is : {date_df.collect()}")
    except NameError as exp:
        logger.error(
            f"NameError in the method - validate_spark_session_object(). Please check the Stack Trace. ", exc_info=True)
        raise
    except Exception as exp:
        logger.error(f"Error in the method - validate_spark_session_object(). Please check the Stack Trace. ", exc_info=True)
        raise
    else:
        logging.info("SparkSession object is validated successfully and ready to use\n\n")


def validate_df_by_count(df, dfName):
    try:
        logger.info(f"The DataFrame Validation by count is started for Dataframe {dfName}...")
        df_count = df.count()
        logger.info(f"The DataFrame count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the method - validate_df_by_count(). Please check the Stack Trace. ", exc_info=True)
        raise
    else:
        logger.info(f"The DataFrame Validation by count is completed.\n\n")


def validate_df_by_top10_records(df, dfName):
    try:
        logger.info(f"The DataFrame Validation by top-10 record is started for DataFrame {dfName}...")
        logger.info(f"The DataFrame top-10 records are:.")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False, justify="left"))
    except Exception as exp:
        logger.error("Error in the method - validate_df_by_top10_records(). Please check the Stack Trace. ", exc_info=True)
        raise
    else:
        logger.info("The DataFrame Validation by top-10 record is completed.\n\n")


def validate_df_by_schema(df, dfName):
    try:
        logger.info(f"The DataFrame Schema Validation started for DataFrame {dfName}...")
        schema = df.schema.fields
        logger.info(f"The DataFrame {dfName} schema is: ")
        for i in schema:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method - validate_df_by_schema(). Please check the Stack Trace. ", exc_info=True)
        raise
    else:
        logger.info("The DataFrame Schema Validation is completed.\n\n")
