from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def get_spark_session(exec_env, app_name):
    try:
        logger.info("Creating SparkSession Object")
        if exec_env == "TEST":
            master = "local[*]"
        else:
            master = "yarn"
        spark = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .getOrCreate()
    except NameError as exp:
        logger.error(f"NameError in the method - get_spark_session(). Please check the Stack Trace. ", exc_info=True)
        raise
    except Exception as exp:
        logger.error(f"Error in the method - get_spark_session(). Please check the Stack Trace. ", exc_info=True)
        raise
    else:
        logger.info("SparkSession object is created successfully")
    return spark
