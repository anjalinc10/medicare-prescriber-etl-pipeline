import datetime as date
from pyspark.sql.functions import lit

import logging

logger = logging.getLogger(__name__)


def data_persist_hive(spark, df, dfName, partitionBy, mode):
    try:
        logger.info(f"Started execution of data_persist_hive() function for saving dataframe "+ dfName +" into Hive Table...")
        # Add a Static column with Current Date
        df= df.withColumn("delivery_date", lit(date.datetime.now().strftime("%Y-%m-%d")))
        spark.sql(""" create database if not exists prescriber location 'hdfs://localhost:9000/user/hive/warehouse/prescriber.db' """)
        spark.sql(""" use prescriber """)
        df.write.saveAsTable(dfName,partitionBy=partitionBy, mode = mode)
    except Exception as exp:
        logger.error("Error in the method - data_persist_hive(). Please check the Stack Trace. " , exc_info=True)
        raise
    else:
        logger.info("Finished execution of data_persist_hive() function. Dataframe "+ dfName +" has been saved as Hive Table... \n\n")


def data_persist_postgre(spark, df, dfName, url, driver, dbtable, mode, user, password):
    try:
        logger.info(f"Started execution of data_persist_postgre() function for saving dataframe "+ dfName + " into Postgre Table...")
        df.write.format("jdbc")\
                .option("url", url) \
                .option("driver", driver) \
                .option("dbtable", dbtable) \
                .mode(mode) \
                .option("user", user) \
                .option("password", password) \
                .save()
    except Exception as exp:
        logger.error("Error in the method - data_persist_postgre(). Please check the Stack Trace. " , exc_info=True)
        raise
    else:
        logger.info("Finished execution of data_persist_postgre() function. DataFrame "+ dfName +" has been saved into Postgre Table... \n\n")