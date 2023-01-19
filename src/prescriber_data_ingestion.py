import logging

# Get the custom Logger from Configuration File
logger = logging.getLogger(__name__)


def load_data_file(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.info(f"Started load_data_file() function...")
        if file_format == 'parquet':
            df = spark. \
                read. \
                format(file_format). \
                load(file_dir)
        elif file_format == 'csv':
            df = spark. \
                read. \
                format(file_format). \
                options(header=header). \
                options(inferSchema=inferSchema). \
                load(file_dir)
    except Exception as exp:
        logger.error(f"Error in the method - load_data_file(). Please check the Stack Trace. ", exc_info=True)
        raise
    else:
        logger.info(f"The input File {file_dir} is loaded to the DataFrame.")
        logger.info("Completed load_data_file() function. \n\n")
    return df
