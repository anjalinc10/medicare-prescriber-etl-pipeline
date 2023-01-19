import logging

# get the custom logger
logger = logging.getLogger(__name__)


def extract_files(df, format, filePath, split_no, headerReq, compressionType):
    try:
        logger.info(f"Started executing extract_files() function...")
        df.coalesce(split_no) \
          .write \
          .format(format) \
          .save(filePath, header=headerReq, compression=compressionType)
    except Exception as exp:
        logger.error("Error in the method - extract_files(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("DataFrame is saved to path - {filePath} with format - {format}")
        logger.info("Finished executing extract_files() function... \n\n")
