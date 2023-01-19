# Declare a variable to hold the script name.
JOBNAME="copy_files_local_to_hdfs.sh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define a Log File where lpgs would be generated
LOGFILE="${HOME}/medicare-prescriber-etl-pipeline/logs/copy_files_local_to_hdfs_${date}.log"

### COMMENTS: From this point on, all standard output and standard error will be logged in the log file.
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"
LOCAL_STAGING_PATH="${HOME}/medicare-prescriber-etl-pipeline/staging"
LOCAL_CITY_DIR=${LOCAL_STAGING_PATH}/city_dimension
LOCAL_PRESC_DIR=${LOCAL_STAGING_PATH}/prescriber_fact

HDFS_STAGING_PATH="prescriber_pipeline/staging"
HDFS_CITY_DIR=${HDFS_STAGING_PATH}/city_dimension
HDFS_PRESC_DIR=${HDFS_STAGING_PATH}/prescriber_fact

### Copy the City  and Fact file to HDFS
hdfs dfs -put -f ${LOCAL_CITY_DIR}/* ${HDFS_CITY_DIR}/
hdfs dfs -put -f ${LOCAL_PRESC_DIR}/* ${HDFS_PRESC_DIR}/
echo "${JOBNAME} is Completed...: $(date)"
} > ${LOGFILE} 2>&1  # <--- End of program and end of log.



