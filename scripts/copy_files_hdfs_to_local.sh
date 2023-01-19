# Declare a variable to hold the script name.
JOBNAME="copy_files_hdfs_to_local.sh"

# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define a Log File where logs would be generated
LOGFILE="${HOME}/medicare-prescriber-etl-pipeline/logs/${JOBNAME}_${date}.log"

### COMMENTS: From this point on, all standard output and standard error will be logged in the log file.

{  # <--- Start of the log file.
printf "${JOBNAME} Started...: $(date)\n"
LOCAL_OUTPUT_PATH="${HOME}/medicare-prescriber-etl-pipeline/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/city_dimension
LOCAL_PRESC_DIR=${LOCAL_OUTPUT_PATH}/prescriber_fact

HDFS_OUTPUT_PATH=prescriber_pipeline/output
HDFS_CITY_DIR=${HDFS_OUTPUT_PATH}/city_dimension
HDFS_PRESC_DIR=${HDFS_OUTPUT_PATH}/prescriber_fact

### Delete the existing files from local output path
rm -f ${LOCAL_CITY_DIR}/*
rm -f ${LOCAL_PRESC_DIR}/*

### Copy the City  and Fact file from HDFS to Local
hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
hdfs dfs -get -f ${HDFS_PRESC_DIR}/* ${LOCAL_PRESC_DIR}/
printf "${JOBNAME} is Completed...: $(date)\n"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
