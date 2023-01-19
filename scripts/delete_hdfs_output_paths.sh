# Declare a variable to hold the script name.
JOBNAME="delete_hdfs_output_paths.sh"
# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Define a log file where logs would be generated
LOGFILE="${HOME}/medicare-prescriber-etl-pipeline/logs/${JOBNAME}_${date}.log"

### COMMENTS: From this point on, all standard output and standard error will be logged in the log file.
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

CITY_OUT_PATH="prescriber_pipeline/output/city_dimension"
hdfs dfs -test -d $CITY_OUT_PATH
status=$?
if [ $status -eq 0 ]
then
  printf "The HDFS output directory $CITY_OUT_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $CITY_OUT_PATH
  printf "The HDFS Output directory $CITY_OUT_PATH is deleted before extraction.\n\n"
fi

PRESC_OUT_PATH="prescriber_pipeline/output/prescriber_fact"
hdfs dfs -test -d $PRESC_OUT_PATH
status=$?
if [ $status -eq 0 ]
then
  printf "The HDFS output directory $PRESC_OUT_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $PRESC_OUT_PATH
  printf "The HDFS Output directory $PRESC_OUT_PATH is deleted before extraction.\n\n"
fi

HIVE_CITY_PATH="/user/hive/warehouse/prescriber.db/df_city_final"
hdfs dfs -test -d $HIVE_CITY_PATH
status=$?
if [ $status -eq 0 ]
  then
  printf "The HDFS output directory $HIVE_CITY_PATH is available. Deleting it.."
  hdfs dfs -rm -r -f $HIVE_CITY_PATH
  printf "The HDFS output directory $HIVE_CITY_PATH is deleted for Hive.\n\n "
fi

HIVE_PRESC_PATH="/user/hive/warehouse/prescriber.db/df_presc_final"
hdfs dfs -test -d $HIVE_PRESC_PATH
status=$?
if [ $status -eq 0 ]
  then
  printf "The HDFS output directory $HIVE_PRESC_PATH is available. Deleting it."
  hdfs dfs -rm -r -f $HIVE_PRESC_PATH
  printf "The HDFS Output directory $HIVE_PRESC_PATH is deleted for Hive.\n\n"
fi

echo "${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
