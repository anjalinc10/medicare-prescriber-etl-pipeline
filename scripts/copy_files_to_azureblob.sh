# Declare a variable to hold the script name.
JOBNAME="copy_files_to_azureblob.sh"
# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Log fil path
LOGFILE="${HOME}/medicare-prescriber-etl-pipeline/logs/${JOBNAME}_${date}.log"

bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

### COMMENTS: From this point on, all standard output and standard error will be logged in the log file.
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

### Local Directories
LOCAL_OUTPUT_PATH="${HOME}/medicare-prescriber-etl-pipeline/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/city_dimension
LOCAL_PRESC_DIR=${LOCAL_OUTPUT_PATH}/prescriber_fact

### SAS URLs
citySasUrl="https://projectfiles01.blob.core.windows.net/city-dimension/${bucket_subdir_name}?st=2023-01-19T19:17:32Z&se=2023-01-31T03:17:32Z&si=writeAccess&spr=https&sv=2021-06-08&sr=c&sig=0Y3YSbsjrqMC9SGOTuj34gKLXEoYg9cA40hqsbTecTU%3D"
prescSasUrl="https://projectfiles01.blob.core.windows.net/prescriber-fact/${bucket_subdir_name}?st=2023-01-19T19:19:40Z&se=2023-01-31T03:19:40Z&si=writeAccess&spr=https&sv=2021-06-08&sr=c&sig=V7GesTN1Z7bhv4Z1Z5EOXORxT4x0kLn9OqT2yW732zw%3D"

### Push City  and Fact files to Azure.
azcopy copy "${LOCAL_CITY_DIR}/*" "$citySasUrl"
azcopy copy "${LOCAL_PRESC_DIR}/*" "$prescSasUrl"

echo "The ${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.