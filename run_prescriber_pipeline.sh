PROJECT_HOME="${HOME}/medicare-prescriber-etl-pipeline"

### Copy the input vendor data files from Local to HDFS.
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Calling copy_files_local_to_hdfs.sh ... \n\n"
${PROJECT_HOME}/scripts/copy_files_local_to_hdfs.sh
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Execution of copy_files_local_to_hdfs.sh is completed. \n\n"

### Delete HDFS output paths(if exists) required by prescriber data pipeline.
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Calling delete_hdfs_output_paths.sh to cleanup required output paths before running pipeline...\n"
${PROJECT_HOME}/scripts/delete_hdfs_output_paths.sh
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Execution of delete_hdfs_output_paths.sh is completed. \n\n"

### Call below Spark Job to extract transform and load prescriber data as per business logic
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Calling run_prescriber_pipeline.py ...\n"
spark3-submit --master yarn --jars ${PROJECT_HOME}/lib/postgresql-42.3.5.jar ${PROJECT_HOME}/prescriber_pipeline_main.py
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Execution of run_prescriber_pipeline.py is completed. \n\n"

### Copy processed data files from HDFS to local.
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Calling copy_files_hdfs_to_local.sh to copy processed data files from HDFS to local filesystem...\n"
${PROJECT_HOME}/scripts/copy_files_hdfs_to_local.sh
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Execution of copy_files_hdfs_to_local.sh is completed. \n\n"

### Upload processed data files to Azure Blob storage.
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Calling copy_files_to_azureblob.sh to upload processed data files to azure blob storage...\n"
${PROJECT_HOME}/scripts/copy_files_to_azureblob.sh
printf "`date +"%d/%m/%Y_%H:%M:%S"` - Execution of copy_files_to_azureblob.sh is completed. \n\n"
