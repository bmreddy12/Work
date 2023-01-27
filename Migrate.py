from pyspark.sql.types import StringType
import concurrent.futures
n_config = {}
last_timestamp_dict = {}
destination_bucket_prefix_dict_prod = {}
number_of_files = {}
all_keys = []
env = 'prod'
source_bucket = ''
for action in last_timestamp_dict:
    total_files_log_path = "s3a://<bucket_name>/total_paths/action={}".format(action)
    success_files_log_path = "s3a://<bucket_name>/success_paths/action={}".format(action)
    failed_files_log_path = "s3a: //<bucket_name>/failed_paths/action={}".format(action)
    success_keys = []
    failed_keys = []
    files_to_migrate = []
    print(action)
    n_id = n_config[env][action]
    try:
        S3_resource = getCredentials(source_bucket, n_id)
    except Exception as e:
        print(str (e))
    destination_bucket_prefix=destination_bucket_prefix_dict_prod[action]
    for key in all_keys:
         if "timestamp" in key and key.endswith('.avro') and action in key:
             timestamp = int(key.split('/')[3].split('=')[1])
             if timestamp < int (last_timestamp_dict[action]):
                 files_to_migrate.append(key)
    number_of_files [action] = len (files_to_migrate)
    total_paths_df = spark.createDataframe (files_to_migrate, StringType())
    total_paths_df.coalesce(1).write.mode ("overwrite").format ("com.databricks.spark.avro").save(total_files_log_path)
    try:
        with concurrent.futures.ThreadPoolExecutor (max_workers=40) as executor:
            for key in files_to_migrate:
                future = executor.submit(prepare_path, key, destination_bucket_prefix)
    except Exception as e:
        print(str(e))
    success_df = spark.createDataFrame(success_keys, StringType())
    success_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.avro").save(success_files_log_path)
    failed_df = spark.createDataFrame(failed_keys, StringType())
    failed_df.coalesce(1).write.mode("overwrite").format("com. databricks.spark.avro").save(failed_files_log_path)
