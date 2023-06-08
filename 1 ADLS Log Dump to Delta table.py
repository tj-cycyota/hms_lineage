# Databricks notebook source
# MAGIC %md # 00. Setup Auth to storage accounts
# MAGIC Note: The service pincipal details below are used to both generate logs from operations on Storage Account A (e.g. read from a Delta table) AND read those resulting logs in storage account B

# COMMAND ----------

# # Uncomment and set SP access details, if not already set on the cluster
# APPLICATION_ID = "xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx"
# DIRECTORY_ID = "xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx"
# APP_KEY = dbutils.secrets.get(scope = "<SCOPE>", key = "adls-app-key")
# spark.conf.set("fs.azure.account.auth.type", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id", APPLICATION_ID)
# spark.conf.set("fs.azure.account.oauth2.client.secret", APP_KEY)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/"+DIRECTORY_ID+"/oauth2/token")

# COMMAND ----------

# MAGIC %md # 01. Simulate Read/Write operations from storage
# MAGIC
# MAGIC This part is only needed if you do not already have logs to process on the storage account you are trying to monitor

# COMMAND ----------

# STORAGE_ACCOUNT = "<NAME OF STORAGE ACCOUNT>"
# CONTAINER = "<NAME OF CONTAINER TO CONTAIN DATA>"
# TABLE_PATH = "/chewy/"
# source = f"abfss://"+CONTAINER+"@"+STORAGE_ACCOUNT+".dfs.core.windows.net/"
# print(source)

# #OP 1: list command
# dbutils.fs.ls(source+TABLE_PATH)

# #OP 2: read command
# from pyspark.sql.functions import col
# display(spark.read.format("delta").load(source+TABLE_PATH).filter(col("Size")=="Large"))

# #OP 3: write command (append)
# print(source+"chewy/")
# spark.read.format("delta").load(source+TABLE_PATH).limit(1).write.mode("append").save(source+"chewy/")

# COMMAND ----------

# MAGIC %md # 02. Read in StorageBlobLogs and materialize to Delta table
# MAGIC
# MAGIC When you setup Diagnostic Settings for your production storage account, you configure a location to ADLS storage: https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/diagnostic-settings?tabs=portal#destinations
# MAGIC
# MAGIC The variables below should be for that storage account (e.g. where the logs live)

# COMMAND ----------

# MAGIC %md Schema of logs, excluding partition columns, which are derivable from the "time" column and contain duplicates (e.g. "m" used for both months and mins)
# MAGIC ```
# MAGIC root
# MAGIC  |-- callerIpAddress: string (nullable = true)
# MAGIC  |-- category: string (nullable = true)
# MAGIC  |-- correlationId: string (nullable = true)
# MAGIC  |-- durationMs: long (nullable = true)
# MAGIC  |-- identity: struct (nullable = true)
# MAGIC  |    |-- authorization: array (nullable = true)
# MAGIC  |    |    |-- element: struct (containsNull = true)
# MAGIC  |    |    |    |-- action: string (nullable = true)
# MAGIC  |    |    |    |-- denyAssignmentId: string (nullable = true)
# MAGIC  |    |    |    |-- principals: array (nullable = true)
# MAGIC  |    |    |    |    |-- element: struct (containsNull = true)
# MAGIC  |    |    |    |    |    |-- id: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- type: string (nullable = true)
# MAGIC  |    |    |    |-- reason: string (nullable = true)
# MAGIC  |    |    |    |-- result: string (nullable = true)
# MAGIC  |    |    |    |-- roleAssignmentId: string (nullable = true)
# MAGIC  |    |    |    |-- roleDefinitionId: string (nullable = true)
# MAGIC  |    |    |    |-- type: string (nullable = true)
# MAGIC  |    |-- delegatedResource: struct (nullable = true)
# MAGIC  |    |    |-- objectId: string (nullable = true)
# MAGIC  |    |    |-- resourceId: string (nullable = true)
# MAGIC  |    |    |-- tenantId: string (nullable = true)
# MAGIC  |    |-- requester: struct (nullable = true)
# MAGIC  |    |    |-- appId: string (nullable = true)
# MAGIC  |    |    |-- audience: string (nullable = true)
# MAGIC  |    |    |-- objectId: string (nullable = true)
# MAGIC  |    |    |-- tenantId: string (nullable = true)
# MAGIC  |    |    |-- tokenIssuer: string (nullable = true)
# MAGIC  |    |    |-- upn: string (nullable = true)
# MAGIC  |    |-- tokenHash: string (nullable = true)
# MAGIC  |    |-- type: string (nullable = true)
# MAGIC  |-- location: string (nullable = true)
# MAGIC  |-- operationName: string (nullable = true)
# MAGIC  |-- operationVersion: string (nullable = true)
# MAGIC  |-- properties: struct (nullable = true)
# MAGIC  |    |-- accessTier: string (nullable = true)
# MAGIC  |    |-- accountName: string (nullable = true)
# MAGIC  |    |-- clientRequestId: string (nullable = true)
# MAGIC  |    |-- conditionsUsed: string (nullable = true)
# MAGIC  |    |-- contentLengthHeader: long (nullable = true)
# MAGIC  |    |-- downloadRange: string (nullable = true)
# MAGIC  |    |-- etag: string (nullable = true)
# MAGIC  |    |-- lastModifiedTime: string (nullable = true)
# MAGIC  |    |-- metricResponseType: string (nullable = true)
# MAGIC  |    |-- objectKey: string (nullable = true)
# MAGIC  |    |-- referrerHeader: string (nullable = true)
# MAGIC  |    |-- requestBodySize: long (nullable = true)
# MAGIC  |    |-- requestHeaderSize: long (nullable = true)
# MAGIC  |    |-- responseBodySize: long (nullable = true)
# MAGIC  |    |-- responseHeaderSize: long (nullable = true)
# MAGIC  |    |-- serverLatencyMs: long (nullable = true)
# MAGIC  |    |-- serviceType: string (nullable = true)
# MAGIC  |    |-- tlsVersion: string (nullable = true)
# MAGIC  |    |-- userAgentHeader: string (nullable = true)
# MAGIC  |-- protocol: string (nullable = true)
# MAGIC  |-- resourceId: string (nullable = true)
# MAGIC  |-- resourceType: string (nullable = true)
# MAGIC  |-- schemaVersion: string (nullable = true)
# MAGIC  |-- statusCode: long (nullable = true)
# MAGIC  |-- statusText: string (nullable = true)
# MAGIC  |-- time: string (nullable = true)
# MAGIC  |-- uri: string (nullable = true)
# MAGIC ```

# COMMAND ----------

# (mostly) Standard schema of logs
from pyspark.sql.types import *
schema = StructType([
    StructField("callerIpAddress", StringType(), True),
    StructField("category", StringType(), True),
    StructField("correlationId", StringType(), True),
    StructField("durationMs", StringType(), True),
    StructField("identity", StringType(), True),
    StructField("location", StringType(), True),
    StructField("operationName", StringType(), True),
    StructField("operationVersion", StringType(), True),
    StructField("properties", StructType([
       StructField("accessTier", StringType(), True),
       StructField("accountName", StringType(), True),
       StructField("clientRequestId", StringType(), True),
       StructField("conditionsUsed", StringType(), True),
       StructField("contentLengthHeader", StringType(), True),
       StructField("downloadRange", StringType(), True),
       StructField("etag", StringType(), True),
       StructField("lastModifiedTime", StringType(), True),
       StructField("metricResponseType", StringType(), True),
       StructField("objectKey", StringType(), True),
       StructField("referrerHeader", StringType(), True),
       StructField("requestHeaderSize", StringType(), True),
       StructField("requestBodySize", StringType(), True),
       StructField("responseBodySize", StringType(), True),
       StructField("responseHeaderSize", StringType(), True),
       StructField("serverLatencyMs", StringType(), True),
       StructField("serviceType", StringType(), True),
       StructField("tlsVersion", StringType(), True),
       StructField("userAgentHeader", StringType(), True)
      ]), True),
    StructField("protocol", StringType(), True),
    StructField("resourceId", StringType(), True),
    StructField("resourceType", StringType(), True),
    StructField("schemaVersion", StringType(), True),
    StructField("statusCode", StringType(), True),
    StructField("statusText", StringType(), True),
    StructField("time", StringType(), True),
    StructField("uri", StringType(), True),
    StructField("y", StringType(), True),
    StructField("m", StringType(), True),
    StructField("d", StringType(), True),
    StructField("h", StringType(), True),
    StructField("min", StringType(), True)
  ])

# COMMAND ----------

# MAGIC %md # 03. Setup streaming table with all logs

# COMMAND ----------

# REQUIRED: provide all variables
# Log location
LOG_STORAGE_ACCOUNT = "<NAME OF STORAGE ACCOUNT WHERE LOGS WERE SETUP>"

# Monitored location
MONITORED_STORAGE_ACCOUNT = "<NAME OF MONITORED STORAGE ACCOUNT>" # Name of the storage account being monitored, e.g. where table data resides
SUBSCRIPTION = "xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx" # Subscription ID of subscription where MONITORED_STORAGE_ACCOUNT resides
RESOURCE_GROUP = "xxxxx" # Name of resource group where the MONITORED_STORAGE_ACCOUNT resides

# Default container names below, only update if needed
READ_LOGS_CONTAINER = "insights-logs-storageread"
WRITE_LOGS_CONTAINER = "insights-logs-storagewrite"

# COMMAND ----------

# # Test cell: Uncomment below to confirm path is readable. After a few seconds, a DataFrame displaying logs should print
# df = (spark.readStream
#           .format("cloudFiles")
#           .option("cloudFiles.format", "json")
#           .schema(schema)
#           .load(write_logs_path)
#     )
# display(df)

# COMMAND ----------

#Feel free to set these paths below to any path of ADLS or DBFS. You may want to write them to the same location as other admin/monitoring tables. 

# read_logs_paths
read_logs_table_path = "dbfs:/tmp/hms_table_lineage_v0/read_logs"
read_logs_chkpt_path = "dbfs:/tmp/hms_table_lineage_v0/read_logs_checkpoints"

# write_logs_paths
write_logs_table_path = "dbfs:/tmp/hms_table_lineage_v0/write_logs"
write_logs_chkpt_path = "dbfs:/tmp/hms_table_lineage_v0/write_logs_checkpoints"

read_logs_json_path = f"abfss://{READ_LOGS_CONTAINER}@{LOG_STORAGE_ACCOUNT}.dfs.core.windows.net/resourceId=/subscriptions/{SUBSCRIPTION}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/{MONITORED_STORAGE_ACCOUNT}/blobServices/default/"
write_logs_json_path = f"abfss://{WRITE_LOGS_CONTAINER}@{LOG_STORAGE_ACCOUNT}.dfs.core.windows.net/resourceId=/subscriptions/{SUBSCRIPTION}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/{MONITORED_STORAGE_ACCOUNT}/blobServices/default/"

# COMMAND ----------

def get_log_streaming_df(log_path, log_schema):
  streaming_df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .schema(log_schema)
          .load(log_path)
        )
  return streaming_df

def write_df_to_delta(streaming_df, table_path, checkpoint_path):
  from pyspark.sql.functions import input_file_name, current_timestamp
  return (streaming_df
    .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(table_path)
  )

# Read Logs
read_logs_streaming_df = get_log_streaming_df(read_logs_json_path, schema)
read_logs_delta_table  = write_df_to_delta(read_logs_streaming_df, read_logs_table_path, read_logs_chkpt_path)
print(read_logs_delta_table)

# Write Logs
write_logs_streaming_df = get_log_streaming_df(write_logs_json_path, schema)
write_logs_delta_table  = write_df_to_delta(write_logs_streaming_df, write_logs_table_path, write_logs_chkpt_path)
print(write_logs_delta_table)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS hms_table_lineage_v0")
spark.sql(f"CREATE TABLE IF NOT EXISTS hms_table_lineage_v0.read_logs USING DELTA LOCATION '{read_logs_table_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS hms_table_lineage_v0.write_logs USING DELTA LOCATION '{write_logs_table_path}'")
spark.sql("USE hms_table_lineage_v0;")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Verify that ClusterIds are appearing
# MAGIC SELECT 
# MAGIC properties.clientRequestId,
# MAGIC regexp_extract(properties.clientRequestId, "(\\d{4}-\\d{6}-[a-z0-9]+)") as clusterId
# MAGIC
# MAGIC FROM read_logs
# MAGIC WHERE cast(time as timestamp) >= "2023-01-01"
# MAGIC AND LOWER(properties.userAgentHeader) LIKE "%databricks%"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hms_table_lineage_v0;
# MAGIC
# MAGIC WITH 
# MAGIC --Uncomment if want to test on specific tables first
# MAGIC -- table_filters AS (
# MAGIC --   VALUES "chewy", "chewy_filtered"
# MAGIC -- ),
# MAGIC
# MAGIC read_logs AS (
# MAGIC   SELECT 
# MAGIC     category,
# MAGIC     operationName,
# MAGIC     regexp_extract(properties.clientRequestId, "(\\d{4}-\\d{6}-[a-z0-9]+)") as clusterId,
# MAGIC     uri,
# MAGIC     collect_set(cast(time as timestamp)) as accessTimes
# MAGIC   FROM read_logs
# MAGIC   WHERE cast(time as timestamp) >= "2023-06-07"
# MAGIC   AND LOWER(properties.userAgentHeader) LIKE "%databricks%"
# MAGIC   AND operationName == "ReadFile"
# MAGIC   GROUP BY 1,2,3,4
# MAGIC ),
# MAGIC
# MAGIC write_logs AS (
# MAGIC   SELECT 
# MAGIC     category,
# MAGIC     operationName,
# MAGIC     regexp_extract(properties.clientRequestId, "(\\d{4}-\\d{6}-[a-z0-9]+)") as clusterId,
# MAGIC     uri,
# MAGIC     collect_set(cast(time as timestamp)) as accessTimes
# MAGIC   FROM write_logs
# MAGIC   WHERE cast(time as timestamp) >= "2023-06-07"
# MAGIC   AND LOWER(properties.userAgentHeader) LIKE "%databricks%"
# MAGIC   GROUP BY 1,2,3,4
# MAGIC ),
# MAGIC
# MAGIC unioned AS (
# MAGIC SELECT * FROM read_logs
# MAGIC UNION ALL
# MAGIC SELECT * FROM write_logs
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   coalesce(properties.accountName, split_part(properties.objectKey, "/", 2)) AS storageAccountName,
# MAGIC   split_part(properties.objectKey, "/", 3) AS container,
# MAGIC   regexp_extract(properties.clientRequestId, "(\\d{4}-\\d{6}-[a-z0-9]+)") as clusterId,
# MAGIC   REGEXP_EXTRACT(uri, "^(.*?)(?:/(_delta|part-))") AS table_path,
# MAGIC   *
# MAGIC FROM unioned 
# MAGIC -- INNER JOIN table_filters 
# MAGIC --   ON unioned.uri LIKE '%' || table_filters.col1 || '%'
