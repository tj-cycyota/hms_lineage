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

# MAGIC %md # 01. Confirm Lookup tables exist and as expected
# MAGIC
# MAGIC ## HMS Details
# MAGIC Prior to running this script, you should have run this script to dump HMS information to a Delta table: https://github.com/himanshuguptadb/Unity_Catalog/tree/master/External%20Metastore%20Inventory
# MAGIC
# MAGIC The code below assumes a database name for a table called `ExternaL_Metastore_Inventory`
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# If all lookup tables are in the same location, set variable once here; if not, set in following cells
schema = "hms_table_lineage_v0"

# COMMAND ----------

# Schema+Table names where HMS details are stored
ExternaL_Metastore_Inventory_schema = schema
ExternaL_Metastore_Inventory_table = "ExternaL_Metastore_Inventory"

# COMMAND ----------

# Validate we can parse Location paths from HMS
# Results should show 3 new columns with expected storage_account name, storage_container name, and table_path (including sub-directories)
display(
  spark.sql(f"""
            SELECT 
            regexp_extract(Location, "@(.*?)\\.dfs") as storage_account,
            regexp_extract(Location, "://(.*?)@") as storage_container,
            regexp_extract(Location, "windows\\.net/(.*)") as table_path,
            * 
            FROM {ExternaL_Metastore_Inventory_schema}.{ExternaL_Metastore_Inventory_table}
            """)
)

# COMMAND ----------

# MAGIC %md ## Job Run details
# MAGIC Prior to running this script, you should have run a script to hit the Databricks jobs/runs API to get cluster Ids for each job run.
# MAGIC
# MAGIC The code below assumes you have table called `runs` containing results from the Databricks `jobs/runs/list` API

# COMMAND ----------

# Schema+Table names where job runs are available
Job_Run_schema = schema
Job_Run_table = "runs"

# COMMAND ----------

# Validate we can parse ClusterIds from Job Runs
# Results should show (at minimum) a mapping of JobId to ClusterId
display(
  spark.sql(f"""
            SELECT 
              job_id,
              cluster_instance.cluster_id,
              start_time,
              run_name,
              run_id,
              state.result_state,
              creator_user_name,
              task,
              trigger,
              format,
              workspace
            FROM {Job_Run_schema}.{Job_Run_table}
            WHERE state.result_state == "SUCCESS"
            """)
)

# COMMAND ----------

# MAGIC %md ## ADLS Access logs
# MAGIC Prior to running this script, you should have run notebook `1` to ingest ADLS logs as a Delta table
# MAGIC
# MAGIC The code below assumes you have 2 tables, one for read logs and one for write logs

# COMMAND ----------

# Schema+Table names where ADLS Access logs are stored
ADLS_Logs_schema = schema
ADLS_read_logs = "read_logs"
ADLS_write_logs = "write_logs"

# COMMAND ----------

# Validate that table paths are as expected for each table type
# NOTE: in testing, not all possible table types/storage patterns were accounted for, so regex adjustment may be needed
display(
  spark.sql(f"""
SELECT 
  cast(time as timestamp) as time,
  category,
  operationName,
  -- regexp_extract(properties.clientRequestId, "(\d{4}-\d{6}-[a-z0-9]+)") as clusterId,
  split_part(properties.clientRequestId, "--", 1) as clusterId,
  uri,
  regexp_extract(uri, "https://(.*?)\\.dfs") as storage_account,
  -- regexp_extract(uri, "\.net/([\w-]+)[/?&]") as storage_container,
  regexp_extract(uri, "https?://[^/]+/([^/?&]*)") as storage_container,
  regexp_extract(uri, "https?://[^/]+/[^/]+/([^/?&]+(?:/[^/?&]+)*)") as file_key_raw,
  regexp_extract(regexp_extract(uri, "https?://[^/]+/[^/]+/([^/?&]+(?:/[^/?&]+)*)"), "(.*?)(?=/[^/]*(?:_delta_log|\\.[^.]+$))") as table_path,
  properties.objectKey

FROM {ADLS_Logs_schema}.{ADLS_read_logs}
WHERE LOWER(properties.userAgentHeader) LIKE "%databricks%"
AND regexp_extract(properties.clientRequestId, "(\\d{4}-\\d{6}-[a-z0-9]+)") IS NOT NULL
AND operationName IN ("ReadFile")
LIMIT 100
            """)
)

# COMMAND ----------

# MAGIC %md # 02. Join Jobs to Tables via Cluster ID

# COMMAND ----------

display(
  spark.sql(f"""
            
WITH job_clusters AS (
  SELECT 
    job_id,
    cluster_instance.cluster_id
  FROM {Job_Run_schema}.{Job_Run_table}
  WHERE state.result_state == "SUCCESS"
),

external_metastore AS (
  SELECT 
    regexp_extract(Location, "@(.*?)\\.dfs") as storage_account,
    regexp_extract(Location, "://(.*?)@") as storage_container,
    regexp_extract(Location, "windows\\.net/(.*)") as table_path,
    * 
  FROM {ExternaL_Metastore_Inventory_schema}.{ExternaL_Metastore_Inventory_table}  
),

combined_logs AS (
  SELECT * FROM {ADLS_Logs_schema}.{ADLS_read_logs}
  UNION ALL 
  SELECT * FROM {ADLS_Logs_schema}.{ADLS_write_logs}
),

parsed_logs AS (
  SELECT 
  cast(time as timestamp) as time,
    category,
    operationName,
    -- regexp_extract(properties.clientRequestId, "(\d{4}-\d{6}-[a-z0-9]+)") as clusterId,
    split_part(properties.clientRequestId, "--", 1) as clusterId,
    uri,
    regexp_extract(uri, "https://(.*?)\\.dfs") as storage_account,
    -- regexp_extract(uri, "\.net/([\w-]+)[/?&]") as storage_container,
    regexp_extract(uri, "https?://[^/]+/([^/?&]*)") as storage_container,
    regexp_extract(uri, "https?://[^/]+/[^/]+/([^/?&]+(?:/[^/?&]+)*)") as file_key_raw,
    regexp_extract(regexp_extract(uri, "https?://[^/]+/[^/]+/([^/?&]+(?:/[^/?&]+)*)"), "(.*?)(?=/[^/]*(?:_delta_log|\\.[^.]+$))") as table_path,
    properties.objectKey
  FROM combined_logs
  WHERE LOWER(properties.userAgentHeader) LIKE "%databricks%"
  AND regexp_extract(properties.clientRequestId, "(\\d{4}-\\d{6}-[a-z0-9]+)") IS NOT NULL
  -- AND uri LIKE "%chewy%"
)

SELECT * FROM parsed_logs
WHERE operationName IN ("AppendFile", "ReadFile")
            """)
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # TO DO
# MAGIC * Join logic to determine which jobs read/write which tables
