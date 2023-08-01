# HMS Lineage scripts
## Objective
Map Databricks jobs to the tables from which they read/write via ADLS Diagnostic Logs. 

## Context
Databricks jobs read and write various tables. Historically, these tables have been managed by a Hive Metastore (HMS), either the Databricks provided HMS that is default for each workspace, or an externally managed HMS. In a project to upgrade a Databricks deployment to leverage [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/), it sometimes becomes necessary to build a "lineage graph" of which jobs read and write which tables in order to sequence an upgrade properly, as some tables may be dependent on other tables. While UC provides this lineage capability [out of the box](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/#data-lineage-for-unity-catalog), it is possible (if complex) to reconstruct aspects of it with Databricks and Azure primitives.

## Pre-requisites
To successfully follow the notebooks in this repo, the following must be true:

* You have enabled [ADLS Diagnostic Logs](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-best-practices#monitor-telemetry) on the storage locations for the tables on which you want to construct lineage.
* You have run the [External Metastore Inventory](https://github.com/himanshuguptadb/Unity_Catalog/tree/master/External%20Metastore%20Inventory) script to "dump" HMS information to a queryable Delta table.
* You have run the script (provided by your Databricks representative or manually constructed) to a create a queryable Delta table with all Jobs Run information available on the `jobs/runs/list` API.

## Details
In upgrading to Unity Catalog, it is important to identify the dependencies between automated jobs, and the data tables they read from and write to. Instead of asking each developer or job owner to determine inputs/outputs of each production job, in Azure it is usually possible to determine this by mapping ADLS logs to cluster IDs. 

For each job run (e.g. an instance of a job occurrence), a new ephemeral job cluster gets created to run the job’s tasks. As this cluster accesses cloud storage, [ADLS StorageBlobLogs](https://learn.microsoft.com/en-us/azure/storage/blobs/monitor-blob-storage?tabs=azure-portal#sample-kusto-queries) detail the cluster that used some credential (e.g. Service Principal) to access a table at a specific storage path

By creating a mapping of: job run → job cluster id → storage access event → storage path → external HMS table entry, we can determine the specific tables associated with each job:
![mapping logic](https://github.com/tj-cycyota/hms_lineage/blob/master/_resources/hms_lineage_mapping.png?raw=true)
