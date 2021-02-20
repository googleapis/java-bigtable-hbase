# MapReduce tools for HBase and Cloud Bigtable
A collection of tools in MapReduce for HBase and Bigtable

## MapReduce Job to import an HBase snapshot into Cloud Bigtable

### Prerequisites
Take a snapshot of the HBase table.
```bash
hbase> snapshot ‘<tableName>’, ‘<snapshotName>’
```

Export the HBase snapshot to a GCS bucket (Note: the [GCS connector is required](https://cloud.google.com/dataproc/docs/concepts/connectors/install-storage-connector) for the on-prem application to write to GCS)
```bash
hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot \
 -snapshot <snapshotName> \
 -copy-to gs://<bucket/<snapshot-dir> \
 -mappers <num-mappers>
```

### Usage

1. Download shaded jar (bigtable-mapreduce-import-VERSION-SNAPSHOT-shaded.jar) or build the parent module, e.g., `mvn clean compile package -DskipTests`

2. [Create a Dataproc Cluster](https://cloud.google.com/dataproc/docs/guides/create-cluster) for executing the Import Job

3. Provision [Bigtable instance and cluster](https://cloud.google.com/bigtable/docs/instances-clusters-nodes) and update the below environment variables for the project configuration. Important note: the below is an example with default 1 node with HDD type. A production deployment should size the Bigtable cluster and use the appropriate [storage type](https://cloud.google.com/bigtable/docs/choosing-ssd-hdd) that meets the workload requirements.

```bash
# set env properties
export PROJECT_ID=$(gcloud info --format='value(config.project)')
export ZONE=$(gcloud info --format='value(config.properties.compute.zone)')
export REGION=${ZONE%-*}
export DATAPROC_CLUSTER=<DATAPROC_CLUSTER_NAME>

# bigtable table properties
export CBT_INSTANCE=<BIGTABLE_INSTANCE>
export CBT_CLUSTER=<BIGTABLE_CLUSTER>
export CBT_TABLENAME=<TABLENAME>
export CBT_COLUMN_FAMILY=<CF1[,CF]>

# create Bigtable instance and cluster
cbt -project ${PROJECT_ID} createinstance ${CBT_INSTANCE} \
 "${CBT_INSTANCE} desc" ${CBT_CLUSTER} ${ZONE} 1 HDD 
```

&nbsp;4. Create the table and all column families to match the source table (more details on creating tables can be found [here](https://cloud.google.com/bigtable/docs/cbt-reference#create_a_table))
 
```bash
cbt -project ${PROJECT_ID} -instance ${CBT_INSTANCE} createtable ${CBT_TABLENAME} \
  "families=${CBT_COLUMN_FAMILY}"
```

&nbsp;5. After creating the table, verify the table and column families. For example use the command:

```bash
cbt -project ${PROJECT_ID} -instance ${CBT_INSTANCE} ls ${CBT_TABLENAME}
```

&nbsp;6. Run the following command to start the import snapshot job on the Dataproc cluster that was created earlier. Update the arguments for the job. 

```bash
# env properties for dataproc / import job
export DATAPROC_REGION=${REGION}
export DATAPROC_JOB_JAR=bigtable-mapreduce-import-1.14.1-SNAPSHOT-shaded.jar
export DATAPROC_JOB_IMPORT_CLASS=com.google.cloud.bigtable.mapreduce.hbasesnapshots.ImportHBaseSnapshotJob

# job properties
export IMPORT_PROPS="-Dgoogle.bigtable.project.id=${PROJECT_ID} \
 -Dgoogle.bigtable.instance.id=${CBT_INSTANCE}"

# job arguments 
export ARG_SNAPSHOT_NAME=<SNAPSHOT_NAME>
export ARG_TABLENAME=${CBT_TABLENAME}
export ARG_SNAPSHOT_ROOT_DIR=<SNAPSHOT_ROOT_DIR>
export ARG_RESTORE_BASE_DIR=<SNAPSHOT_RESTORE_BASE_DIR>
export IMPORT_ARGS="${ARG_SNAPSHOT_NAME} ${ARG_TABLENAME} \
 ${ARG_SNAPSHOT_ROOT_DIR} ${ARG_RESTORE_BASE_DIR}"

# launch job
gcloud dataproc jobs submit hadoop \
  --cluster ${DATAPROC_CLUSTER} \
  --region $DATAPROC_REGION \
  --project ${PROJECT_ID} \
  --jars ${DATAPROC_JOB_JAR} \
  --class ${DATAPROC_JOB_IMPORT_CLASS} \
  -- ${IMPORT_PROPS} \
   ${IMPORT_ARGS}
```

ImportHBaseSnapshotJob Usage:

```bash
Usage: hadoop jar <JAR> <CLASS> <properties> <snapshot-name> <tablename> 
  <snapshot-root-dir> <restore-base-dir> <gs-default-fs>

Required properties:
  -Dgoogle.bigtable.project.id=<bigtable-project-id>
  -Dgoogle.bigtable.instance.id=<bigtable-instance-id>

Required arguments:
  snapshot-name     - name of hbase snapshot to read
  tablename         - bigtable table to import into
  snapshot-root-dir - directory of snapshot,
         e.g., gs://hbase-migration-table1-bucket/export/table1-snapshot
  restore-base-dir  - a temporary base directory to restore hlinks of the 
         snapshot into for read, 
         e.g., gs://hbase-migration-table1-bucket/export/table1-restore
```
