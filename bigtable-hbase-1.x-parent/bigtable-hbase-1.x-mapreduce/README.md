# Map/Reduce jobs to import from/export to Bigtable

This module provides a work alike to some of the jobs implemented in hbase-server.
Specifically this currently has the ability to export and import SequenceFiles
from/to Cloud Bigtable, import HBase snapshots using a Map Reduce cluster (ie. 
dataproc), and HashTable/SyncTable for validation.  

## Setup

To use the tools in this folder, you can download them from the maven repository, or
you can build them using Maven.

[//]: # ({x-version-update-start:bigtable-client-parent:released})
### Download the jars
Download [bigtable-hbase-1.x-mapreduce jars](https://search.maven.org/artifact/com.google.cloud.bigtable/bigtable-hbase-1.x-mapreduce), which is an aggregation of all required jars.

### Build the jars yourself

Go to the top level directory and build the repo
then return to this sub directory.

```
cd ../../
mvn clean install -DskipTests=true
cd bigtable-hbase-1.x-parent/bigtable-hbase-1.x-mapreduce
```

## Expected Usage 

### On-prem Hadoop

1. Download or build bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar
2. Download service account credentials json from Google Cloud Console.
3. Submit the job using your edge node's hadoop installation. 
   ```bash
    # Export to SequenceFiles
    GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
    hadoop jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        export-table \
        -Dgoogle.bigtable.project.id=<project-id> \
        -Dgoogle.bigtable.instance.id=<instance-id> \
        <table-id> \
        <outputdir>
   
    # Import from SequenceFiles
    GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
    hadoop jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        import-table \
        -Dgoogle.bigtable.project.id=<project-id> \
        -Dgoogle.bigtable.instance.id=<instance-id> \
        <table-id> \
        <inputdir>
   
    # Import from HBase snapshot
    GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
    hadoop jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        import-snapshot \
        -Dgoogle.bigtable.project.id=<project-id> \
        -Dgoogle.bigtable.instance.id=<instance-id> \
        <snapshot-name> \
        <snapshot-dir> \
        <table-id> \
        <tmp-dir>
   
    # HashTable on HBase
    GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
    hadoop jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        hash-table \
        -Dhbase.zookeeper.quorum=<source-zk-quorum> \
        <source-table-id> \
        <hash-outputdir-hbase>
   
    # SyncTable on Bigtable (dryrun enabled by default)
    GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
    hadoop jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        sync-table \
        --sourcezkcluster=<source-zk-quorum> \
        --targetbigtableproject=<project-id> \
        --targetbigtableinstance=<instance-id> \
        <hash-outputdir-hbase> \
        <source-table-id> \
        <target-table-id>
   ```

### Dataproc

1. Download or build bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar.
2. Install the gcloud sdk.
3. Configure [Bigtable IAM roles](https://cloud.google.com/bigtable/docs/access-control#roles) 
    for the [Dataproc Service Account](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts#what_are_service_accounts) 
    when running on Dataproc.
4. Submit the job. 
   ```bash
    # Export to SequenceFiles
    gcloud dataproc jobs submit hadoop \
        --project <project-id> \
        --cluster <dataproc-cluster> \
        --region <dataproc-region> \
        --jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        -- \
        export-table \
        -Dgoogle.bigtable.project.id=<project-id> \
        -Dgoogle.bigtable.instance.id=<instance-id> \
        <table-id> \
        <outputdir>
   
    # Import from SequenceFiles
    gcloud dataproc jobs submit hadoop \
        --project <project-id> \
        --cluster <dataproc-cluster> \
        --region <dataproc-region> \
        --jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        -- \
        import-table \
        -Dgoogle.bigtable.project.id=<project-id> \
        -Dgoogle.bigtable.instance.id=<instance-id> \
        <table-id> \
        <inputdir>
   
    # Import from HBase snapshot
    gcloud dataproc jobs submit hadoop \
        --project <project-id> \
        --cluster <dataproc-cluster> \
        --region <dataproc-region> \
        --jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        -- \
        import-snapshot \
        -Dgoogle.bigtable.project.id=<project-id> \
        -Dgoogle.bigtable.instance.id=<instance-id> \
        <snapshot-name> \
        <snapshot-dir> \
        <table-id> \
        <tmp-dir>
   
    # HashTable on HBase
    gcloud dataproc jobs submit hadoop \
        --project <project-id> \
        --cluster <dataproc-cluster> \
        --region <dataproc-region> \
        --jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        -- \
        hash-table \
        -Dhbase.zookeeper.quorum=<source-zk-quorum> \
        <table-id> \
        <hash-outputdir-hbase>
   
    # SyncTable on Bigtable (dryrun enabled by default)
    gcloud dataproc jobs submit hadoop \
        --project <project-id> \
        --cluster <dataproc-cluster> \
        --region <dataproc-region> \
        --jar bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar \
        -- \
        sync-table \
        --sourcezkcluster=<source-zk-quorum> \
        --targetbigtableproject=<project-id> \
        --targetbigtableinstance=<instance-id> \
        <hash-outputdir-hbase> \
        <source-table-id> \
        <target-table-id>
   ```

## Examples
### Example job to import an HBase snapshot into Cloud Bigtable

The following is an example for backing up an HBase table in Google Cloud Storage 
as an HBase snapshot and then importing the HBase snapshot to Bigtable.

#### On-prem hbase cluster
1. Take a snapshot of the HBase table.
    ```bash
    hbase> snapshot ‘<tableName>’, ‘<snapshotName>’
    ```
   
2. Export the HBase snapshot to a GCS bucket (Note: the [GCS connector is required](https://cloud.google.com/dataproc/docs/concepts/connectors/install-storage-connector)
for the on-prem application to write to GCS).
    ```bash
    hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot \
     -snapshot <snapshotName> \
     -copy-to gs://<bucket>/<snapshot-dir> \
     -mappers <num-mappers>
    ```

#### Google cloud platform project

1. [Install the gcloud sdk](https://cloud.google.com/sdk/docs/install) or use the [cloud shell](https://cloud.google.com/shell/docs/running-gcloud-commands) and set the following 
environment variables for running the subsequent steps.

    GCP environment properties 
    ```bash
    # set env properties
    export PROJECT_ID=<PROJECT_ID>
    export ZONE=<ZONE>
    export REGION=${ZONE%-*}
    export DATAPROC_CLUSTER=<DATAPROC_CLUSTER_NAME>
    
    # bigtable table properties
    export CBT_INSTANCE=<BIGTABLE_INSTANCE>
    export CBT_CLUSTER=<BIGTABLE_CLUSTER>
    export CBT_TABLENAME=<TABLENAME>
    export CBT_COLUMN_FAMILY=<CF1[,CF]>
    
    # dataproc job jar
    export JOB_JAR=bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar
    
    # dataproc job args
    export JOB_ARG_SNAPSHOT_NAME=<SNAPSHOT_NAME>
    export JOB_ARG_SNAPSHOT_DIR=<SNAPSHOT_DIR>
    export JOB_ARG_TEMP_DIR=<JOB_TEMP_DIR>
    ```

2. [Create a Dataproc Cluster](https://cloud.google.com/dataproc/docs/guides/create-cluster) for executing the import snapshot job.

3. Provision [Bigtable instance and cluster](https://cloud.google.com/bigtable/docs/instances-clusters-nodes). **Important note:** the below is an example with
default 1 node with HDD type. A production deployment should size the Bigtable
cluster and use the appropriate [storage type](https://cloud.google.com/bigtable/docs/choosing-ssd-hdd) that meets the workload requirements.

    Create bigtable instance and cluster

    ```bash
    # create Bigtable instance and cluster
    cbt -project ${PROJECT_ID} createinstance ${CBT_INSTANCE} \
     "${CBT_INSTANCE} desc" ${CBT_CLUSTER} ${ZONE} 1 HDD 
    ```

4. Create the table and all column families to match the source table (more 
details on creating tables can be found [here](https://cloud.google.com/bigtable/docs/cbt-reference#create_a_table)).

    ```bash
    cbt -project ${PROJECT_ID} -instance ${CBT_INSTANCE} createtable ${CBT_TABLENAME} \
      "families=${CBT_COLUMN_FAMILY}"
    ```

5. After creating the table, verify the table and column families. For example use
the command:

    ```bash
    cbt -project ${PROJECT_ID} -instance ${CBT_INSTANCE} ls ${CBT_TABLENAME}
    ```

#### Run the import snapshot job

1. Run the following command to start the `import-snapshot` job on the Dataproc cluster
that was created. Slowly scale the dataproc cluster to increase/decrease throughput 
and similarly scale up/down the bigtable cluster to meet the throughput demand. See 
Bigtable [scaling limitations](https://cloud.google.com/bigtable/docs/scaling#limitations) if observing slower performance than expected.

    ```bash
    gcloud dataproc jobs submit hadoop \
        --project ${PROJECT_ID} \
        --cluster ${DATAPROC_CLUSTER} \
        --region ${REGION} \
        --project ${PROJECT_ID} \
        --jar ${JOB_JAR} \
        -- \
        import-snapshot \
        -Dgoogle.bigtable.project.id=${PROJECT_ID} \
        -Dgoogle.bigtable.instance.id=${CBT_INSTANCE} \
        ${JOB_ARG_SNAPSHOT_NAME} \
        ${JOB_ARG_SNAPSHOT_DIR} \
        ${CBT_TABLENAME} \
        ${JOB_ARG_TEMP_DIR}
    ```

Additional options: 

1. Set client based timeouts for the buffered mutator requests (default 600000ms).
For example:

    ```bash
    -Dgoogle.bigtable.rpc.use.timeouts=true
    -Dgoogle.bigtable.mutate.rpc.timeout.ms=600000
    ```

2. Latency based throttling may reduce the impact the import batch job may have on 
other workloads. Throttling should be tested for the use-case and can be enabled by 
setting the properties for the job. For example:

    ```bash
    -Dgoogle.bigtable.buffered.mutator.throttling.enable=true
    -Dgoogle.bigtable.buffered.mutator.throttling.threshold.ms=100
    ```

3. The number of splits per region (default 2). For example:

    ```bash
    -Dgoogle.bigtable.import.snapshot.splits.per.region=3
    ```

4. Other mapreduce configurations can be set as properties. For example:

    ```bash
    -Dmapreduce.map.maxattempts=4
    -Dmapreduce.map.speculative=false
    -Dhbase.snapshot.thread.pool.max=10
    ```

### Example jobs to validate the data migrated from source to target

1. Set the following additional environment variables for running the validation steps.
    ```bash
    # hash-table validation job
    export HBASE_TABLENAME=<HBASE_TABLENAME>
    # hbase zookeeper quorum (ie. zk1.example.com:2181)
    export HBASE_ZK_QUORUM=<ZK_QUORUM>
    export HASH_OUTPUTDIR=<HASH_OUTPUTDIR>

    # sync-table validation job
    export HBASE_ZK_QUORUM_FULL=${HBASE_ZK_QUORUM}:/hbase
    ```

2. Run `hash-table` and compute hashes for ranges on the source table and output 
   results to a GCS bucket (See [HashTable/SyncTable](https://hbase.apache.org/book.html#_step_1_hashtable) doc for more details).
    ```bash
    hadoop jar ${JOB_JAR} \
        hash-table \
        -Dhbase.zookeeper.quorum=${HBASE_ZK_QUORUM} \
        ${HBASE_TABLENAME} \
        ${HASH_OUTPUTDIR}
    ```

3. Run `sync-table` to generate hashes on the target table and compare these hashes with
   the output from `hash-table`. For diverging hashes, a cell-level comparison is performed    
   between the source and target and summarized in the job counters. 
    ```bash
    # dryrun mode (readonly) enabled by default 
    gcloud dataproc jobs submit hadoop \
        --project ${PROJECT_ID} \
        --cluster ${DATAPROC_CLUSTER} \
        --region ${REGION} \
        --project ${PROJECT_ID} \
        --jar ${JOB_JAR} \
        -- \
        sync-table \
        --sourcezkcluster=${HBASE_ZK_QUORUM_FULL} \
        --targetbigtableproject=${PROJECT_ID} \
        --targetbigtableinstance=${CBT_INSTANCE} \
        ${HASH_OUTPUTDIR} \
        ${HBASE_TABLENAME} \
        ${CBT_TABLENAME}
    ```
Note: Connection with the source is required for providing cell-level comparison. Users may 
enable debug mode `--properties mapreduce.map.log.level=DEBUG` on the job to provide additional 
details on the diverging hash ranges and cell mismatches if divergence is detected. Job 
configurations may also be updated to run `hash-table` against bigtable and `sync-table` run 
against hbase.

Additional Options:

1. Disable dry run mode to perform synchronization between source and target for diverging hash ranges.

    ```bash
    --dryrun=false
    ```

2. Other job configuration and details may be referred to in [HBase SyncTable description](https://www.google.com/url?sa=D&q=https%3A%2F%2Fhbase.apache.org%2Fbook.html%23_step_2_synctable).

## Backwards compatibility

To maintain backwards compatibility of this artifact, we still provide
`bigtable-hbase-1.x-mapreduce-2.5.0-shaded.jar` artifact that includes
hadoop jars. However we encourage our users to migrate to 
`bigtable-hbase-1.x-mapreduce-2.5.0-shaded-byo-hadoop.jar` to avoid dependency
conflicts with the existing classpath on Hadoop workers.

[//]: # ({x-version-update-end})
