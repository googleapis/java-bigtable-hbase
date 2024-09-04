# HBase Sequence Files to Cloud Bigtable using Beam

This folder contains tools to support importing and exporting HBase data to
Google Cloud Bigtable using Cloud Dataflow.

## Setup

To use the tools in this folder, you can download them from the maven
repository, or
you can build them using Maven.


[//]: # ({x-version-update-start:bigtable-client-parent:released})

### Download the jars

Download [the import/export jars](https://search.maven.org/artifact/com.google.cloud.bigtable/bigtable-beam-import),
which is an aggregation of all required jars.

### Build the jars yourself

Go to the top level directory and build the repo
then return to this sub directory.

```
cd ../../
mvn clean install -DskipTests=true
cd bigtable-dataflow-parent/bigtable-beam-import
```

***

# Tools

## Data export pipeline

You can export data into a snapshot or into sequence files. If you're migrating
your data from HBase to Bigtable, using snapshots is the preferred method.

### Exporting snapshots from HBase

Perform these steps from Unix shell on an HBase edge node.

1. Set the environment variables
    ```
    TABLE_NAME=your-table-name
    SNAPSHOT_NAME=your-snapshot-name 
    SNAPSHOT_EXPORT_PATH=/hbase-migration-snap
    BUCKET_NAME="gs://bucket-name"
   
    NUM_MAPPERS=16
    ```
1. Take the snapshot
    ```
    echo "snapshot '$TABLE_NAME', '$SNAPSHOT_NAME'" | hbase shell -n
    ```

1. Export the snapshot
    1.
   Install [hadoop connectors](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/INSTALL.md)
    1. Copy to a GCS bucket
   ```
    hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot $SNAPSHOT_NAME \
        -copy-to $BUCKET_NAME$SNAPSHOT_EXPORT_PATH/data -mappers $NUM_MAPPERS
    ```
1. Create hashes for the table to be used during the data validation step.
   [Visit the HBase documentation for more information on each parameter](http://hbase.apache.org/book.html#_step_1_hashtable).
   ```
   hbase org.apache.hadoop.hbase.mapreduce.HashTable --batchsize=10 --numhashfiles=10 \
   $TABLE_NAME $BUCKET_NAME$SNAPSHOT_EXPORT_PATH/hashtable
   ```    

### Exporting sequence files from HBase

1. On your HDFS set the environment variables.
    ```
    TABLE_NAME="my-new-table"
    EXPORTDIR=/usr/[USERNAME]/hbase-${TABLE_NAME}-export
    hadoop fs -mkdir -p ${EXPORTDIR}
    MAXVERSIONS=2147483647
    ```
1. On an edge node, that has HBase classpath configured, run the export
   commands.
    ```
    cd $HBASE_HOME
    bin/hbase org.apache.hadoop.hbase.mapreduce.Export \
        -Dmapred.output.compress=true \
        -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
        -Dhbase.client.scanner.caching=100 \
        -Dmapred.map.tasks.speculative.execution=false \
        -Dmapred.reduce.tasks.speculative.execution=false \
        $TABLE_NAME $EXPORTDIR $MAXVERSIONS
    ```

### Exporting snapshots from Bigtable

Exporting HBase snapshots from Bigtable is not supported.

### Exporting sequence files from Bigtable

1. Set the environment variables.
    ```    
    PROJECT_ID=your-project-id
    INSTANCE_ID=your-instance-id
    CLUSTER_NUM_NODES=3
    TABLE_NAME=your-table-name
    
    BUCKET_NAME=gs://bucket-name
    ```
1. Run the export.
   ```
   java -jar bigtable-beam-import-2.3.0.jar export \
        --runner=dataflow \
        --project=$PROJECT_ID \
        --bigtableInstanceId=$INSTANCE_ID \
        --bigtableTableId=$TABLE_NAME \
        --destinationPath=$BUCKET_NAME/hbase_export/ \
        --tempLocation=$BUCKET_NAME/hbase_temp/ \
        --maxNumWorkers=$(expr 3 \* $CLUSTER_NUM_NODES) \
        --region=$REGION
   ```

## Importing to Bigtable

You can import data into Bigtable from a snapshot or sequence files. Before you
begin your import you must create
the tables and column families in Bigtable via
the [schema translation tool](https://github.com/googleapis/java-bigtable-hbase/tree/master/bigtable-hbase-1.x-parent/bigtable-hbase-1.x-tools)
or using the Bigtable command line tool and running the following:

    cbt createtable your-table-name
    cbt createfamily your-table-name your-column-family

Once your import is completed follow the instructions for the validator below to
ensure it was successful.

Please pay attention to the Cluster CPU usage and adjust the number of Dataflow
workers accordingly.

### Snapshots (preferred method)

1. Set the environment variables.
    ```
    PROJECT_ID=your-project-id
    INSTANCE_ID=your-instance-id
    TABLE_NAME=your-table-name
    REGION=us-central1

    SNAPSHOT_GCS_PATH="$BUCKET_NAME/hbase-migration-snap"
    SNAPSHOT_NAME=your-snapshot-name
    ```

1. Run the import.
    ```
    java -jar bigtable-beam-import-2.3.0.jar importsnapshot \
        --runner=DataflowRunner \
        --project=$PROJECT_ID \
        --bigtableInstanceId=$INSTANCE_ID \
        --bigtableTableId=$TABLE_NAME \
        --hbaseSnapshotSourceDir=$SNAPSHOT_GCS_PATH/data \
        --snapshotName=$SNAPSHOT_NAME \
        --stagingLocation=$SNAPSHOT_GCS_PATH/staging \
        --gcpTempLocation=$SNAPSHOT_GCS_PATH/temp \
        --maxNumWorkers=$(expr 3 \* $CLUSTER_NUM_NODES) \
        --region=$REGION
    ```

### Snappy compressed Snapshots

1. Set the environment variables.
    ```
    PROJECT_ID=your-project-id
    INSTANCE_ID=your-instance-id
    TABLE_NAME=your-table-name
    REGION=us-central1

    SNAPSHOT_GCS_PATH="$BUCKET_NAME/hbase-migration-snap"
    SNAPSHOT_NAME=your-snapshot-name
    ```

1. Run the import.
    ```
    java -jar bigtable-beam-import-2.3.0.jar importsnapshot \
        --runner=DataflowRunner \
        --project=$PROJECT_ID \
        --bigtableInstanceId=$INSTANCE_ID \
        --bigtableTableId=$TABLE_NAME \
        --hbaseSnapshotSourceDir=$SNAPSHOT_GCS_PATH/data \
        --snapshotName=$SNAPSHOT_NAME \
        --stagingLocation=$SNAPSHOT_GCS_PATH/staging \
        --gcpTempLocation=$SNAPSHOT_GCS_PATH/temp \
        --maxNumWorkers=$(expr 3 \* $CLUSTER_NUM_NODES) \
        --region=$REGION \
        --enableSnappy=true
    ```

### Sequence Files

1. Set the environment variables.
    ```
    PROJECT_ID=your-project-id
    INSTANCE_ID=your-instance-id
    CLUSTER_NUM_NODES=3
    CLUSTER_ZONE=us-central1-a
    TABLE_NAME=your-table-name
    
    BUCKET_NAME=gs://bucket-name
    ```
1. Run the import.
    ```
    java -jar bigtable-beam-import-2.3.0.jar import \
        --runner=dataflow \
        --project=$PROJECT_ID \
        --bigtableInstanceId=$INSTANCE_ID \
        --bigtableTableId=$TABLE_NAME \
        --sourcePattern=$BUCKET_NAME/hbase-export/part-* \
        --tempLocation=$BUCKET_NAME/hbase_temp \
        --maxNumWorkers=$(expr 3 \* $CLUSTER_NUM_NODES)  \
        --zone=$CLUSTER_ZONE \
        --region=$REGION
    ```

## Validating data

Once your snapshot or sequence file is imported, you should run the validator to
check if there are any rows with mismatched data.

1. Set the environment variables.
    ```
    PROJECT_ID=your-project-id
    INSTANCE_ID=your-instance-id
    TABLE_NAME=your-table-name
    REGION=us-central1
    
    SNAPSHOT_GCS_PATH="$BUCKET_NAME/hbase-migration-snap"
    ```
1. Run the sync job. It will put the results
   into `$SNAPSHOT_GCS_PATH/data-verification/output-TIMESTAMP`.
    ```
    java -jar bigtable-beam-import-2.3.0.jar sync-table  \
        --runner=dataflow \
        --project=$PROJECT_ID \
        --bigtableInstanceId=$INSTANCE_ID \
        --bigtableTableId=$TABLE_NAME \
        --outputPrefix=$SNAPSHOT_GCS_PATH/sync-table/output-${date +"%s"} \
        --stagingLocation=$SNAPSHOT_GCS_PATH/sync-table/staging \
        --hashTableOutputDir=$SNAPSHOT_GCS_PATH/hashtable \
        --tempLocation=$SNAPSHOT_GCS_PATH/sync-table/dataflow-test/temp \
        --region=$REGION
    ```

## Tracking lineage

CloudBigtableIO supports data lineage for Dataflow
jobs. [Data lineage](https://cloud.google.com/dataplex/docs/about-data-lineage)
is a
[Dataplex](https://cloud.google.com/dataplex/docs/introduction) feature that
lets you track how data moves through your systems. In
order to begin
automatically tracking lineage
information [enable the Lineage API](https://cloud.google.com/dataplex/docs/use-lineage#enable-apis)
in the project where the Dataflow job
is running and the project where you view lineage in the Dataplex web interface.
In addition, you must enable lineage during Dataflow job creation by providing
the service option
`--enable_lineage`.


[//]: # ({x-version-update-end})
