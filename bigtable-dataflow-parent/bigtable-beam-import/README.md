# HBase Sequence Files to Cloud Bigtable using Beam

This folder contains tools to support importing and exporting HBase data to
Google Cloud Bigtable using Cloud Dataflow.

## Setup 

To use the tools in this folder, you can download them from the maven repository, or
you can build them using Maven. 


[//]: # ({x-version-update-start:bigtable-dataflow-parent:released})
### Download the jars
Download [the import/export jars](http://search.maven.org/remotecontent?filepath=com/google/cloud/bigtable/bigtable-beam-import/1.14.1/bigtable-beam-import-1.14.1-shaded.jar), which is an aggregation of all required jars.

### Build the jars yourself

Go to the top level directory and build the repo
then return to this sub directory.

```
cd ../../
mvn clean install -DskipTests=true
cd bigtable-dataflow-parent/bigtable-beam-import
```


## Exporting data

This folder contains pipelines to help export data via snapshots or into sequence files
and can be used for HBase or Bigtable. If you're migrating your data from HBase to Bigtable
using snapshots is the preferred method. 

### Exporting snapshots from HBase

```
SNAPSHOT_NAME=your-snapshot-name
```

```
hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot $SNAPSHOT_NAME \
    -copy-to /integration-test/data -mappers 16
```

### Exporting sequence files from HBase

On your HDFS set the environment variables.

```
TABLENAME="my-new-table"
EXPORTDIR=/usr/[USERNAME]/hbase-${TABLENAME}-export
hadoop fs -mkdir -p ${EXPORTDIR}
MAXVERSIONS=2147483647
```

Then in your HBase home directory run the export commands. 

```
cd $HBASE_HOME
bin/hbase org.apache.hadoop.hbase.mapreduce.Export $TABLENAME \
    /user/hbase-$TABLENAME \
    -export $MAXVERSIONS
bin/hbase org.apache.hadoop.hbase.mapreduce.Export \
    -Dmapred.output.compress=true \
    -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -DRAW_SCAN=true \
    -Dhbase.client.scanner.caching=100 \
    -Dmapred.map.tasks.speculative.execution=false \
    -Dmapred.reduce.tasks.speculative.execution=false \
    $TABLENAME $EXPORTDIR $MAXVERSIONS

```

### Exporting snapshots from Bigtable

Exporting HBase snapshots from Bigtable is not supported.

### Exporting sequence files from Bigtable

Set the environment variables.

```
PROJECT_ID=your-project-id
INSTANCE_ID=your-instance-id
CLUSTER_NUM_NODES=3
TABLE_NAME=your-table-name

BUCKET_NAME=gs://bucket-name
```

Run the export.
```
java -jar bigtable-beam-import-1.14.1-shaded.jar export \
    --runner=dataflow \
    --project=$PROJECT_ID \
    --bigtableInstanceId=$INSTANCE_ID \
    --bigtableTableId=$TABLE_NAME \
    --destinationPath=$BUCKET_NAME/hbase_export/ \
    --tempLocation=$BUCKET_NAME/hbase_temp]/ \
    --maxNumWorkers=$(expr 10 \* $CLUSTER_NUM_NODES) \
```


## Importing to Bigtable

This folder contains pipelines to help import data via snapshots or sequence files
and can be used for HBase or Bigtable. Before you begin your import you must create
the tables in Bigtable via the [schema translation tool](bigtable-hbase-1.x-parent/bigtable-hbase-tools/README.md)
or using the Bigtable command line tool and running `cbt createtable your-table-name`.
Once your import is completed follow the instructions for the validator below to ensure it was successful.

Please pay attention to the Cluster CPU usage and adjust the number of Dataflow workers accordingly.

### Snapshots (preferred method)

Set the environment variables.

```
PROJECT_ID=your-project-id
INSTANCE_ID=your-instance-id
TABLE_NAME=your-table-name
REGION=us-central1

SNAPSHOT_PATH="gs://your-bucket/hbase-migration-snap"
SNAPSHOT_NAME=your-snapshot-name
```

Run the import.
```
java -jar bigtable-beam-import-1.14.1-SNAPSHOT-shaded.jar importsnapshot \
    --runner=DataflowRunner \
    --project=$PROJECT_ID \
    --bigtableInstanceId=$INSTANCE_ID \
    --bigtableTableId=$TABLE_NAME \
    --hbaseSnapshotSourceDir=$SNAPSHOT_PATH/data \
    --snapshotName=$SNAPSHOT_NAME \
    --stagingLocation=$SNAPSHOT_PATH/staging \
    --tempLocation=$SNAPSHOT_PATH/temp \
    --region=$REGION
```


### Sequence Files

Set the environment variables.

```
PROJECT_ID=your-project-id
INSTANCE_ID=your-instance-id
CLUSTER_NUM_NODES=3
CLUSTER_ZONE=us-central1-a
TABLE_NAME=your-table-name

BUCKET_NAME=gs://bucket-name
```

Run the import.
```
java -jar bigtable-beam-import-1.14.1-shaded.jar import \
    --runner=dataflow \
    --project=$PROJECT_ID \
    --bigtableInstanceId=$INSTANCE_D \
    --bigtableTableId=$TABLE_NAME \
    --sourcePattern='$BUCKET_NAME/hbase-export/part-*' \
    --tempLocation=$BUCKET_NAME/hbase_temp \
    --maxNumWorkers=$(expr 3 \* $CLUSTER_NUM_NODES)  \
    --zone=$CLUSTER_ZONE
```


## Validating data

Once your snapshot or sequence file is imported, you should run the validator to
check if there are any rows with mismatched data. 

Set the environment variables.
```
PROJECT_ID=your-project-id
INSTANCE_ID=your-instance-id
TABLE_NAME=your-table-name
REGION=us-central1

SNAPSHOT_PATH="gs://your-bucket/hbase-migration-snap"
```


Run the sync job. It will put the results into `$SNAPSHOT_PATH/schema-validator/output`,
so if you run the command multiple times, you might want to add a number to the output,
like so `$SNAPSHOT_PATH/schema-validator/output-1`. 
```
java -jar bigtable-beam-import-1.14.1-SNAPSHOT-shaded.jar sync-table  \
    --runner=dataflow \
    --project=$PROJECT_ID \
    --bigtableInstanceId=$INSTANCE_D \
    --bigtableTableId=$TABLE_NAME \
    --outputPrefix=$SNAPSHOT_PATH/schema-validator/output \
    --stagingLocation=$SNAPSHOT_PATH/staging \
    --hashTableOutputDir=$SNAPSHOT_PATH/hashtable \
    --tempLocation=$SNAPSHOT_PATH/dataflow-test/temp \
    --region=$REGION
```


[//]: # ({x-version-update-end})
