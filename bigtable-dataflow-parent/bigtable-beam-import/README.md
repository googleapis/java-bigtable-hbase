# HBase Sequence Files to Cloud Bigtable using Beam

This project supports importing and exporting HBase Sequence Files to Google Cloud Bigtable using
Cloud Dataflow.

## Instructions

Download [the import/export jar](http://search.maven.org/remotecontent?filepath=com/google/cloud/bigtable/bigtable-beam-import/1.1.0/bigtable-beam-import-1.1.0-shaded.jar), which is an aggregation of all required jars.

Please pay attention to the Cluster CPU usage and adjust the number of Dataflow workers accordingly.

## Export

On the command line:

```
java -jar bigtable-beam-import-1.1.0-shaded.jar export \
    --runner=dataflow \
    --project=[your_project_id] \
    --bigtableInstanceId=[your_instance_id] \
    --bigtableTableId=[your_table_id] \
    --destinationPath=gs://[bucket_name]/[export_directory]/ \
    --tempLocation=gs://[bucket_name]/[temp_work_directory]/ \
    --maxNumWorkers=[10x number of nodes] \
    --zone=[zone of your cluster]
```

## Import

Create the table in your cluster.

On the command line:

```
java -jar bigtable-beam-import-1.1.0-shaded.jar import \
    --runner=dataflow \
    --project=[your_project_id] \
    --bigtableInstanceId=[your_instance_id] \
    --bigtableTableId=[your_table_id] \
    --sourcePattern='gs://[bucket_name]/[import_directory]/part-*' \
    --tempLocation=gs://[bucket_name]/[temp_work_directory] \
    --maxNumWorkers=[3x number of nodes] \
    --zone=[zone of your cluster]
```
