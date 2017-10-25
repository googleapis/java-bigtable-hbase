# HBase Sequence Files to Cloud Bigtable Over Dataflow

NOTE: Please use bigtable-beam-import instead.  Instructions can be found [here](https://github.com/GoogleCloudPlatform/cloud-bigtable-client/tree/master/bigtable-dataflow-parent/bigtable-beam-import).

This project supports importing and exporting HBase Sequence Files to Google Cloud Bigtable using
Cloud Dataflow.

## Instructions

Download [the import/export jar](http://search.maven.org/remotecontent?filepath=com/google/cloud/bigtable/bigtable-dataflow-import/1.0.0-pre3/bigtable-dataflow-import-1.0.0-pre3-shaded.jar), which is an aggregation of all required jars.

## Export

On the command line:

```
java -jar bigtable-dataflow-import-1.0.0-pre3-shaded.jar export \
    --runner=BlockingDataflowPipelineRunner \
    --bigtableProjectId=[your_project_id] \
    --bigtableInstanceId=[your_instance_id] \
    --bigtableTableId=[your_table_id] \
    --destination=gs://[bucket_name]/[export_directory] \
    --project=[your_project_id] \
    --stagingLocation=gs://[bucket_name]/[jar_staging_directory] \
    --maxNumWorkers=[5x number of nodes] \
    --zone=[zone of your cluster]
```

## Import

Create the table in your cluster.

On the command line:

```
java -jar bigtable-dataflow-import-1.0.0-pre3-shaded.jar import \
    --runner=BlockingDataflowPipelineRunner \
    --bigtableProjectId=[your_project_id] \
    --bigtableInstanceId=[your_instance_id] \
    --bigtableTableId=[your_table_id] \
    --filePattern='gs://[bucket_name]/[import_directory]/*' \
    --project=[your_project_id] \
    --stagingLocation=gs://[bucket_name]/[jar_staging_directory] \
    --maxNumWorkers=[5x number of nodes] \
    --zone=[zone of your cluster]
```
