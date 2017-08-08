# Cloud Bigtable + Beam

This project integrates Cloud Bigtable with Beam using the HBase API.

## Writing
This connector allows you to write data to Bigtable through Beam.

```java
// Create a DoFn that creates a Put or Delete.  MUTATION_TRANSFORM is a simplistic example.
static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    c.output(new Put(c.element().getBytes()).addColumn(FAMILY, QUALIFIER, VALUE));
  }
};

public static void main(String[] args) {
  DataflowPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);

  CloudBigtableTableConfiguration config = new CloudBigtableTableConfiguration.Builder()
    .withProjectId(projectId)
    .withInstanceId(instanceId)
    .withTableId(tableId)
    .build();

  Pipeline p = Pipeline.create(options);

  p
     .apply(Create.of("Hello", "World"))
     .apply(ParDo.of(MUTATION_TRANSFORM))
     .apply(CloudBigtableIO.writeToTable(config));

  p.run();
}
```

## Reading

A [Source](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/io/Source.html) is a Beam construct that can read data for use as part of a Pipeline.

Here's an example that uses the [Source to count the rows](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/blob/master/java/dataflow-connector-examples/src/main/java/com/google/cloud/bigtable/dataflow/example/SourceRowCount.java) of a Table:

```java
DoFn<Long, String> stringifier = new DoFn<Long, String>() {
  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    context.output(context.element().toString());
  }
};

DataflowPipelineOptions options =
    PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);

// See the hbase hadoop job at
// https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/mapreduce/RowCounter.java#L151
// for more ways to configure this scan.
Scan scan = new Scan();
scan.setFilter(new FirstKeyOnlyFilter());

CloudBigtableScanConfiguration config = new CloudBigtableTableConfiguration.Builder()
  .withProjectId(projectId)
  .withInstanceId(instanceId)
  .withTableId(tableId)
  .withScan(scan)
  .build();

Pipeline p = Pipeline.create(options);

p
   .apply(Read.from(CloudBigtableIO.read(config)))
   .apply(Count.<Result>globally())
   .apply(ParDo.of(stringifier))
   .apply(TextIO.Write.to("gs://<some bucket and directory>");

p.run();

// Once this is done, you can get the result file via "gsutil cp <resultLocation>-00000-of-00001"
```

