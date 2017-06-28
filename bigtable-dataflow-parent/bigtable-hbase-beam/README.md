# Cloud Bigtable + Beam 

This project integrates Cloud Bigtable with Beam using the HBase API.

## Writing
This connector allows you to write data to Cloud Bigtable through Beam with the HBase API.

### Example
Here's an example of a simple pipeline that writes to Cloud Bigtable.  Here are the steps for the pipeline:
1. Add the words "Hello" and "World" to the pipeline.
2. Converts the words to HBase Put objects.
3. Sends the Puts to Cloud Bigtable.

```java
// Create a DoFn that creates a Put or Delete.  MUTATION_TRANSFORM is a simplistic example.
static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
  @Override
  public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
    c.output(new Put(c.element().getBytes()).addColumn(FAMILY, QUALIFIER, VALUE));
  }
};

public static void main(String[] args) {
  // Common properties
  String projectId = "Enter a project Id";

 // Cloud Bigtable properties
  String instanceId = "Enter an instance Id";
  String tableId = "Enter a table Id";

  // Dataflow properties
  String zoneId = "Enter a zone Id, preferably the zone of your Cloud Bigtable cluster";
  String dataflowGcsBucket = "gs://enter-your-bucket/and-optional-directory";

  DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
  options.setProject(projectId);
  options.setZone(zoneId);
  options.setStagingLocation(dataflowGcsBucket + "/stage");
  options.setTempLocation(dataflowGcsBucket + "/temp");
  options.setRunner(DataflowRunner.class);

  Pipeline p = Pipeline.create(options);
 
  p
     .apply(Create.of("Hello", "World"))
     .apply(ParDo.of(MUTATION_TRANSFORM))
     .apply(BigtableHBaseIO.write(projectId, instanceId, tableId));

  p.run().waitUntilFinish();
}
```

## Reading

A [Source](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/io/Source.html) is a Beam construct that can read data for use as part of a Pipeline.

Here's an example that uses the [Source to count the rows](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/blob/master/java/dataflow-connector-examples/src/main/java/com/google/cloud/bigtable/dataflow/example/SourceRowCount.java) of a Table:

```java
DoFn<Long, String> stringifier = new DoFn<Long, String>() {
  @Override
  public void processElement(DoFn<Long, String>.ProcessContext context) throws Exception {
    context.output(context.element().toString());
  }
};

// Common properties
String projectId = "Enter a project Id";

 // Cloud Bigtable properties
String instanceId = "Enter an instance Id";
String tableId = "Enter a table Id";

// See the hbase hadoop job at
// https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/mapreduce/RowCounter.java#L151
// for more ways to configure this scan.
Scan scan = new Scan();
scan.setFilter(new FirstKeyOnlyFilter());

// Dataflow properties
String zoneId = "Enter a zone Id, preferably the zone of your Cloud Bigtable cluster";
String dataflowGcsBucket = "gs://enter-your-bucket/and-optional-directory";

DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
options.setProject(projectId);
options.setZone(zoneId);
options.setStagingLocation(dataflowGcsBucket + "/stage");
options.setTempLocation(dataflowGcsBucket + "/temp");
options.setRunner(DataflowRunner.class);

Pipeline p = Pipeline.create(options);

p
   .apply(BigtableHBaseIO.read(projectId, instanceId, tableId).withScan(scan))
   .apply(Count.<Result>globally())
   .apply(ParDo.of(stringifier))
   .apply(TextIO.Write.to(dataflowGcsBucket + "/output"));

p.run().waitUntilFinish();

// Once this is done, you can get the result file via "gsutil cp <resultLocation>-00000-of-00001"
```

