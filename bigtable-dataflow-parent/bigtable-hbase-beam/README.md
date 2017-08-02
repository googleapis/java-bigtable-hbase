# Cloud Bigtable + Beam 

This project integrates Cloud Bigtable with Beam using the HBase API.

## Maven
To use the connector, add the following configuraiton to your maven pom.xml file:

```xml
 <dependency>
     <groupId>com.google.cloud.bigtable</groupId>
     <artifactId>bigtable-hbase-beam</artifactId>
     <version>0.9.8-SNAPSHOT/version>
 </dependency>

 <dependency>
     <groupId>io.netty</groupId>
     <artifactId>netty-tcnative-boringssl-static</artifactId>
     <version>1.1.33.Fork26</version>
 </dependency>
```

## Writing
BigtableHBaseIO.write(...) allows you to write data to Cloud Bigtable through Beam with the HBase API.

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
BigtableHBaseIO.read(...) allows you to read data from Cloud Bigtable.

### Example
This example reads all the records from Cloud Bigtable and returns a count of the rows.

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

// Once this is done, you can get the result file via "gsutil cp ${dataflowGcsBucket}/oputput-00000-of-00001"
```

