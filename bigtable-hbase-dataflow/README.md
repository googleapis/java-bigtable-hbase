# Cloud Bigtable + Cloud Dataflow

This project is an early release of Cloud Bigtable + Cloud Dataflow integration. This implementation
using the HBase classes for the integration.  There are plans foran additional integration based on
a new Java oriented API for Cloud Bigtable.

## Writing
A [PTransform](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/PTransform)
is a Dataflow construct that operates on distributed input - A [PCollection]https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/values/PCollection.  See the [Reading and Writing Data](https://cloud.google.com/dataflow/model/reading-and-writing-data) section in the Google Dataflow documentation

Here's [an example](https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/master/bigtable-hbase-dataflow/src/main/java/com/google/cloud/bigtable/dataflow/example/HelloWorldWrite.java) of the CloudbigtableIO.writeToTable() that writes the words "Hello" and "World" to a Bigtable Table:

```java
  // Create a DoFn that creates a Put or Delete.  MUTATION_TRANSFORM is a simplistic example.
  static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
    @Override
    public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
      c.output(new Put(c.element().getBytes()).addColumn(FAMILY, QUALIFIER, VALUE));
    }
  };

  public static void main(String[] args) {
    // CloudBigtableOptions is one way to retrieve the options.  It's not required to use this
    // specific PipelineOptions extension; CloudBigtableOptions is there as a convenience.
    CloudBigtableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CloudBigtableOptions.class);

    // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to
    CloudBigtableTableConfiguration config = CloudBigtableTableConfiguration.fromCBTOptions(options);

    Pipeline p = Pipeline.create(options);
    // This sets up serialization for Puts and Deletes so that Dataflow can potentially move them through
    // the network
    CloudBigtableIO.initializeForWrite(p);

    p
       .apply(Create.of("Hello", "World"))
       .apply(ParDo.of(MUTATION_TRANSFORM))
       .apply(CloudBigtableIO.writeToTable(config));

    p.run();
```

To run the example via maven, use the following command, with the values in the brackets replaced with your information:

`mvn exec:java -Dexec.mainClass="com.google.cloud.bigtable.dataflow.example.HelloWorldWrite" -Dexec.args="--runner=BlockingDataflowPipelineRunner --project=[project_id] --stagingLocation=gs://[some_bucket] --bigtableProjectId=[project_id] --bigtableClusterId=[cluster_id] --bigtableZoneId=[zone_id] --bigtableTableId=[table_id]" -Dexec.classpathScope="test"`

## Reading

A [Source](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/Source) is a Dataflow construct that can read data for use as part of a Pipeline.  The Bigtable Source is not fully optimized for use in Dataflow, but is (or at least should be) functional for your use.  It will be improved when issue #400 is resolved.

Here's an example that uses the [Source to count the rows](https://github.com/GoogleCloudPlatform/cloud-bigtable-cdf/blob/master/bigtable-dataflow-example/src/main/java/com/google/cloud/dataflow/sdk/io/bigtable/example/SourceRowCount.java) of a Table:

```java

    DoFn<Long, String> stringifier = new DoFn<Long, String>() {
      @Override
      public void processElement(DoFn<Long, String>.ProcessContext context) throws Exception {
        context.output(context.element().toString());
      }
    };

    CountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CountOptions.class);

    // See the hbase hadoop job at
    // https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/mapreduce/RowCounter.java#L151
    // for more ways to configure this scan.
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());

    // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
    // You can supply an optional Scan() to filter the rows that will be read.
    CloudBigtableScanConfiguration config = CloudBigtableScanConfiguration.fromCBTOptions(options);

    Pipeline p = Pipeline.create(options);

    p
       .apply(Read.from(CloudBigtableIO.read(config)))
       .apply(Count.<Result>globally())
       .apply(ParDo.of(stringifier))
       .apply(TextIO.Write.to(options.getResultLocation()));

    p.run();

    // Once this is done, you can get the result file via "gsutil cp <resultLocation>-00000-of-00001"
```

This can be run this example with the *com.google.cloud.bigtable.dataflow.example.SourceRowCount*
class with the following parameters:

`--runner=BlockingDataflowPipelineRunner --project=[project_id] --stagingLocation=gs://[some_bucket] --bigtableProjectId=[project_id] --bigtableClusterId=[cluster_id] --bigtableZoneId=[zone_id] --bigtableTableId=[table_id]"`

This will also require setting a classpath argument for ALPN.

`-Xbootclasspath=-Xbootclasspath/p:{Path to your maven repository}/org/mortbay/jetty/alpn/alpn-boot/{alpn.version}/{alpn.version}.jar`


