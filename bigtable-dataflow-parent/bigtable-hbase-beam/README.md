# Cloud Bigtable + Beam 

This project integrates Cloud Bigtable with Beam using the HBase API.

## Writing
This connector allows you to write data to Bigtable through Beam.

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

  // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
  CloudBigtableTableConfiguration config = CloudBigtableTableConfiguration.fromCBTOptions(options);

  Pipeline p = Pipeline.create(options);
  // This sets up serialization for Puts and Deletes so that Dataflow can potentially move them through
  // the network.
  CloudBigtableIO.initializeForWrite(p);

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
CloudBigtableScanConfiguration config = CloudBigtableScanConfiguration.fromCBTOptions(options, scan);

Pipeline p = Pipeline.create(options);

p
   .apply(Read.from(CloudBigtableIO.read(config)))
   .apply(Count.<Result>globally())
   .apply(ParDo.of(stringifier))
   .apply(TextIO.Write.to(options.getResultLocation()));

p.run();

// Once this is done, you can get the result file via "gsutil cp <resultLocation>-00000-of-00001"
```

