# HBase Sequence Files to Cloud Bigtable Over Dataflow

This project supports importing HBase Sequence Files to Google Cloud Bigtable using
Cloud Dataflow.

## Code Sample

The following code may be used to import a HBase Sequence Files into a bigtable:

```
  public static void main(String[] args) throws Exception {
    HBaseImportOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(HBaseImportOptions.class);
    Pipeline p = CloudBigtableIO.initializeForWrite(Pipeline.create(options));
    p
        .apply("ReadSequenceFile", Read.from(HBaseImportIO.createSource(options)))
        .apply("ConvertResultToMutations", HBaseImportIO.transformToMutations())
        .apply("WriteToTable", CloudBigtableIO.writeToTable(
            CloudBigtableTableConfiguration.fromCBTOptions(options)));
    p.run();
  }
```

## Command Line Arguments

The code sample in the previous section may be run with the following command line arguments:

```
    -Xbootclasspath/p:${ALPN-JAR} \
    --runner=... \
    --stagingLocation=... \
    --bigtableProjectId=... \
    --bigtableClusterId=... \
    --bigtableZoneId=... \
    --bigtableTableId=... \
    --filePattern=... \
    --HBase094DataFormat=...
```

### ALPN

Follow the [instructions here](https://cloud.google.com/bigtable/docs/installing-hbase-shell#alpn)
to find out the correct version of alpn for your JRE, then download alpn-boot-${ALPN_VERSION}.jar
 from the [maven repository](http://central.maven.org/maven2/org/mortbay/jetty/alpn/alpn-boot).
Replace ${ALPN-JAR} above with the path of the downloaded jar.

### More on Arguments

+ runner: sepcifies where dataflow job is run. If the input files are on local file system,
    use "DirectPipelineRunner". If the input files are on google cloud,
    "BlockingDataflowPipelineRunner" will lauch the job and waits for its completion.

+ stagingLocation: specifies the GCS location where run-files for the dataflow job should be
    uploaded to.

+ bigtableProjectId: id of the bigtable project that has the target table. 

+ bigtableClusterId: cluser id of the bigtable project.

+ bigtableZoneId: zone id of the bigtable project.

+ filePattern: specifies the location of the input file(s). This may be either the path to a
    directory (e.g., gs://mybucket/my-hbase-sequence-file-foler/) or files (e.g., /tmp/part-m*)

+ HBase094DataFormat: optional argument. If input files were exported in HBase 0.94 or earlier,
    set this argument to 'true' so that the correct deserializer may be chosen.
    The default is 'false'.


