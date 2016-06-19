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

+ project: id of the cloud project with which to run dataflow jobs.

+ runner: specifies where dataflow job is run. If the input files are on local file system,
    use "DirectPipelineRunner". If the input files are on google cloud,
    "BlockingDataflowPipelineRunner" will lauch the job and waits for its completion.

+ stagingLocation: specifies the GCS location where run-files for the dataflow job should be
    uploaded to.

+ bigtableProjectId: id of the bigtable project that has the target table. 

+ bigtableClusterId: cluster id of the bigtable project.

+ bigtableZoneId: zone id of the bigtable project.

+ filePattern: specifies the location of the input file(s). This may be either the path to a
    directory (e.g., gs://mybucket/my-hbase-sequence-file-foler/) or files (e.g., /tmp/part-m*)

+ HBase094DataFormat: optional argument. If input files were exported in HBase 0.94 or earlier,
    set this argument to 'true' so that the correct deserializer may be chosen.
    The default is 'false'.

### Delete Markers

Delete markers are special HBase cells that are included only in raw scan results. They mark certain
regular cell(s) for deletion and exclude them from regular scans. An HBase sequence file may contain
delete markers if the 'hbase.mapreduce.include.deleted.rows' property was set to true in the
export job, either by command-line argument or in core-site.xml file.

The following example is an HBase table with delete markers:

```
hbase(main):051:0> scan 'delete-markers', {RAW=>true,VERSIONS=>100}
ROW                           COLUMN+CELL
 r1                           column=cf1:c1, timestamp=1050, value=1
 r1                           column=cf1:c1, timestamp=1045, type=Delete
 r1                           column=cf1:c1, timestamp=1040, type=Delete
 r1                           column=cf1:c1, timestamp=1040, value=1
 r1                           column=cf1:c1, timestamp=1030, value=1
 r1                           column=cf1:c1, timestamp=1020, value=1
 r1                           column=cf1:c1, timestamp=1010, value=1
 r1                           column=cf1:c1, timestamp=1000, type=DeleteColumn
 r1                           column=cf1:c1, timestamp=1000, value=1
 r1                           column=cf1:c1, timestamp=990, value=1
 r1                           column=cf1:c1, timestamp=980, value=1
 r1                           column=cf1:c1, timestamp=970, value=1
 r1                           column=cf1:c2, timestamp=100, value=1
 r1                           column=cf2:, timestamp=1040, type=DeleteFamilyVersion
 r1                           column=cf2:, timestamp=1000, type=DeleteFamily
 r1                           column=cf2:c1, timestamp=1050, value=1
 r1                           column=cf2:c1, timestamp=1040, value=1
 ...
```

There are four types of Delete Markers:

Type     | How to create in a Delete     | Description |
---------|-------------------------------------|------|
Delete   | Delete.addColumn(family, qualifier), Delete.addColumn(family, qualifier, timestamp) | Delete a cell with specific timestamp
DeleteColumn | Delete.addColumns(family, qualifier),  Delete.addColumns(family, qualifier, timestamp) | Delete all cells with given qualifier in a family whose timestamp <= given timestamp |
DeleteFamilyVersion | Delete.addFamilyVersion(family, timestamp) | Delete all cells in a family whose timestamp <= given timestamp, regardless of qualifier
DeleteFamily | Delete.addFamily(family), Delete.addFamily(family, timestamp) | Delete all cells in the row, whose timestamp <= given timestamp, regardless of family or qualifier
---------|-------------------------------------|------|

Note:

+ Of the delete markers in the example output above:
  ++ The first type=Delete marker refers to an non-existing cell (timestamp=1045) and has no effect
  ++ The second type=Delete marker deletes the cell cf1:c1 with timestmap=1040
  ++ The type=DeleteColumn marker deletes the 4 cf1:c1 cells with timestamps 970, 980, 990,
     and 1000.
  ++ The type=DeleteFamilyVersion marker deletes the cell cf2:c1 with timestamp=1040
  ++ The type=DeleteFamily marker would delete any cells in column family cf2 with timestamp <= 1000
     if the table contains any.
+ For all methods where timestamp is not given, a timestamp is assigned by HBase. Please refer to
  HBase documentation for details.
+ As of HBase 1.1.2, if an HBase table with DeleteFamilyVersion marker is exported as sequence
  file and the raw scan property (hbase.mapreduce.include.deleted.rows) is true, the export would
  fail with an unrecognized-type error. Therefore existing sequence files do not contain this
  marker, although future files may.

Unlike HBase, Cloud Bigtable does not expose Delete Markers, so to the user all deletes take effect
instantly. As a result, there is no use importing delete markers and deleted cells into bigtable.

We recommend that user make sure the the raw scan property (hbase.mapreduce.include.deleted.rows)
is set to false when exporting HBase tables so that delete markers and delete cells are excluded
from the export. If delete markers are encountered by this importer, this tool will remove them
as well as all regular cells marked as deleted by them from the input, and only write the
remaining cells to bigtable.
