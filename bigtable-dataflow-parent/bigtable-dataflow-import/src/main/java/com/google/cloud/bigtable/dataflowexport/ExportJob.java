package com.google.cloud.bigtable.dataflowexport;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.dataflowimport.HBaseImportIO;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumLongFn;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Joiner;
import java.nio.charset.CharacterCodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.bigtable.dataflowexport.ExportJob \
-Dexec.args="--runner=BlockingDataflowPipelineRunner \
--project=igorbernstein-dev \
--stagingLocation=gs://igorbernstein-dev/dataflow-staging \
--zone=us-east1-c \
--diskSizeGb=20 \
--bigtableInstance=instance1 \
--bigtableTable=table2 \
--destination=gs://igorbernstein-dev/export51"
 */

public class ExportJob {
  private static final Logger LOG = LoggerFactory.getLogger(ExportJob.class);

  public static void main(String[] args) throws CharacterCodingException {
    PipelineOptionsFactory.register(ExportOptions.class);

    DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
        .as(DataflowPipelineOptions.class);

    ExportOptions exportOptions = PipelineOptionsFactory.fromArgs(args)
        .as(ExportOptions.class);

    // Configure Bigtable scan parameters
    Scan scan = new Scan()
        .setStartRow(exportOptions.getStartRow().getBytes())
        .setStopRow(exportOptions.getStopRow().getBytes())
        .setMaxVersions(exportOptions.getMaxVersions());

    if (!exportOptions.getFilter().isEmpty()) {
      scan.setFilter(new ParseFilter().parseFilterString(exportOptions.getFilter()));
    }

    String bigtableProject = exportOptions.getBigtableProject();
    if (bigtableProject.isEmpty()) {
      bigtableProject = options.getProject();
    }
    CloudBigtableScanConfiguration scanConfig = new CloudBigtableScanConfiguration.Builder()
        .withProjectId(bigtableProject)
        .withInstanceId(exportOptions.getBigtableInstance())
        .withTableId(exportOptions.getBigtableTable())
        .withScan(scan)
        .build();

    Pipeline pipeline = new Builder()
        .withDataflowOptions(options)
        .withScanConfig(scanConfig)
        .withDestination(exportOptions.getDestination())
        .build();

    pipeline.run();
  }

  interface ExportOptions extends PipelineOptions {
    @Description("Bigtable project (Defaults to the dataflow project)")
    @Default.String("")
    String getBigtableProject();
    void setBigtableProject(String project);

    @Description("Bigtable instance name")
    String getBigtableInstance();
    void setBigtableInstance(String instance);

    @Description("Bigtable table name")
    String getBigtableTable();
    void setBigtableTable(String table);

    @Description("The row where to start the export from")
    @Default.String("")
    String getStartRow();
    void setStartRow(String startRow);

    @Description("The row where to stop the export")
    @Default.String("")
    String getStopRow();
    void setStopRow(String stopRow);

    @Description("Maximum number of cell versions")
    @Default.Integer(Integer.MAX_VALUE)
    int getMaxVersions();
    void setMaxVersions(int maxVersions);

    @Description("Filter string. See: http://hbase.apache.org/0.94/book/thrift.html#thrift.filter-language")
    @Default.String("")
    String getFilter();
    void setFilter(String filter);

    @Description("Destination path on GCS")
    String getDestination();
    void setDestination(String destination);
  }

  public static class Builder {
    private DataflowPipelineOptions dataflowOptions;
    private CloudBigtableScanConfiguration scanConfig;
    private String destination;
    private long desiredShardSize = 1024 * 1024 * 1024;

    public Builder withDataflowOptions(DataflowPipelineOptions dataflowOptions) {
      this.dataflowOptions = dataflowOptions;
      return this;
    }
    public Builder withScanConfig(CloudBigtableScanConfiguration scanConfig) {
      this.scanConfig = scanConfig;
      return this;
    }
    public Builder withDestination(String destination) {
      this.destination = destination;
      return this;
    }
    public Builder withDesiredShardSize(long desiredShardSize) {
      this.desiredShardSize = desiredShardSize;
      return this;
    }

    public Pipeline build() {
      checkNotNull(scanConfig, "scanConfig must be set");
      checkNotNull(dataflowOptions, "dataflowOptions must be set");
      checkNotNull(destination, "destination must be set");
      checkState(destination.startsWith("gs://"), "destination must begin with gs://");

      BoundedSource<Result> source = CloudBigtableIO.read(scanConfig);
      Pipeline pipeline = Pipeline.create(dataflowOptions);

      // Coders
      SerializableCoder<BoundedSource<Result>> sourceCoder = SerializableCoder.of(new TypeDescriptor<BoundedSource<Result>>() {});
      KvCoder<String, BoundedSource<Result>> kvCoder = KvCoder.of(StringUtf8Coder.of(), sourceCoder);

      // Pipeline
      pipeline
          // Initialize with the source as the only element.
          // Note that we aren't reading the source's element
          .apply("Initialize", Create.of(source).withCoder(sourceCoder))
          // Shard the source into a bunch of pieces
          // Note that we aren't reading the source's element
          .apply("Shard", ParDo.of(new ShardScans(desiredShardSize))).setCoder(kvCoder)
          // Make sure that each shard can be handled a different worker
          .apply("Fanout", GroupByKey.<String, BoundedSource<Result>>create())
          // Now, read the actual rows and write out the files
          .apply("Write", ParDo.of(new WriteResultsToSeq(destination)));

      return pipeline;
    }
  }

  static class ShardScans extends DoFn<BoundedSource<Result>, KV<String, BoundedSource<Result>>> {
    private final long desiredShardSize;

    public ShardScans(long desiredShardSize) {
      this.desiredShardSize = desiredShardSize;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      BoundedSource<Result> source = c.element();

      List<? extends BoundedSource<Result>> boundedSources = source
          .splitIntoBundles(desiredShardSize, c.getPipelineOptions());

      LOG.info("Split into {} shards", boundedSources.size());

      // randomize order to make sure that scans are distributed across tablets
      Collections.shuffle(boundedSources);


      for (BoundedSource<Result> sourceBundle : boundedSources) {
        c.output(KV.of(UUID.randomUUID().toString(), sourceBundle));
      }
    }
  }

  static class WriteResultsToSeq extends DoFn<KV<String, Iterable<BoundedSource<Result>>>, Void> {
    private final String basePath;
    private final Aggregator<Long, Long> itemCounter;
    private final Aggregator<Long, Long> readDuration;
    private final Aggregator<Long, Long> writeDuration;
    private final Aggregator<Long, Long> totalDuration;
    private final Aggregator<Long, Long> bytesWritten;

    private transient GcsUtil gcsUtil;
    private transient Configuration hadoopConfig;


    public WriteResultsToSeq(String basePath) {
      this.basePath = basePath;

      itemCounter = createAggregator("itemsProcessed", new SumLongFn());
      readDuration = createAggregator("readDuration", new SumLongFn());
      writeDuration = createAggregator("writeDuration", new SumLongFn());
      totalDuration = createAggregator("totalDuration", new SumLongFn());
      bytesWritten = createAggregator("bytesWritten", new SumLongFn());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
    }

    @Override
    public void startBundle(Context c) throws Exception {
      gcsUtil = new GcsUtil.GcsUtilFactory().create(c.getPipelineOptions());

      hadoopConfig = new Configuration(false);

      // Not using setStrings to avoid loading StringUtils which tries to access HADOOP_HOME
      hadoopConfig.set("io.serializations", Joiner.on(',').join(
          WritableSerialization.class.getName(), ResultSerialization.class.getName())
      );
      hadoopConfig.set("fs.gs.project.id", c.getPipelineOptions().as(DataflowPipelineOptions.class).getProject());
      for (Map.Entry<String, String> entry : HBaseImportIO.CONST_FILE_READER_PROPERTIES.entrySet()) {
        hadoopConfig.set(entry.getKey(), entry.getValue());
      }

      super.startBundle(c);
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      final String shardId = c.element().getKey();
      final BoundedSource<Result> source = c.element().getValue().iterator().next();

      LOG.info("Starting new shard");

      final String tempFilePath = basePath + "/_temporary/" + shardId;
      final String finalFilePath = basePath + "/" + shardId;

      final SequenceFile.Writer.Option[] writerOptions = new SequenceFile.Writer.Option[]{
          SequenceFile.Writer.file(new Path(tempFilePath)),
          SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
          SequenceFile.Writer.valueClass(Result.class),
          SequenceFile.Writer.compression(CompressionType.BLOCK)
      };

      final long start = System.nanoTime();
      long localStart = System.nanoTime();

      long reportedTotalDuration = 0;

      try (BoundedSource.BoundedReader<Result> reader = source.createReader(c.getPipelineOptions())) {
        boolean hasMore = reader.start();
        readDuration.addValue(System.nanoTime() - localStart);

        if (hasMore) {
          try (SequenceFile.Writer writer = SequenceFile.createWriter(hadoopConfig, writerOptions)) {
            final ImmutableBytesWritable key = new ImmutableBytesWritable();
            Result value;

            while (hasMore) {
              localStart = System.nanoTime();
              value = reader.getCurrent();
              key.set(value.getRow());
              writer.append(key, value);
              writeDuration.addValue(System.nanoTime() - localStart);

              localStart = System.nanoTime();
              hasMore = reader.advance();
              readDuration.addValue(System.nanoTime() - localStart);

              itemCounter.addValue(1L);
              final long newTotalDuration = System.nanoTime() - start;
              totalDuration.addValue(newTotalDuration - reportedTotalDuration);
              reportedTotalDuration = newTotalDuration;
            }

            bytesWritten.addValue(writer.getLength());
          }
        }
      }
      final long newTotalDuration = System.nanoTime() - start;
      totalDuration.addValue(newTotalDuration - reportedTotalDuration);

      // finalize
      gcsUtil.copy(Collections.singletonList(tempFilePath), Collections.singletonList(finalFilePath));
      gcsUtil.remove(Collections.singletonList(tempFilePath));
    }
  }
}
