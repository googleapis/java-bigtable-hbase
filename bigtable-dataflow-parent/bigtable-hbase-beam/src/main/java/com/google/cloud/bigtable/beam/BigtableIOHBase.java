package com.google.cloud.bigtable.beam;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseMutationAdapter;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.util.ZeroCopyByteStringUtil;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

/**
 * Utilities to configure {@link BigtableIO} with HBase artifacts
 */
@Experimental
public class BigtableIOHBase {

  public static Read read(String projectId, String instanceId, String tableId) {
    return new Read(projectId, instanceId, tableId);
  }

  public static Write write(String projectId, String instanceId, String tableId) {
    return new Write(projectId, instanceId, tableId);
  }

  private BigtableIOHBase() {}

  /**
   * This is a wrapper around {@link org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.Read} that adds
   * HBase semantics
   */
  public static class Read extends PTransform<PBegin, PCollection<Result>> {
    private static final long serialVersionUID = 1L;

    private static final DoFn<Row, Result> ROW_TO_RESULT_TRANSFORM = new DoFn<Row, Result>() {
      private static final long serialVersionUID = 1L;
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        c.output(Adapters.ROW_ADAPTER.adaptResponse(c.element()));
      }
    };

    private final SerializableConfiguration serializableConfiguration;
    private final String tableId;
    private final SerializableScan scan = new SerializableScan(new Scan());

    private Read(String projectId, String instanceId, String tableId) {
      Preconditions.checkNotNull(projectId, "Please configure a projectId.");
      Preconditions.checkNotNull(instanceId, "Please configure a instanceId.");
      Preconditions.checkNotNull(tableId, "Please configure a tableId.");
      this.serializableConfiguration =
          new SerializableConfiguration(BigtableConfiguration.configure(projectId, instanceId));
      this.tableId = tableId;
    }

    /**
     * Add an extended configuration option.  See {@link BigtableOptionsFactory} for more details.
     */
    public Read withConfiguration(String key, String value) {
      serializableConfiguration.get().set(key, value);
      return this;
    }

    /**
     * Attach a {@link RowFilter} and potentially a {@link ByteKeyRange} to a {@link Read}.
     */
    public Read withScan(Scan scan) {
      this.scan.set(scan);
      return this;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.add(DisplayData.item("configuration",
                serializableConfiguration.get().toString()));
        builder.add(DisplayData.item("tableId", tableId));
        builder.addIfNotNull(DisplayData.item("scan", scan.get().toString()));
    }

    @Override
    public PCollection<Result> expand(PBegin input) {
      RowFilter filter = Adapters.SCAN_ADAPTER.adapt(scan.get(), new DefaultReadHooks()).getFilter();
      byte[] startRow = scan.get().getStartRow();
      byte[] stopRow = scan.get().getStopRow();
      ByteKeyRange keyRange = ByteKeyRange.ALL_KEYS;
      if (startRow.length > 0){
        keyRange   = keyRange.withStartKey(ByteKey.copyFrom(startRow));
      }
      if (stopRow.length > 0){
        keyRange = keyRange.withEndKey(ByteKey.copyFrom(stopRow));
      }

      BigtableOptions options =
          BigtableOptionsFactory.fromConfiguration(serializableConfiguration.get());
      BigtableIO.Read bigtableRead =
          BigtableIO.read()
              .withBigtableOptions(options)
              .withKeyRange(keyRange)
              .withRowFilter(filter)
              .withTableId(tableId);
      return input.getPipeline()
          .apply("BigtableIO.Read", bigtableRead)
          .apply("HBase Result transformer", ParDo.of(ROW_TO_RESULT_TRANSFORM));
    }
  }

  public static class Write extends PTransform<PCollection<Mutation>, PDone> {
    private static final long serialVersionUID = 1L;
    private final String tableId;
    private final SerializableConfiguration serializableConfiguration;

    public Write(String projectId, String instanceId, String tableId) {
      Preconditions.checkNotNull(projectId, "Please configure a projectId.");
      Preconditions.checkNotNull(instanceId, "Please configure a instanceId.");
      Preconditions.checkNotNull(tableId, "Please configure a tableId.");

      this.serializableConfiguration =
          new SerializableConfiguration(BigtableConfiguration.configure(projectId, instanceId));
      this.tableId = tableId;
    }

    /**
     * Add an extended configuration option.  See {@link BigtableOptionsFactory} for more details.
     */
    public Write withConfiguration(String key, String value) {
      serializableConfiguration.get().set(key, value);
      return this;
    }

    /**
     * Adds a transformer between an HBase {@link Mutation} such as a {@link Put} or a
     * {@link Delete} and the input for a
     * {@link org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.Write}, and also adds a
     * {@link org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.Write}
     */
    @Override
    public PDone expand(PCollection<Mutation> input) {
      SingleOutput<Mutation, KV<ByteString, Iterable<com.google.bigtable.v2.Mutation>>> transform =
          ParDo.of(createTransform());

      BigtableIO.Write write = BigtableIO.write()
          .withBigtableOptions(
            BigtableOptionsFactory.fromConfiguration(serializableConfiguration.get()))
          .withTableId(tableId);

      return input
          .apply("Convert HBase to Bigtable Mutation",  transform)
          .apply("BigtableIO Mutation writer", write);
    }

    private DoFn<Mutation, KV<ByteString, Iterable<com.google.bigtable.v2.Mutation>>>
        createTransform() {

      return new DoFn<Mutation, KV<ByteString, Iterable<com.google.bigtable.v2.Mutation>>>() {
        private static final long serialVersionUID = 1L;
        private transient HBaseMutationAdapter mutationsAdapter;

        @Setup
        public void setup() {
          Configuration config = serializableConfiguration.get();
          BigtableOptions options = BigtableOptionsFactory.fromConfiguration(config);
          PutAdapter putAdapter = Adapters.createPutAdapter(config, options);
          mutationsAdapter = Adapters.createMutationsAdapter(putAdapter);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          Mutation hbaseMutation = c.element();
          MutateRowsRequest.Entry mutateRowEntry = mutationsAdapter.toEntry(hbaseMutation);
          Iterable<com.google.bigtable.v2.Mutation> mutationsList =
              mutateRowEntry.getMutationsList();
          c.output(KV.of(ZeroCopyByteStringUtil.wrap(hbaseMutation.getRow()), mutationsList));
        }
      };
    }
  }
}
