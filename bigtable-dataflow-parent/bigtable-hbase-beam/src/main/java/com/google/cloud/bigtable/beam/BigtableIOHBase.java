package com.google.cloud.bigtable.beam;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseMutationAdapter;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Utilities to configure {@link BigtableIO} with HBase artifacts
 */
public class BigtableIOHBase {

  public static final DoFn<Row, Result> ROW_TO_RESULT_TRANSFORM = new DoFn<Row, Result>() {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(Adapters.ROW_ADAPTER.adaptResponse(c.element()));
    }
  };

  public Read read(String projectId, String instanceId) {
    return new Read(projectId, instanceId);
  }

  /**
   * This is a wrapper around {@link org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.Read} that adds
   * HBase semantics
   */
  public static class Read extends PTransform<PBegin, PCollection<Result>> {
    private SerializableConfiguration serializableConfiguration;
    private SerializableScan scan = new SerializableScan(new Scan());
    private String tableId;

    private Read(String projectId, String instanceId) {
      this.serializableConfiguration = new SerializableConfiguration(BigtableConfiguration.configure(projectId, instanceId));
    }

    public Read withConfiguration(String key, String value) {
      serializableConfiguration.get().set(key, value);
      return this;
    }

    public Read withTable(String tableId) {
      this.tableId = tableId;
      return this;
    }

    /**
     * Attach a {@link RowFilter} and potentially a {@link ByteKeyRange} to a {@link Read}.
     */
    public Read withScan(Scan scan) {
      this.scan.set(scan);
      return this;
    }

    public Read withTableId(String tableId) {
      this.tableId = tableId;
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
      return input.getPipeline().apply(bigtableRead).apply(ParDo.of(ROW_TO_RESULT_TRANSFORM));
    }
  }

  public static DoFn<Mutation, MutateRowRequest> getWriteTransform(BigtableOptions options) {
    PutAdapter putAdapter = Adapters.createPutAdapter(new Configuration(), options);
    final HBaseMutationAdapter mutationsAdapter = Adapters.createMutationsAdapter(putAdapter);
    return new DoFn<Mutation, MutateRowRequest>(){
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        c.output(mutationsAdapter.adapt(c.element()).build());
      }
    };
  }

  public static class Write extends PTransform<PCollection<KV<byte[], Iterable<Mutation>>>, PDone> {
  }
}
