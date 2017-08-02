/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.beam;

import com.google.auto.value.AutoValue;
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
public class BigtableHBaseIO {

  public static Read read(String projectId, String instanceId, String tableId) {
    return new AutoValue_BigtableHBaseIO_Read.Builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .setTableId(tableId)
        .setConfiguration(new Configuration(false))
        .setScan(new Scan())
        .build();
  }

  public static Write write(String projectId, String instanceId, String tableId) {
    return new AutoValue_BigtableHBaseIO_Write.Builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .setTableId(tableId)
        .setConfiguration(new Configuration(false))
        .build();
  }

  private BigtableHBaseIO() {}

  interface CloudBigtableConfigurable {
    abstract SerializableConfiguration getSerializableConfiguration();
    abstract String getProjectId();
    abstract String getInstanceId();
    abstract String getTableId();
  }

  private static BigtableOptions getOptions(CloudBigtableConfigurable cbtConfigurable) {
    Configuration originalConfiguration = cbtConfigurable.getSerializableConfiguration().get();
    Configuration config = new Configuration(originalConfiguration);
    if (config.get(BigtableOptionsFactory.PROJECT_ID_KEY) == null) {
      config.set(BigtableOptionsFactory.PROJECT_ID_KEY, cbtConfigurable.getProjectId());
    }
    if (config.get(BigtableOptionsFactory.INSTANCE_ID_KEY) == null) {
      config.set(BigtableOptionsFactory.INSTANCE_ID_KEY, cbtConfigurable.getInstanceId());
    }
    return BigtableOptionsFactory.fromConfiguration(config);
  }

  /**
   * This is a wrapper around {@link org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.Read} that adds
   * HBase semantics
   */
  @AutoValue
  public static abstract class Read extends PTransform<PBegin, PCollection<Result>>
      implements CloudBigtableConfigurable {
    private static final long serialVersionUID = 1L;

    private static final DoFn<Row, Result> ROW_TO_RESULT_TRANSFORM = new DoFn<Row, Result>() {
      private static final long serialVersionUID = 1L;
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        c.output(Adapters.ROW_ADAPTER.adaptResponse(c.element()));
      }
    };

    abstract SerializableScan getSerializableScan();

    @Override
    public abstract String toString();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSerializableConfiguration(SerializableConfiguration configuration);
      Builder setConfiguration(Configuration configuration) {
        return setSerializableConfiguration(new SerializableConfiguration(new Configuration(configuration)));
      }

      abstract Builder setProjectId(String projectId);
      abstract Builder setInstanceId(String instanceId);
      abstract Builder setTableId(String tableId);
      abstract Builder setSerializableScan(SerializableScan serializableScan);

      Builder setScan(Scan scan) {
        return setSerializableScan(new SerializableScan(scan));
      }

      abstract Read build();
    }

    /**
     * Add an extended configuration option.  See {@link BigtableOptionsFactory} for more details.
     */
    public Read withConfiguration(String key, String value) {
      Configuration newConfig = new Configuration(getSerializableConfiguration().get());
      newConfig.set(key, value);
      return toBuilder().setConfiguration(newConfig).build();
    }

    /**
     * Attach a {@link RowFilter} and potentially a {@link ByteKeyRange} to a {@link Read}.
     */
    public Read withScan(Scan scan) {
      return toBuilder().setScan(scan).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.add(DisplayData.item("configuration",
                getSerializableConfiguration().get().toString()));
        builder.add(DisplayData.item("tableId", getTableId()));
        builder.addIfNotNull(DisplayData.item("scan", getSerializableScan().get().toString()));
    }

    @Override
    public PCollection<Result> expand(PBegin input) {
      Scan scan = getSerializableScan().get();
      RowFilter filter = Adapters.SCAN_ADAPTER.adapt(scan, new DefaultReadHooks()).getFilter();
      byte[] startRow = scan.getStartRow();
      byte[] stopRow = scan.getStopRow();
      ByteKeyRange keyRange = ByteKeyRange.ALL_KEYS;
      if (startRow.length > 0){
        keyRange   = keyRange.withStartKey(ByteKey.copyFrom(startRow));
      }
      if (stopRow.length > 0){
        keyRange = keyRange.withEndKey(ByteKey.copyFrom(stopRow));
      }

      BigtableIO.Read bigtableRead =
          BigtableIO.read()
              .withBigtableOptions(getOptions(this))
              .withKeyRange(keyRange)
              .withRowFilter(filter)
              .withTableId(getTableId());
      return input.getPipeline()
          .apply("BigtableIO.Read", bigtableRead)
          .apply("HBase Result transformer", ParDo.of(ROW_TO_RESULT_TRANSFORM));
    }
  }

  @AutoValue
  public static abstract class Write extends PTransform<PCollection<Mutation>, PDone>
      implements CloudBigtableConfigurable {
    private static final long serialVersionUID = 1L;

    @Override
    public abstract String toString();

    abstract Builder toBuilder();

    private transient HBaseMutationAdapter mutationsAdapter;

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSerializableConfiguration(SerializableConfiguration configuration);
      Builder setConfiguration(Configuration configuration) {
        return setSerializableConfiguration(new SerializableConfiguration(new Configuration(configuration)));
      }

      abstract Builder setProjectId(String projectId);
      abstract Builder setInstanceId(String instanceId);
      abstract Builder setTableId(String tableId);

      abstract Write build();
    }

    /**
     * Add an extended configuration option.  See {@link BigtableOptionsFactory} for more details.
     */
    public Write withConfiguration(String key, String value) {
      Configuration newConfig = new Configuration(getSerializableConfiguration().get());
      newConfig.set(key, value);
      return toBuilder().setConfiguration(newConfig).build();
    }

    /**
     * Adds a transformer between an HBase {@link Mutation} such as a {@link Put} or a
     * {@link Delete} and the input for a
     * {@link org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.Write}, and also adds a
     * {@link org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.Write}
     */
    @Override
    public PDone expand(PCollection<Mutation> input) {
      return input
          .apply("Convert HBase to Bigtable Mutation",  ParDo.of(createTransform()))
          .apply("BigtableIO Mutation writer", BigtableIO.write()
              .withBigtableOptions(getOptions(this))
              .withTableId(getTableId()));
    }

    private DoFn<Mutation, KV<ByteString, Iterable<com.google.bigtable.v2.Mutation>>>
        createTransform() {

      return new DoFn<Mutation, KV<ByteString, Iterable<com.google.bigtable.v2.Mutation>>>() {
        private static final long serialVersionUID = 1L;

        @Setup
        public void setup() {
          if (mutationsAdapter == null) {
            Configuration config = getSerializableConfiguration().get();
            BigtableOptions options = getOptions(Write.this);
            PutAdapter putAdapter = Adapters.createPutAdapter(config, options);
            mutationsAdapter = Adapters.createMutationsAdapter(putAdapter);
          }
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
