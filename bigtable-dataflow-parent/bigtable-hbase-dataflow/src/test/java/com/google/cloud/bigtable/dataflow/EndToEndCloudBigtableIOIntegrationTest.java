/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.dataflow;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.bigtable.repackaged.com.google.cloud.hbase.BigtableConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * Reads 100,000 rows to Bigtable and makes sure that they are counted correctly.
 *
 * @author sduskis
 *
 */
public class EndToEndCloudBigtableIOIntegrationTest {
  public static final String STAGING_LOCATION_KEY = "dataflowStagingLocation";
  public static final String ZONE_ID_KEY = "dataflowZoneId";

  private static String stagingLocation = System.getProperty(STAGING_LOCATION_KEY);
  private static String zoneId = System.getProperty(ZONE_ID_KEY);
  private static String projectId =
      System.getProperty(CloudBigtableIOIntegrationTest.BIGTABLE_PROJECT_KEY);
  private static String instanceId =
      System.getProperty(CloudBigtableIOIntegrationTest.BIGTABLE_INSTANCE_KEY);

  private static final TableName TABLE_NAME =
      TableName.valueOf("EndToEndCloudBigtableIOIntegrationTest");
  protected static final byte[] FAMILY = Bytes.toBytes("cf");
  protected static final byte[] QUALIFIER = Bytes.toBytes("qual");
  protected static final byte[] VALUE = Bytes.toBytes("val");
  private static final DoFn<String, Mutation> WRITE_20000 = new DoFn<String, Mutation>() {

    private static final long serialVersionUID = 1L;

    private Aggregator<Long, Long> rowCounter = createAggregator("sent_puts", new Sum.SumLongFn());

    @Override
    public void processElement(DoFn<String, Mutation>.ProcessContext context) throws Exception {
      String prefix = context.element() + "_";
      for (int i = 0; i < 20_000; i++) {
        rowCounter.addValue(1l);
        context.output(new Put(Bytes.toBytes(prefix + i)).addColumn(FAMILY, QUALIFIER, VALUE));
      }
    }
  };

  @Test
  public void test() throws IOException {
    confirmInput();
    dropAndCreateTable();

    CloudBigtableOptions options = PipelineOptionsFactory.fromArgs(getPipelineArgs()).withValidation()
        .as(CloudBigtableOptions.class);

    writeData(options);
    confirmCount(options);
  }

  private static void confirmInput() {
    Assert.assertNotNull("Set -D" + STAGING_LOCATION_KEY + ".", stagingLocation);
    Assert.assertNotNull("Set -D" + ZONE_ID_KEY + ".", zoneId);
    Assert.assertNotNull("Set -D" + CloudBigtableIOIntegrationTest.BIGTABLE_PROJECT_KEY + ".",
      projectId);
    Assert.assertNotNull("Set -D" + CloudBigtableIOIntegrationTest.BIGTABLE_INSTANCE_KEY + ".",
      instanceId);
  }

  private static void dropAndCreateTable() throws IOException {
    try (Connection conn = BigtableConfiguration.connect(projectId, instanceId);
        Admin admin = conn.getAdmin()) {
      if (admin.tableExists(TABLE_NAME)) {
        admin.deleteTable(TABLE_NAME);
      }
      admin.createTable(new HTableDescriptor(TABLE_NAME).addFamily(new HColumnDescriptor(FAMILY)));
    }
  }

  private String[] getPipelineArgs() {
    return new String[]{
        "--runner=BlockingDataflowPipelineRunner",
        "--project=" + projectId,
        "--zone=" + zoneId,
        "--stagingLocation=" + stagingLocation,
        "--bigtableProjectId=" + projectId,
        "--bigtableInstanceId=" + instanceId,
        "--bigtableTableId=" + TABLE_NAME.getNameAsString()
    };
  }

  private void writeData(CloudBigtableOptions options) {
    // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
    CloudBigtableTableConfiguration config =
        CloudBigtableTableConfiguration.fromCBTOptions(options);

    Pipeline p = Pipeline.create(options);
    // This sets up serialization for Puts and Deletes so that Dataflow can potentially move them
    // through the network
    CloudBigtableIO.initializeForWrite(p);

    p
        .apply("Numbers", Create.of("1", "2", "3", "4", "5"))
        .apply("Create Puts", ParDo.of(WRITE_20000))
        .apply("Write to BT", CloudBigtableIO.writeToTable(config));

    p.run();
  }

  private void confirmCount(CloudBigtableOptions options) {
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setFilter(new FirstKeyOnlyFilter());

    CloudBigtableScanConfiguration config = CloudBigtableScanConfiguration.fromCBTOptions(options, scan);

    Pipeline p = Pipeline.create(options);
    PCollection<Long> count = p
        .apply("Read from BT", Read.from(CloudBigtableIO.read(config)))
        .apply("Count", Count.<Result> globally());

    DataflowAssert.thatSingleton(count).isEqualTo(100_000l);

    p.run();
  }
}
