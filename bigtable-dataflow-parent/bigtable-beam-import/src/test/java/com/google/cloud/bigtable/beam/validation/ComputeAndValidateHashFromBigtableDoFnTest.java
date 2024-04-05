/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.beam.validation;

import static com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.test.helper.BigtableEmulatorRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.BigtableTableHashAccessor.BigtableResultHasher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class ComputeAndValidateHashFromBigtableDoFnTest {

  private static final byte[] EMPTY_ROW_KEY = HConstants.EMPTY_BYTE_ARRAY;
  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  public static final String FAKE_TABLE = "fake-table";
  private static final String ROW_KEY_PREFIX = "row-";
  private static final String VALUE_PREFIX = "value-";
  private static final byte[] EXTRA_VALUE = "add".getBytes();
  private static final byte[] CF = "cf".getBytes();
  private static final byte[] CF2 = "cf".getBytes();
  private static final byte[] COL = "col".getBytes();
  private static final long TS = 1000l;
  private static final int FIRST_ROW_INDEX = 20;
  private static final int LAST_ROW_INDEX = 31;

  @Rule public final BigtableEmulatorRule bigtableEmulator = new BigtableEmulatorRule();

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private ComputeAndValidateHashFromBigtableDoFn doFn;

  // Clients that will be connected to the emulator
  private BigtableTableAdminClient tableAdminClient;

  private Connection connection;
  private Table table;
  // Fake a TableHashWrapper.
  private FakeTableHashWrapper fakeTableHashWrapper;

  private List<RangeHash> hashes;

  @Before
  public void setUp() throws IOException {
    hashes = new ArrayList<>();
    // Initialize the clients to connect to the emulator
    tableAdminClient =
        BigtableTableAdminClient.create(
            BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort())
                .setProjectId("fake-project")
                .setInstanceId("fake-instance")
                .build());

    CloudBigtableTableConfiguration config =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId("fake-project")
            .withInstanceId("fake-instance")
            .withTableId(FAKE_TABLE)
            .withConfiguration(
                BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY,
                "localhost:" + bigtableEmulator.getPort())
            .build();

    connection = BigtableConfiguration.connect(config.toHBaseConfig());
    table = connection.getTable(TableName.valueOf(FAKE_TABLE));
    fakeTableHashWrapper = new FakeTableHashWrapper();
    // Scan all the cells for the column, HBase scan fetches 1 cell/column by default
    fakeTableHashWrapper.scan = new Scan().setMaxVersions();

    FakeTableHashWrapperFactory fakeFactory = new FakeTableHashWrapperFactory(fakeTableHashWrapper);

    doFn =
        new ComputeAndValidateHashFromBigtableDoFn(
            config,
            StaticValueProvider.of(FAKE_TABLE),
            StaticValueProvider.of("proj"),
            StaticValueProvider.of("hash"),
            fakeFactory);

    // Create a test table that can be used in tests
    tableAdminClient.createTable(
        CreateTableRequest.of(FAKE_TABLE)
            .addFamily(new String(CF), GCRULES.maxVersions(100))
            .addFamily(new String(CF2), GCRULES.maxVersions(100)));

    p.getCoderRegistry().registerCoderForClass(RangeHash.class, new RangeHashCoder());

    // Fill CBT table with data.
    writeDataToTable();
  }

  @After
  public void tearDown() throws IOException {
    doFn.cleanupConnection();
    // TODO should we delete the table for each test?
    tableAdminClient.deleteTable(FAKE_TABLE);
    tableAdminClient.close();
    connection.close();
  }

  private byte[] getRowKey(int i) {
    return (ROW_KEY_PREFIX + i).getBytes();
  }

  private byte[] getValue(int rowIndex, int cellIndex) {
    return (VALUE_PREFIX + rowIndex + "-" + cellIndex).getBytes();
  }

  private void writeDataToTable() throws IOException {
    List<Put> puts = new ArrayList<>();
    // Tests use the rows 21-30. Setup some extra data simulate the real world scenario where
    // there will be other workitems working parallely on the table.
    for (int i = 20; i < 32; i++) {
      for (int j = 0; j < 2; j++) {
        // Insert rows with 2 cells each
        Put put = new Put(getRowKey(i));
        put.addColumn(CF, COL, TS + j, getValue(i, j));
        puts.add(put);
      }
    }
    table.put(puts);
  }

  /** Deletes the row range [startIndex, stopIndex) */
  private void deleteRange(int startIndex, int stopIndex) throws IOException {
    for (int i = startIndex; i < stopIndex; i++) {
      table.delete(new Delete(getRowKey(i)));
    }
  }

  // Creates a RangeHash for range [startRow, stopRow).
  private RangeHash createHash(byte[] startRow, byte[] stopRow) throws IOException {
    LOG.debug("Creating hash for rows " + startRow + " to " + stopRow);
    BigtableResultHasher hasher = new BigtableResultHasher();
    hasher.startBatch(new ImmutableBytesWritable(startRow));

    // Scan all the cells for a column.
    Scan scan = new Scan().setMaxVersions().withStartRow(startRow).withStopRow(stopRow, false);

    // Read the rows from Bigtable and compute the expected hash.
    for (Result result : table.getScanner(scan)) {
      LOG.debug("Adding result to hash: " + result);
      hasher.hashResult(result);
    }
    hasher.finishBatch();
    return RangeHash.of(
        new ImmutableBytesWritable(startRow),
        new ImmutableBytesWritable(stopRow),
        hasher.getBatchHash());
  }

  private void validateCounters(
      PipelineResult result, Long expectedMatches, Long expectedMismatches) {
    MetricQueryResults metrics = result.metrics().allMetrics();
    Map<String, Long> counters =
        StreamSupport.stream(metrics.getCounters().spliterator(), false)
            .collect(Collectors.toMap((m) -> m.getName().getName(), (m) -> m.getAttempted()));
    Assert.assertEquals(expectedMatches, counters.get("ranges_matched"));
    Assert.assertEquals(expectedMismatches, counters.get("ranges_not_matched"));
  }

  ////////// Happy case tests for various setups//////////////////////
  @Test
  public void testHashMatchesForMultipleRange() throws Exception {
    hashes.add(createHash(getRowKey(21), getRowKey(24)));
    hashes.add(createHash(getRowKey(24), getRowKey(28)));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(getRowKey(21)), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).empty();
    PipelineResult result = p.run();
    validateCounters(result, 2L, 0L);
  }

  @Test
  public void testHashMatchesForSingleRange() throws Exception {
    hashes.add(createHash(getRowKey(21), getRowKey(24)));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(getRowKey(21)), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).containsInAnyOrder();
    PipelineResult result = p.run();
    validateCounters(result, 1L, 0L);
  }

  @Test
  public void testHashMatchesForFullTableScanWithMultipleRange() throws Exception {
    hashes.add(createHash(EMPTY_ROW_KEY, getRowKey(24)));
    hashes.add(createHash(getRowKey(24), EMPTY_ROW_KEY));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(EMPTY_ROW_KEY), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).empty();
    PipelineResult result = p.run();
    validateCounters(result, 2L, 0L);
  }

  @Test
  public void testHashMatchesForMultipleSingleRowRange() throws Exception {
    hashes.add(createHash(getRowKey(22), getRowKey(23)));
    hashes.add(createHash(getRowKey(23), getRowKey(24)));
    hashes.add(createHash(getRowKey(24), getRowKey(25)));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(getRowKey(22)), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).empty();
    PipelineResult result = p.run();
    validateCounters(result, 3L, 0L);
  }

  ///////////////// Test mismatches when Bigtable has extra rows ////////////////////
  @Test
  public void testAdditionalCellInMiddle() throws Exception {
    hashes.add(createHash(getRowKey(21), getRowKey(24)));
    hashes.add(createHash(getRowKey(24), getRowKey(27)));
    hashes.add(createHash(getRowKey(27), getRowKey(30)));

    // Add an extra cell in the table
    table.put(new Put(getRowKey(25)).addColumn(CF, COL, EXTRA_VALUE));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(getRowKey(21)), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).containsInAnyOrder(hashes.get(1));
    PipelineResult result = p.run();
    validateCounters(result, 2L, 1L);
  }

  @Test
  public void testAdditionalRowsAtEnds() throws Exception {
    hashes.add(createHash(EMPTY_ROW_KEY, getRowKey(24)));
    hashes.add(createHash(getRowKey(24), getRowKey(27)));
    hashes.add(createHash(getRowKey(27), EMPTY_ROW_KEY));

    // Add an extra row in the beginning
    table.put(new Put(getRowKey(1)).addColumn(CF, COL, EXTRA_VALUE));

    // Add an extra row at the end.
    table.put(new Put(getRowKey(5)).addColumn(CF, COL, EXTRA_VALUE));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(EMPTY_ROW_KEY), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).containsInAnyOrder(hashes.get(0), hashes.get(2));
    PipelineResult result = p.run();
    validateCounters(result, 1L, 2L);
  }

  ///////////////////// Test different values ///////////////////////////
  @Test
  public void testDifferentValues() throws Exception {
    hashes.add(createHash(EMPTY_ROW_KEY, getRowKey(21)));
    hashes.add(createHash(getRowKey(21), getRowKey(23)));
    hashes.add(createHash(getRowKey(23), getRowKey(25)));
    hashes.add(createHash(getRowKey(25), getRowKey(27)));
    hashes.add(createHash(getRowKey(27), EMPTY_ROW_KEY));

    // Modify the CF
    table.delete(new Delete(getRowKey(20)).addColumns(CF, COL, TS));
    table.put(new Put(getRowKey(1)).addColumn(CF2, COL, TS, getValue(20, 0)));

    // Modify the qualifier
    table.delete(new Delete(getRowKey(22)).addColumns(CF, COL, TS));
    table.put(new Put(getRowKey(22)).addColumn(CF, "random-col".getBytes(), TS, getValue(22, 0)));

    // Modify the timestamp
    table.delete(new Delete(getRowKey(24)).addColumns(CF, COL, TS));
    table.put(new Put(getRowKey(24)).addColumn(CF, COL, 1, getValue(24, 0)));

    // Modify the value
    table.delete(new Delete(getRowKey(26)).addColumns(CF, COL, TS));
    table.put(new Put(getRowKey(26)).addColumn(CF, COL, getValue(26, 0)));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(EMPTY_ROW_KEY), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output)
        .containsInAnyOrder(hashes.get(0), hashes.get(1), hashes.get(2), hashes.get(3));
    PipelineResult result = p.run();
    validateCounters(result, 1L, 4L);
  }

  ////////////////// Tests with CBT missing data //////////////////////////////
  @Test
  public void testMissingRows() throws Exception {
    hashes.add(createHash(EMPTY_ROW_KEY, getRowKey(21)));
    hashes.add(createHash(getRowKey(21), getRowKey(23)));
    hashes.add(createHash(getRowKey(23), getRowKey(25)));
    hashes.add(createHash(getRowKey(25), getRowKey(27)));
    hashes.add(createHash(getRowKey(27), EMPTY_ROW_KEY));

    // Delete a row at the beginning
    table.delete(new Delete(getRowKey(FIRST_ROW_INDEX)));

    // Delete a row at the middle
    table.delete(new Delete(getRowKey(24)));

    // Delete a row at the end
    table.delete(new Delete(getRowKey(LAST_ROW_INDEX)));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(EMPTY_ROW_KEY), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).containsInAnyOrder(hashes.get(0), hashes.get(2), hashes.get(4));
    PipelineResult result = p.run();
    validateCounters(result, 2L, 3L);
  }

  @Test
  public void testMissingRanges() throws Exception {
    hashes.add(createHash(EMPTY_ROW_KEY, getRowKey(21)));
    hashes.add(createHash(getRowKey(21), getRowKey(23)));
    hashes.add(createHash(getRowKey(23), getRowKey(25)));
    hashes.add(createHash(getRowKey(25), getRowKey(27)));
    hashes.add(createHash(getRowKey(27), getRowKey(29)));
    hashes.add(createHash(getRowKey(29), EMPTY_ROW_KEY));

    // Delete a range at the beginning
    deleteRange(FIRST_ROW_INDEX, 21);

    // Delete a range in middle
    deleteRange(23, 25);

    // Delete row ranges at the end, bigtable scanner will finish with multiple row-ranges to
    // process.
    deleteRange(27, LAST_ROW_INDEX + 1);

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(EMPTY_ROW_KEY), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output)
        .containsInAnyOrder(hashes.get(0), hashes.get(2), hashes.get(4), hashes.get(5));
    PipelineResult result = p.run();
    validateCounters(result, 2L, 4L);
  }

  @Test
  public void testCbtEmpty() throws Exception {
    hashes.add(createHash(EMPTY_ROW_KEY, getRowKey(25)));
    hashes.add(createHash(getRowKey(25), getRowKey(29)));
    hashes.add(createHash(getRowKey(29), EMPTY_ROW_KEY));

    // Delete all data from bigtable
    deleteRange(FIRST_ROW_INDEX, LAST_ROW_INDEX);

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(EMPTY_ROW_KEY), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).containsInAnyOrder(hashes);
    PipelineResult result = p.run();
    validateCounters(result, 0L, 3L);
  }

  ////////////////////// Test that scan is used from TableHash.////////////////////////
  @Test
  public void testScanFromTableHash() throws Exception {
    hashes.add(createHash(getRowKey(21), getRowKey(24)));
    hashes.add(createHash(getRowKey(24), getRowKey(27)));
    hashes.add(createHash(getRowKey(27), getRowKey(30)));

    // Update the TableHashWrapper Scan to default. Scan from HashTable.TableHash determines the
    // cells used to compute hash. CBT has to use the same cells for validation.
    fakeTableHashWrapper.scan = new Scan();

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(getRowKey(21)), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output).containsInAnyOrder(hashes);
    PipelineResult result = p.run();
    validateCounters(result, 0L, 3L);
  }

  ////////////////////// Combination of different cases //////////////////////////////////
  @Test
  public void testMismatchesComprehensive() throws Exception {
    hashes.add(createHash(EMPTY_ROW_KEY, getRowKey(21)));
    hashes.add(createHash(getRowKey(21), getRowKey(23)));
    hashes.add(createHash(getRowKey(23), getRowKey(25)));
    hashes.add(createHash(getRowKey(25), getRowKey(27)));
    hashes.add(createHash(getRowKey(27), getRowKey(29)));
    hashes.add(createHash(getRowKey(29), EMPTY_ROW_KEY));

    // Delete a range at the beginning from CBT
    deleteRange(FIRST_ROW_INDEX, 21);

    // Delete a row in middle from CBT
    table.delete(new Delete(getRowKey(23)));

    // Update a value in CBT
    table.delete(new Delete(getRowKey(27)).addColumns(CF, COL, TS));
    table.put(new Put(getRowKey(27)).addColumn(CF, COL, getValue(27, 0)));

    // Add an extra row at the end.
    table.put(new Put(getRowKey(5)).addColumn(CF, COL, EXTRA_VALUE));

    PCollection<KV<String, Iterable<List<RangeHash>>>> input =
        p.apply(Create.of(KV.of(new String(EMPTY_ROW_KEY), Arrays.asList(hashes))));

    PCollection<RangeHash> output = input.apply(ParDo.of(doFn));
    PAssert.that(output)
        .containsInAnyOrder(hashes.get(0), hashes.get(2), hashes.get(4), hashes.get(5));
    PipelineResult result = p.run();
    validateCounters(result, 2L, 4L);
  }
}
