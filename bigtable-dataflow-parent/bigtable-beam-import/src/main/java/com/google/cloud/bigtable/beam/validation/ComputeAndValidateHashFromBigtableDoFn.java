/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import static com.google.cloud.bigtable.beam.validation.SyncTableUtils.immutableBytesToString;

import com.google.bigtable.repackaged.com.google.common.base.Preconditions;
import com.google.bigtable.repackaged.com.google.common.collect.Lists;
import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.CloudBigtableConfiguration;
import com.google.cloud.bigtable.beam.TemplateUtils;
import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import com.google.cloud.bigtable.beam.validation.SyncTableJob.SyncTableOptions;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.BigtableTableHashAccessor.BigtableResultHasher;

/**
 * A {@link DoFn} that takes a row range and hash from HBase and validates the hash from rows read
 * from Cloud Bigtable.
 */
class ComputeAndValidateHashFromBigtableDoFn
    extends AbstractCloudBigtableTableDoFn<KV<String, Iterable<List<RangeHash>>>, RangeHash> {

  private static final long serialVersionUID = 2349094L;
  private final ValueProvider<String> tableName;
  private final ValueProvider<String> projectId;
  private final ValueProvider<String> sourceHashDir;

  private final TableHashWrapperFactory tableHashWrapperFactory;

  // Counter for reporting matching and mismatching ranges. Names are similar to HBase sync-table
  // job.
  private final Counter matches = Metrics.counter("cbt-dataflow-validate", "ranges_matched");
  private final Counter mismatches = Metrics.counter("cbt-dataflow-validate", "ranges_not_matched");

  public ComputeAndValidateHashFromBigtableDoFn(SyncTableOptions options) {
    super(TemplateUtils.BuildSyncTableConfig(options));
    this.tableName = options.getBigtableTableId();
    // Create a local copy of ValueProviders, PipelineOptions are not serializable.
    projectId = options.getBigtableProject();
    sourceHashDir = options.getHashTableOutputDir();
    tableHashWrapperFactory = new TableHashWrapperFactory();
  }

  @VisibleForTesting
  ComputeAndValidateHashFromBigtableDoFn(
      CloudBigtableConfiguration config,
      ValueProvider<String> tableName,
      ValueProvider<String> projectId,
      ValueProvider<String> sourceHashDir,
      TableHashWrapperFactory factory) {
    super(config);
    this.tableName = tableName;
    this.tableHashWrapperFactory = factory;
    this.sourceHashDir = projectId;
    this.projectId = sourceHashDir;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    List<List<RangeHash>> wrapperdRangeHashes = Lists.newArrayList(context.element().getValue());
    // BufferedHadoopHashTableSource generates only 1 item per groupby key, key is startKey for the
    // Sorted ranges.
    Preconditions.checkState(
        wrapperdRangeHashes.size() == 1, "Can not have muiple entries for a key");
    List<RangeHash> rangeHashes = wrapperdRangeHashes.get(0);
    Preconditions.checkState(!rangeHashes.isEmpty(), "Can not have empty ranges in DO_FN");

    ImmutableBytesWritable rangeStartInclusive = rangeHashes.get(0).startInclusive;
    ImmutableBytesWritable rangeEndExclusive =
        rangeHashes.get(rangeHashes.size() - 1).stopExclusive;

    BigtableResultHasher resultHasher = new BigtableResultHasher();
    resultHasher.startBatch(rangeStartInclusive);

    // Since all the row-ranges are sorted in HashTable's data files, 1 big scan can be used
    // to read all the row ranges. Parallelism is achieved by splitting the HashTable's data
    // files into smaller bundle of row-ranges in GroupBy.
    ResultScanner scanner =
        createBigtableScan(rangeStartInclusive.copyBytes(), rangeEndExclusive.copyBytes());

    Iterator<RangeHash> rangeHashIterator = rangeHashes.iterator();
    long numRows = 0;

    RangeHash currentRangeHash = rangeHashIterator.next();

    // Process each row and validate hashes
    for (Result result : scanner) {
      numRows++;
      if (numRows % 10_000 == 0) {
        // Heartbeat in logs in case a large scan gets hung.
        DOFN_LOG.debug("Processed " + numRows + " rows ");
      }

      ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());

      // Check if the rowKey belongs to current range, if not keep iterating through the
      // rangeHashes until rowKey's range is found.
      while (!isWithinUpperBound(currentRangeHash.stopExclusive, rowKey)) {
        validateBatchHash(context, resultHasher, currentRangeHash);
        // THIS SHOULD NEVER HAPPEN. Bigtable is being scanned till the last
        // RangeHash.endKeyExclusive(), so bigtable's result should not outlast the
        // rangeHashes.
        Preconditions.checkState(
            rangeHashIterator.hasNext(),
            "Buffer reached to end while scan is still active at row : %s. "
                + "Affected Range: [%s, %s)."
                + immutableBytesToString(result.getRow())
                + immutableBytesToString(rangeStartInclusive)
                + immutableBytesToString(rangeEndExclusive));
        currentRangeHash = rangeHashIterator.next();
      }

      // Always Hash the current row.
      resultHasher.hashResult(result);
    }

    // Bigtable scan is finished at this point and rangeHashes may contain additional row ranges.
    // Last range will always be unverified as the range end is exclusive and
    // currentRow > rangeEndExclusive will never by true. Verify the last range.
    validateBatchHash(context, resultHasher, currentRangeHash);

    // If there are remaining ranges in the rangeHashes they all need to reported as mismatched as
    // there is nothing in Cloud Bigtable for those row ranges.
    // for (int i = bufferIndex; i < rangeHashes.size(); i++) {
    while (rangeHashIterator.hasNext()) {
      currentRangeHash = rangeHashIterator.next();
      reportMismatch(context, currentRangeHash);
    }

    DOFN_LOG.debug(
        "Finishing context by outputting {}  keys in range [{}, {}).",
        rangeHashes.size(),
        immutableBytesToString(rangeStartInclusive),
        immutableBytesToString(rangeEndExclusive));
  }

  private ResultScanner createBigtableScan(byte[] startKeyInclusive, byte[] stopKeyExclusive)
      throws IOException {
    Table table = getConnection().getTable(TableName.valueOf(tableName.get()));
    // Get the scan from TableHash, HashTable can be run to hash a small part of data (selected
    // column families, timestamp range, maxVersions etc), this scan allows us to fetch the same
    // data from Cloud Bigtable to match.
    TableHashWrapper tableHash =
        tableHashWrapperFactory.getTableHash(projectId.get(), sourceHashDir.get());
    Scan scan = tableHash.getScan();
    // Set the workitem boundaries on the scan.
    if (startKeyInclusive.length > 0) {
      scan.withStartRow(startKeyInclusive, true);
    }
    if (stopKeyExclusive.length > 0) {
      scan.withStopRow(stopKeyExclusive, false);
    }

    return table.getScanner(scan);
  }

  /**
   * Determines if row >= stopExclusive for a row range (start, stopExclusive). Empty stopExclusive
   * represents a range with no upper bound.
   */
  private boolean isWithinUpperBound(
      ImmutableBytesWritable stopExclusive, ImmutableBytesWritable row) {
    return stopExclusive.equals(HConstants.EMPTY_END_ROW) || row.compareTo(stopExclusive) < 0;
  }

  private void validateBatchHash(
      ProcessContext context, BigtableResultHasher resultHasher, RangeHash currentRangeHash) {
    // The batch is always started, so its safe to finish the batch. If there were no rows, we will
    // get a hash for empty batch.
    resultHasher.finishBatch();
    if (!resultHasher.getBatchHash().equals(currentRangeHash.hash)) {
      reportMismatch(context, currentRangeHash);
    } else {
      matches.inc();
    }
    // Start a new batch
    resultHasher.startBatch(currentRangeHash.stopExclusive);
  }

  private void reportMismatch(ProcessContext context, RangeHash currentRangeHash) {
    mismatches.inc();
    DOFN_LOG.info(
        "MISMATCH ON RANGE [{}, {}).",
        immutableBytesToString(currentRangeHash.startInclusive),
        immutableBytesToString(currentRangeHash.stopExclusive));
    context.output(currentRangeHash);
  }
}
