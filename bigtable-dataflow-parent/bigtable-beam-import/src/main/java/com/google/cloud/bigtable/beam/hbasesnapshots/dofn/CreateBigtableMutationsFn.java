/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotKey;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} class for converting Hbase {@link Result} to list of Hbase {@link Mutation}s. */
@InternalApi("For internal usage only")
public class CreateBigtableMutationsFn
    extends DoFn<KV<SnapshotKey, Result>, KV<String, Iterable<Mutation>>> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateBigtableMutationsFn.class);

  private final Counter droppedRows =
      Metrics.counter(CreateBigtableMutationsFn.class, "droppedRows");
  private final Counter droppedCells =
      Metrics.counter(CreateBigtableMutationsFn.class, "droppedCells");
  private final Counter droppedRowKeys =
      Metrics.counter(CreateBigtableMutationsFn.class, "droppedRowKeys");
  private final int maxMutationsPerRequestThreshold;

  private final boolean filterLargeRows;
  private final long filterLargeRowThresholdBytes;

  private final boolean filterLargeCells;
  private final int filterLargeCellThresholdBytes;

  private final boolean filterLargeRowKeys;
  private final int filterLargeRowKeysThresholdBytes;
  private final boolean filterWideRows;

  public CreateBigtableMutationsFn(
      int maxMutationsPerRequestThreshold,
      boolean filterLargeRows,
      long filterLargeRowThresholdBytes,
      boolean filterLargeCells,
      int filterLargeCellThresholdBytes,
      boolean filterLargeRowKeys,
      int filterLargeRowKeysThresholdBytes,
      boolean filterWideRows) {

    Preconditions.checkArgument(
        maxMutationsPerRequestThreshold > 0,
        "maxMutationsPerRequestThreshold must be greater than 0");
    this.maxMutationsPerRequestThreshold = maxMutationsPerRequestThreshold;

    this.filterLargeRows = filterLargeRows;
    this.filterLargeRowThresholdBytes = filterLargeRowThresholdBytes;

    this.filterLargeCells = filterLargeCells;
    this.filterLargeCellThresholdBytes = filterLargeCellThresholdBytes;

    this.filterLargeRowKeys = filterLargeRowKeys;
    this.filterLargeRowKeysThresholdBytes = filterLargeRowKeysThresholdBytes;
    this.filterWideRows = filterWideRows;
  }

  @ProcessElement
  public void processElement(
      @Element KV<SnapshotKey, Result> element,
      OutputReceiver<KV<String, Iterable<Mutation>>> outputReceiver)
      throws IOException {
    if (element == null
        || element.getKey() == null
        || element.getValue() == null
        || element.getValue().isEmpty()) {
      return;
    }

    // Extract metadata for routing and logging.
    String targetTable = element.getKey().getTableName();
    String snapshotName = element.getKey().getSnapshotName();
    byte[] rowKey = element.getValue().getRow();

    // Apply filters and chunk cells into Mutations.
    // Returns null if the entire row should be dropped.
    List<Mutation> mutations =
        convertAndValidateThresholds(rowKey, element.getValue().listCells(), snapshotName);

    // Output the mutations mapped to the target table name.
    if (mutations != null && !mutations.isEmpty()) {
      outputReceiver.output(KV.of(targetTable, mutations));
    }
  }

  /**
   * Converts a list of HBase cells for a specific row into a list of Bigtable mutations. It also
   * applies various filtering rules based on row key size, cell size, total row size, and wide row
   * thresholds.
   *
   * <p>The process follows these steps: 1. Validate row key size. 2. Iterate over cells and filter
   * large cells if enabled. 3. Accumulate total row size and filter large rows if enabled. 4. Chunk
   * cells into multiple Put requests if they exceed the mutation count threshold.
   *
   * <p>Note on limits: When specific filters (e.g., large row/cell/row key filtering) are disabled
   * or not met, this method does not fail-fast locally. The decision to reject requests that
   * violate Bigtable constraints (such as payload size or mutation limits) is deferred to the
   * underlying Cloud Bigtable client and server to avoid duplicating validation logic.
   *
   * @param rowKey the row key of the row being processed
   * @param cells the list of cells for this row
   * @param snapshotName the name of the snapshot for logging purposes
   * @return a list of Mutations (Puts), or null if the entire row should be dropped
   * @throws IOException if an error occurs
   */
  private List<Mutation> convertAndValidateThresholds(
      byte[] rowKey, List<Cell> cells, String snapshotName) throws IOException {

    // 1. Check row key size. We do this first to fail-fast and avoid allocations,
    // since the row key size is constant and independent of cell processing.
    if (filterLargeRowKeys && rowKey.length > filterLargeRowKeysThresholdBytes) {
      LOG.warn(
          "For snapshot "
              + snapshotName
              + ": Dropping row, row key length, "
              + rowKey.length
              + ", exceeds filter length threshold, "
              + filterLargeRowKeysThresholdBytes
              + ", row key: "
              + Bytes.toStringBinary(rowKey));
      droppedRowKeys.inc();
      return null; // Signal skip
    }

    List<Mutation> mutations = new ArrayList<>();
    Put put = null;
    int chunkCellCount = 0; // Tracks cells in the current Put
    int totalCellCount = 0; // Tracks total cells in the row
    long totalByteSize = 0L; // Tracks estimated serialized size of cells in the current row
    boolean loggedLargeCellForRow = false;

    // Iterate over all cells in the row to build mutations. We apply cell-level and
    // row-level filters during iteration, and split the row into multiple Put requests
    // if the number of cells exceeds maxMutationsPerRequestThreshold.
    for (Cell cell : cells) {

      // 2. Handle large cells first. We do this before accumulating row size so that
      // cells we intend to drop anyway don't unfairly cause the entire row to exceed
      // the row size threshold.
      if (filterLargeCells && cell.getValueLength() > filterLargeCellThresholdBytes) {
        if (!loggedLargeCellForRow) {
          LOG.warn(
              "For snapshot "
                  + snapshotName
                  + ": Dropping large cells for row "
                  + Bytes.toStringBinary(rowKey)
                  + ". At least one cell exceeds threshold "
                  + filterLargeCellThresholdBytes);
          loggedLargeCellForRow = true;
        }
        droppedCells.inc();
        continue; // Skip this cell, do NOT add to totalByteSize or mutations
      }

      // 3. Accumulate size and check row size.
      // Drops the entire row if it exceeds the row size threshold.
      totalByteSize += CellUtil.estimatedSerializedSizeOf(cell);
      if (filterLargeRows && totalByteSize > filterLargeRowThresholdBytes) {
        LOG.warn(
            "For snapshot "
                + snapshotName
                + ": Dropping row, row length, "
                + totalByteSize
                + ", exceeds filter length threshold, "
                + filterLargeRowThresholdBytes
                + ", row key: "
                + Bytes.toStringBinary(rowKey));
        droppedRows.inc();
        return null; // Signal skip for the entire row
      }

      // 4. Chunk cells into multiple Put requests to avoid exceeding Bigtable's limit
      // of 100,000 mutations per request. If filterWideRows is enabled and the total
      // cells in the row exceed the threshold, we drop the entire row to avoid loss
      // of atomicity.
      if (chunkCellCount == maxMutationsPerRequestThreshold || chunkCellCount == 0) {
        if (totalCellCount > 0 && filterWideRows) {
          LOG.warn(
              "For snapshot "
                  + snapshotName
                  + ": Dropping wide row, cell count exceeds threshold "
                  + maxMutationsPerRequestThreshold
                  + ", row key: "
                  + Bytes.toStringBinary(rowKey));
          droppedRows.inc();
          return null; // Signal skip
        }
        chunkCellCount = 0;
        put = new Put(rowKey);
        mutations.add(put);
      }
      put.add(cell);
      chunkCellCount++;
      totalCellCount++;
    }

    if (mutations.isEmpty()) {
      droppedRows.inc();
      return null;
    }
    return mutations;
  }
}
