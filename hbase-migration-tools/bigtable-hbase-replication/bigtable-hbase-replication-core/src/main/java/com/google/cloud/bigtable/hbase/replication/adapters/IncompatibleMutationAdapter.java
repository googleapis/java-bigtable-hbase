/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.bigtable.hbase.replication.adapters;

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_ENABLED_FILTER_LARGE_CELLS;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.FILTER_LARGE_CELLS_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_CELL_SIZE_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_MAX_CELLS_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_ROW_SIZE_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.PUTS_IN_FUTURE_METRIC_KEY;

import com.google.cloud.bigtable.hbase.adapters.DeleteAdapter;
import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for adapters that translate CBT incompatible mutations into compatible mutations. See
 * <a href="https://cloud.google.com/bigtable/docs/hbase-differences#mutations_and_deletions">cbt
 * docs</a> for detailed list of incompatible mutations.
 *
 * <p>Subclasses must expose the constructor ChildClass(Configuration, MetricSource, Table).
 */
public abstract class IncompatibleMutationAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(IncompatibleMutationAdapter.class);
  // Connection to CBT. This can be used by child classes to fetch current state of row from CBT.
  // For example: DeleteFamilyVersion can be implemented by fetching all the cells for the version
  // (using this connection) and then deleting them.
  private final Connection connection;
  private final Configuration conf;
  private final MetricsExporter metricsExporter;
  // Maximum timestamp that hbase can send to bigtable in ms.
  static final long BIGTABLE_EFFECTIVE_MAX = Long.MAX_VALUE / 1000L;

  private void incrementDroppedIncompatibleMutations() {
    metricsExporter.incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }

  private void incrementIncompatibleMutations() {
    metricsExporter.incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }

  private void incrementTimestampOverflowMutations() {
    metricsExporter.incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 1);
    incrementIncompatibleMutations();
  }

  private void incrementIncompatibleDeletesMutations() {
    metricsExporter.incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 1);
    incrementIncompatibleMutations();
  }

  private void incrementPutsInFutureMutations() {
    metricsExporter.incCounters(PUTS_IN_FUTURE_METRIC_KEY, 1);
  }

  private void incrementDroppedIncompatibleMutationsCellSizeExceeded() {
    incrementIncompatibleMutations();
    incrementDroppedIncompatibleMutations();
    metricsExporter.incCounters(DROPPED_INCOMPATIBLE_MUTATION_CELL_SIZE_METRIC_KEY, 1);
  }

  /**
   * Creates an IncompatibleMutationAdapter with HBase configuration, MetricSource, and CBT
   * connection.
   *
   * <p>All subclasses must expose this constructor.
   *
   * @param conf HBase configuration. All the configurations required by subclases should come from
   *     here.
   * @param metricsExporter Interface to expose Hadoop metric source present in HBase Replication
   *     Endpoint.
   * @param connection Connection to destination CBT cluster. This reference help the subclasses to
   *     query destination table for certain incompatible mutation.
   */
  public IncompatibleMutationAdapter(
      Configuration conf, MetricsExporter metricsExporter, Connection connection) {
    this.conf = conf;
    this.connection = connection;
    this.metricsExporter = metricsExporter;
    // Make sure that the counters show up.
    metricsExporter.incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    metricsExporter.incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    metricsExporter.incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    metricsExporter.incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    metricsExporter.incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    metricsExporter.incCounters(DROPPED_INCOMPATIBLE_MUTATION_CELL_SIZE_METRIC_KEY, 0);
    metricsExporter.incCounters(DROPPED_INCOMPATIBLE_MUTATION_ROW_SIZE_METRIC_KEY, 0);
    metricsExporter.incCounters(DROPPED_INCOMPATIBLE_MUTATION_MAX_CELLS_METRIC_KEY, 0);
  }

  private boolean isValidDelete(Cell delete) {
    try {
      DeleteAdapter.isValidDelete(delete);
      return true;
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }

  /**
   * Translates incompatible mutations to compatible mutations. This method may block for reads on
   * destination table.
   *
   * <p>This method should never throw permanent exceptions.
   */
  public final List<Cell> adaptIncompatibleMutations(BigtableWALEntry walEntry) {
    List<Cell> cellsToAdapt = walEntry.getCells();
    List<Cell> returnedCells = new ArrayList<>(cellsToAdapt.size());
    for (int index = 0; index < cellsToAdapt.size(); index++) {
      Cell cell = cellsToAdapt.get(index);
      // check whether there is timestamp overflow from HBase -> CBT and make sure
      // it does clash with valid delete which require the timestamp to be
      // HConstants.LATEST_TIMESTAMP,
      // this will be true for reverse timestamps.
      // Do not enable trace logging as there can be many writes of this type.
      // The following log message will spam the logs and degrade the replication performance.
      if (cell.getTimestamp() >= BIGTABLE_EFFECTIVE_MAX
          && cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
        incrementTimestampOverflowMutations();
        LOG.trace(
            "Incompatible entry: "
                + cell
                + " cell time: "
                + cell.getTimestamp()
                + " max timestamp from hbase to bigtable: "
                + BIGTABLE_EFFECTIVE_MAX);
      }

      // All puts are valid.
      if (cell.getTypeByte() == KeyValue.Type.Put.getCode()) {
        // check max cell size
        if (conf.getBoolean(FILTER_LARGE_CELLS_KEY, DEFAULT_ENABLED_FILTER_LARGE_CELLS)
            && cell.getValueLength()
                > conf.getInt(
                    FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES_KEY,
                    DEFAULT_FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES)) {
          // Drop the cell as it exceeds the size to be filtered
          incrementDroppedIncompatibleMutationsCellSizeExceeded();

          LOG.warn(
              "Dropping mutation, cell value length, "
                  + cell.getValueLength()
                  + ", exceeds filter length ("
                  + FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES_KEY
                  + "), "
                  + conf.getInt(
                      FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES_KEY,
                      DEFAULT_FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES)
                  + ", cell: "
                  + cell);
          continue;
        }

        // flag if put is issued for future timestamp
        // do not log as we might fill up disk space due condition being true from clock skew
        if (cell.getTimestamp() > walEntry.getWalWriteTime()) {
          incrementPutsInFutureMutations();
        }
        returnedCells.add(cell);
        continue;
      }

      if (CellUtil.isDelete(cell)) {

        // Compatible delete
        if (isValidDelete(cell)) {
          returnedCells.add(cell);
          continue;
        }

        // Incompatible delete: Adapt it.
        try {
          LOG.info("Encountered incompatible mutation: " + cell);
          incrementIncompatibleDeletesMutations();
          returnedCells.addAll(adaptIncompatibleMutation(walEntry, index));
        } catch (UnsupportedOperationException use) {
          // Drop the mutation, not dropping it will lead to stalling of replication.
          incrementDroppedIncompatibleMutations();
          LOG.warn("Dropping incompatible mutation: " + cell);
        }
        continue;
      }

      // Replication should only produce PUT and Delete mutation. Appends/Increments are converted
      // to PUTs. Log the unexpected mutation and drop it as we don't know what CBT client will do.
      LOG.warn("Dropping unexpected type of mutation: " + cell);
      incrementIncompatibleMutations();
      incrementDroppedIncompatibleMutations();
    }
    // Increment compatible mutation if all checks pass
    return returnedCells;
  }

  /**
   * Adapts an incompatible mutation into a compatible mutation. Must throws {@link
   * UnsupportedOperationException} if it can't adapt the mutation.
   *
   * @param walEntry the WAL entry for the cell to Adapt. The wal entry provides context around the
   *     cell to be adapted, things like commit timestamp and other deletes in the entry.
   * @param index The index of the cell to adapt.
   */
  protected abstract List<Cell> adaptIncompatibleMutation(BigtableWALEntry walEntry, int index);
}
