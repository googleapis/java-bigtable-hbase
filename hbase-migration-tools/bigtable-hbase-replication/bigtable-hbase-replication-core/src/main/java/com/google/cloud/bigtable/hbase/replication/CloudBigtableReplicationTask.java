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

package com.google.cloud.bigtable.hbase.replication;

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_FILTER_LARGE_ROWS;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_FILTER_LARGE_ROWS_THRESHOLD_IN_BYTES;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_MAX_CELLS_PER_MUTATION;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.FILTER_LARGE_ROWS_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.FILTER_LARGE_ROWS_THRESHOLD_IN_BYTES_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.MAX_CELLS_PER_MUTATION_KEY;

import com.google.bigtable.repackaged.com.google.api.client.util.Preconditions;
import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.bigtable.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.bigtable.repackaged.com.google.common.base.Objects;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replicates the WAL entries to CBT. Never throws any exceptions to the caller. */
@InternalApi
public class CloudBigtableReplicationTask implements Callable<Boolean> {

  @VisibleForTesting
  interface MutationBuilder {

    boolean canAcceptMutation(Cell mutation);

    void addMutation(Cell mutation) throws IOException;

    void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException;

    long getMutationSize();

    int getMutationNumCells();
  }

  @VisibleForTesting
  static class PutMutationBuilder implements MutationBuilder {

    private final Put put;
    boolean closed = false;

    private long byteSize;

    PutMutationBuilder(byte[] rowKey) {
      put = new Put(rowKey);
      byteSize = 0;
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      Preconditions.checkState(!closed, "Can't add mutations to a closed builder");
      return cell.getTypeByte() == KeyValue.Type.Put.getCode();
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      Preconditions.checkState(!closed, "Can't add mutations to a closed builder");
      byteSize += KeyValueUtil.length(cell);
      put.add(cell);
    }

    @Override
    public void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException {
      rowMutations.add(put);
      closed = true;
    }

    @Override
    public long getMutationSize() {
      return byteSize;
    }

    @Override
    public int getMutationNumCells() {
      return put.size();
    }
  }

  @VisibleForTesting
  static class DeleteMutationBuilder implements MutationBuilder {

    private final Delete delete;

    boolean closed = false;
    private int numDeletes = 0;

    private long byteSize;

    public DeleteMutationBuilder(byte[] rowKey) {
      delete = new Delete(rowKey);
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      Preconditions.checkState(!closed, "Can't add mutations to a closed builder");
      return CellUtil.isDelete(cell);
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      Preconditions.checkState(!closed, "Can't add mutations to a closed builder");
      numDeletes++;
      byteSize += KeyValueUtil.length(cell);
      delete.addDeleteMarker(cell);
    }

    @Override
    public void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException {
      if (numDeletes == 0) {
        // Adding an empty delete will delete the whole row. DeleteRow mutation is always sent as
        // DeleteFamily mutation for each family.
        // This should never happen, but make sure we never do this.
        LOG.warn("Dropping empty delete on row " + Bytes.toStringBinary(delete.getRow()));
        return;
      }
      rowMutations.add(delete);
      // Close the builder.
      closed = true;
    }

    @Override
    public long getMutationSize() {
      return byteSize;
    }

    @Override
    public int getMutationNumCells() {
      return delete.size();
    }
  }

  @VisibleForTesting
  static class MutationBuilderFactory {

    static MutationBuilder getMutationBuilder(Cell cell) {
      if (cell.getTypeByte() == KeyValue.Type.Put.getCode()) {
        return new PutMutationBuilder(CellUtil.cloneRow(cell));
      } else if (CellUtil.isDelete(cell)) {
        return new DeleteMutationBuilder(CellUtil.cloneRow(cell));
      }
      throw new UnsupportedOperationException(
          "Processing unsupported cell type: " + cell.getTypeByte());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(CloudBigtableReplicationTask.class);
  private final Connection connection;
  private final String tableName;
  private final Map<ByteRange, List<Cell>> cellsToReplicateByRow;

  public CloudBigtableReplicationTask(
      String tableName, Connection connection, Map<ByteRange, List<Cell>> entriesToReplicate)
      throws IOException {
    this.cellsToReplicateByRow = entriesToReplicate;
    this.connection = connection;
    this.tableName = tableName;
  }

  /**
   * Replicates the list of WAL entries into CBT.
   *
   * @return true if replication was successful, false otherwise.
   */
  @Override
  public Boolean call() {
    boolean succeeded = true;
    Configuration conf = connection.getConfiguration();

    try {
      Table table = connection.getTable(TableName.valueOf(tableName));

      // Collect all the cells to replicate in this call.
      // All mutations in a WALEdit are atomic, this atomicity must be preserved. The order of WAL
      // entries must be preserved to maintain consistency. Hence, a WALEntry must be flushed before
      // next WAL entry for the same row is processed.
      //
      // However, if there are too many small WAL entries, the sequential flushing is very
      // inefficient. As an optimization, collect all the cells to replicated by row, create
      //  a single rowMutations for each row, while maintaining the order of writes.
      //
      //  And then to a final batch update with all the RowMutations. The rowMutation will guarantee
      //  an atomic in-order application of mutations. The flush will be more efficient.
      //
      // The HBase region server is pushing all the WALs in memory, so this is presumably not
      // creating
      // too much RAM overhead.

      // Build a row mutation per row.
      List<RowMutations> rowMutationsList = new ArrayList<>(cellsToReplicateByRow.size());
      for (Map.Entry<ByteRange, List<Cell>> cellsByRow : cellsToReplicateByRow.entrySet()) {
        // Create a rowMutations and add it to the list to be flushed to CBT.

        // TODO - added for debugging REMOVE
        /**
         * Iterator<Cell> cellIt = cellsByRow.getValue().iterator(); while (cellIt.hasNext()) { Put
         * put = new Put(cellsByRow.getKey().deepCopyToNewArray());
         *
         * <p>for (int i = 0; i < (100_000-1) && cellIt.hasNext(); i++) { put.add(cellIt.next()); }
         *
         * <p>table.put(put); }
         */
        List<RowMutations> rowMutations =
            buildRowMutations(
                cellsByRow.getKey().deepCopyToNewArray(), cellsByRow.getValue(), conf);
        rowMutationsList.addAll(rowMutations);
      }

      // process set of rowMutations in sequence
      for (RowMutations rowMutations : rowMutationsList) {
        Object[] results = new Object[1];
        table.batch(Arrays.asList(rowMutations), results);

        // Make sure that there were no errors returned via results.
        for (Object result : results) {
          if (result != null && result instanceof Throwable) {
            LOG.error("Encountered error while replicating wal entry.", (Throwable) result);
            succeeded = false;
            break;
          }
        }

        // do not attempt subsequent batch
        if (!succeeded) {
          break;
        }
      }
    } catch (Throwable t) {
      LOG.error("Encountered error while replicating wal entry.", t);
      succeeded = false;
    }
    return succeeded;
  }

  @VisibleForTesting
  static List<RowMutations> buildRowMutations(byte[] rowKey, List<Cell> cellList)
      throws IOException {
    return buildRowMutations(rowKey, cellList, new Configuration());
  }

  @VisibleForTesting
  static List<RowMutations> buildRowMutations(
      byte[] rowKey, List<Cell> cellList, Configuration conf) throws IOException {
    int maxByteSize =
        conf.getInt(
            FILTER_LARGE_ROWS_THRESHOLD_IN_BYTES_KEY, DEFAULT_FILTER_LARGE_ROWS_THRESHOLD_IN_BYTES);

    List<RowMutations> rowMutationsList = new ArrayList<>();

    int cellCount = 0;
    RowMutations rowMutationBuffer = new RowMutations(rowKey);
    MutationBuilder mutationBuilder = MutationBuilderFactory.getMutationBuilder(cellList.get(0));
    for (Cell cell : cellList) {

      // ensure that there are < 100K cells per row Mutation
      if (mutationBuilder.getMutationNumCells()
              % conf.getInt(MAX_CELLS_PER_MUTATION_KEY, DEFAULT_MAX_CELLS_PER_MUTATION)
          == 0) {
        // Split the row into multiple mutations if mutations exceeds threshold limit
        if (cellCount > 0) {
          LOG.info(
              "Rolling mutation due to cell count, "
                  + cellCount
                  + " exceeds max cell count, "
                  + 100_000
                  + ", cell: "
                  + cell);
        }

        cellCount = 0;
        rowMutationBuffer = new RowMutations(rowKey);
        mutationBuilder = MutationBuilderFactory.getMutationBuilder(cell);

        rowMutationsList.add(rowMutationBuffer);
      }
      // check row size - alternative to skipping adding cell to mutation due to size constraint
      //  split cells across batch requests - must be in independent sets
      else if (conf.getBoolean(FILTER_LARGE_ROWS_KEY, DEFAULT_FILTER_LARGE_ROWS)
          && mutationBuilder.getMutationSize() + KeyValueUtil.length(cell) > maxByteSize) {

        LOG.warn(
            "Rolling mutation due to cell being appended with length, "
                + KeyValueUtil.length(cell)
                + ", with existing mutation length, "
                + mutationBuilder.getMutationSize()
                + " exceeds max bytes, "
                + maxByteSize
                + ", cell: "
                + cell);

        cellCount = 0;
        rowMutationBuffer = new RowMutations(rowKey);
        mutationBuilder = MutationBuilderFactory.getMutationBuilder(cell);

        rowMutationsList.add(rowMutationBuffer);
      }

      if (!mutationBuilder.canAcceptMutation(cell)) {
        mutationBuilder.buildAndUpdateRowMutations(rowMutationBuffer);
        mutationBuilder = MutationBuilderFactory.getMutationBuilder(cell);
      }

      cellCount++;
      mutationBuilder.addMutation(cell);
    }

    // finalize the last mutation which is yet to be closed.
    mutationBuilder.buildAndUpdateRowMutations(rowMutationBuffer);
    return rowMutationsList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CloudBigtableReplicationTask)) {
      return false;
    }
    CloudBigtableReplicationTask that = (CloudBigtableReplicationTask) o;
    return Objects.equal(tableName, that.tableName)
        && Objects.equal(cellsToReplicateByRow, that.cellsToReplicateByRow);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, cellsToReplicateByRow);
  }

  @Override
  public String toString() {
    return "CloudBigtableReplicationTask{"
        + "tableName='"
        + tableName
        + '\''
        + ", cellsToReplicateByRow="
        + cellsToReplicateByRow
        + '}';
  }
}
