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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
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
class CloudBigtableReplicationTask implements Callable<Boolean> {

  interface MutationBuilder {

    boolean canAcceptMutation(Cell mutation);

    void addMutation(Cell mutation) throws IOException;

    void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException;
  }

  static class PutMutationBuilder implements MutationBuilder {

    private final Put put;
    boolean closed = false;

    PutMutationBuilder(byte[] rowKey) {
      put = new Put(rowKey);
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      if (closed) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      return cell.getTypeByte() == KeyValue.Type.Put.getCode();
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      if (closed) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      put.add(cell);
    }

    @Override
    public void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException {
      rowMutations.add(put);
      closed = true;
    }
  }

  static class DeleteMutationBuilder implements MutationBuilder {

    private final Delete delete;

    boolean closed = false;
    private int numDeletes = 0;

    public DeleteMutationBuilder(byte[] rowKey) {
      delete = new Delete(rowKey);
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      if (closed) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      return CellUtil.isDelete(cell);
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      if (closed) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      numDeletes++;
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
  }

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
        RowMutations rowMutations =
            buildRowMutations(cellsByRow.getKey().deepCopyToNewArray(), cellsByRow.getValue());
        rowMutationsList.add(rowMutations);
      }

      Object[] results = new Object[rowMutationsList.size()];
      table.batch(rowMutationsList, results);

      // Make sure that there were no errors returned via results.
      for (Object result : results) {
        if (result != null && result instanceof Throwable) {
          LOG.error("Encountered error while replicating wal entry.", (Throwable) result);
          succeeded = false;
          break;
        }
      }
    } catch (Throwable t) {
      LOG.error("Encountered error while replicating wal entry.", t);
      succeeded = false;
    }
    return succeeded;
  }

  static RowMutations buildRowMutations(byte[] rowKey, List<Cell> cellList) throws IOException {
    RowMutations rowMutationBuffer = new RowMutations(rowKey);
    // TODO Make sure that there are < 100K cells per row Mutation
    MutationBuilder mutationBuilder = MutationBuilderFactory.getMutationBuilder(cellList.get(0));
    for (Cell cell : cellList) {
      if (!mutationBuilder.canAcceptMutation(cell)) {
        mutationBuilder.buildAndUpdateRowMutations(rowMutationBuffer);
        mutationBuilder = MutationBuilderFactory.getMutationBuilder(cell);
      }
      mutationBuilder.addMutation(cell);
    }

    // finalize the last mutation which is yet to be closed.
    mutationBuilder.buildAndUpdateRowMutations(rowMutationBuffer);
    return rowMutationBuffer;
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
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(cellsToReplicateByRow, that.cellsToReplicateByRow);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, cellsToReplicateByRow);
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
