package com.google.cloud.bigtable.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

/**
 * Replicates the WAL entries to CBT. Returns true if replication was successful, false otherwise.
 */
public class CloudBigtableReplicationTask implements Callable<Boolean> {


  interface MutationBuilder {

    // TODO: Add mechanism to indicate permanent failures?
    boolean canAcceptMutation(Cell mutation);

    void addMutation(Cell mutation) throws IOException;

    void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException;
  }

  static class PutMutationBuilder implements MutationBuilder {

    // TODO make it final
    private Put put;

    PutMutationBuilder(byte[] rowKey) {
      put = new Put(rowKey);
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      if (put == null) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      return cell.getTypeByte() == KeyValue.Type.Put.getCode();
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      if (put == null) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      put.add(cell);
    }

    @Override
    public void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException {
      rowMutations.add(put);
      // LOG.error("Build a put mutation with " + put.size() + " entries.");
      put = null;
    }
  }

  static class DeleteMutationBuilder implements MutationBuilder {

    // TODO make it final
    private Delete delete;
    private int numDeletes = 0;

    public DeleteMutationBuilder(byte[] rowKey) {
      delete = new Delete(rowKey);
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      if (delete == null) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      // TODO Maybe check for permanent exceptions due to compatibility gaps?
      return CellUtil.isDelete(cell);
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      if (delete == null) {
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
      // LOG.error("Build a delete mutation with " + delete.size() + " entries.");
      // Close the builder.
      delete = null;
    }
  }

  private static class MutationBuilderFactory {

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

  public CloudBigtableReplicationTask(String tableName, Connection connection,
      Map<ByteRange, List<Cell>> entriesToReplicate) throws IOException {
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
    Table table;

    try {
      table = connection.getTable(TableName.valueOf(tableName));

      // Collect all the cells to replicate in this call.
      // To guarantee the atomic update for the same WAL edit, it needs to be flushed before starting
      // the next one. However, if there are too many small WAL entries, the flushing is very
      // inefficient. To avoid it, collect all the cells to replicated, create rowMutations for each
      // row, while maintaining the order of writes. And then to a final batch update. The rowMutation
      // will guarantee an atomic put while maintaining the order of mutations in them. The flush will
      // be more efficient.
      // The HBase region server is pushing all the WALs in memory, so this is presumably not creating
      // too much overhead that what is already there.

      LOG.warn("Replicating {} rows.", cellsToReplicateByRow.size());

      // Build a row mutation per row.
      List<RowMutations> rowMutationsList = new ArrayList<>(cellsToReplicateByRow.size());
      for (Map.Entry<ByteRange, List<Cell>> cellsByRow : cellsToReplicateByRow.entrySet()) {
        RowMutations rowMutations = new RowMutations(cellsByRow.getKey().deepCopyToNewArray());
        List<Cell> cellsForRow = cellsByRow.getValue();
        // TODO Make sure that there are < 100K cells per row Mutation

        MutationBuilder mutationBuilder =
            MutationBuilderFactory.getMutationBuilder(cellsForRow.get(0));
        for (Cell cell : cellsForRow) {
          LOG.error("Replicating cell: " + cell + " with val: " + Bytes.toStringBinary(
              CellUtil.cloneValue(cell)));
          if (mutationBuilder.canAcceptMutation(cell)) {
            mutationBuilder.addMutation(cell);
          } else {
            mutationBuilder.buildAndUpdateRowMutations(rowMutations);
            mutationBuilder = MutationBuilderFactory.getMutationBuilder(cell);
            mutationBuilder.addMutation(cell);
          }
        } // Single row exiting.
        // finalize the last mutation which is yet to be closed.
        mutationBuilder.buildAndUpdateRowMutations(rowMutations);
        rowMutationsList.add(rowMutations);
      }

      LOG.error("Replicating rowMutations: " + rowMutationsList);
      Object[] futures = new Object[rowMutationsList.size()];
      // TODO: Decide if we want to tweak retry policies of the batch puts?
      table.batch(rowMutationsList, futures);
      // Make sure that there were no errors returned via futures.
      for (Object future : futures) {
        if (future != null && future instanceof Throwable) {
          LOG.error("{FUTURES} Encountered error while replicating wal entry.", (Throwable) future);
          succeeded = false;
          break;
        }
      }
    } catch (Throwable t) {
      LOG.error("Encountered error while replicating wal entry.", t);
      if (t.getCause() != null) {
        LOG.error("chained exception ", t.getCause());
      }
      succeeded = false;
    }
    return succeeded;
  }
}
