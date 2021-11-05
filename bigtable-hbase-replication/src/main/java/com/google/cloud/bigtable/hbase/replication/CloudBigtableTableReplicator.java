package com.google.cloud.bigtable.hbase.replication;

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableReplicator is responsible for applying the HBase WAL logs for a single table to its peer in
 * Cloud Bigtable.
 */
public class CloudBigtableTableReplicator {

  // TODO: remove this class and move to Bytes.ByteArrayComparator
  // Not required, We can use normal HashMap with Bytes.ByteArrayComparator in
  // org.apache.hadoop.hbase.util.
  public static final class BytesKey {

    private final byte[] array;

    public BytesKey(byte[] array) {
      this.array = array;
    }

    public byte[] getBytes() {
      return this.array;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BytesKey bytesKey = (BytesKey) o;
      return Arrays.equals(array, bytesKey.array);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(array);
    }
  }

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
      LOG.error("Build a put mutation with " + put.size() + " entries.");
      put = null;
    }
  }

  static class DeleteMutationBuilder implements MutationBuilder {

    //TODO make it final
    private Delete delete;

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
      //TODO track succesfully added mutations. If all the deletes are skipped, an empty Delete will
      // delete the row which may be an un-intended consequence.
      if (delete == null) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      /**
       * TODO Decide how to handle permanent errors? Maybe add a config to push such errors to
       * pubsub or a new BT table or a log file.
       *
       * TODO We should offer toggles for timestamp delete,
       * if customers opt in for a best effort timestamp deltes we should do a get and delete. Or
       * maybe a CheckAndMutate default behaviour can be handling as per permanent failure policy
       * (log/pubsub/separate table)
       *
       * TODO do a full compatibility check here. Maybe use a comaptibility checker form the hbase
       * client
       */
      if (CellUtil.isDeleteType(cell)) {
        LOG.info("Deleting cell " + cell.toString() + " with ts: " + cell.getTimestamp());
        delete.addDeleteMarker(cell);
      } else if (CellUtil.isDeleteColumns(cell)) {
        //
        LOG.info("Deleting columns " + cell.toString() + " with ts: " + cell.getTimestamp());
        delete.addDeleteMarker(cell);
      } else if (CellUtil.isDeleteFamily(cell)) {
        LOG.info("Deleting family with ts: " + cell.getTimestamp());
        if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
          LOG.info(
              "SKIPPING family deletion before timestamp as it is not supported by CBT. "
                  + cell.toString());
        } else {
          delete.addDeleteMarker(cell);
        }

        // delete.addFamily(CellUtil.cloneFamily(cell), cell.getTimestamp());
      } else if (CellUtil.isDeleteFamilyVersion(cell)) {
        LOG.info("SKIPPING family version as it is not supported by CBT. " + cell.toString());
      } else {
        throw new IllegalStateException("Unknown delete sent here: " + cell.getTypeByte());
      }
    }

    @Override
    public void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException {
      // Add a check for empty delete. An empty delete basically deletes a row.
      rowMutations.add(delete);
      LOG.error("Build a delete mutation with " + delete.size() + " entries.");
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

  private static final Logger LOG = LoggerFactory.getLogger(CloudBigtableTableReplicator.class);
  private final String tableName;
  private final Table table;

  public CloudBigtableTableReplicator(String tableName, Connection connection) throws IOException {
    this.tableName = tableName;
    this.table = connection.getTable(TableName.valueOf(tableName));
  }

  /**
   * Replicates the list of WAL entries into CBT.
   *
   * @return true if replication was successful, false otherwise.
   */
  public boolean replicateTable(List<WAL.Entry> walEntriesToReplicate) throws IOException {
    boolean succeeded = true;
    // Collect all the cells to replicate in this call.
    // To guarantee the atomic update for the same WAL edit, it needs to be flushed before starting
    // the next one. However, if there are too many small WAL entries, the flushing is very
    // inefficient. To avoid it, collect all the cells to replicated, create rowMutations for each
    // row, while maintaining the order of writes. And then to a final batch update. The rowMutation
    // will guarantee an atomic put while maintaining the order of mutations in them. The flush will
    // more more efficient.
    // The HBase region server is pushing all the WALs in memory, so this is presumably not creating
    // too much overhead that what is already there.
    List<Cell> cellsToReplicate = new ArrayList<>();
    for (WAL.Entry walEntry : walEntriesToReplicate) {
      LOG.warn("Processing WALKey: " + walEntry.getKey().toString() + " with " + walEntry.getEdit()
          .getCells() + " cells.");
      WALKey key = walEntry.getKey();
      cellsToReplicate.addAll(walEntry.getEdit().getCells());
    }
    // group the data by the rowkey.
    Map<BytesKey, List<Cell>> cellsToReplicateByRow =
        cellsToReplicate.stream().collect(groupingBy(k -> new BytesKey(CellUtil.cloneRow(k))));
    LOG.warn(
        "Replicating {} rows, {} cells total.", cellsToReplicateByRow.size(),
        cellsToReplicate.size());

    // Build a row mutation per row.
    List<RowMutations> rowMutationsList = new ArrayList<>(cellsToReplicateByRow.size());
    for (Map.Entry<BytesKey, List<Cell>> cellsByRow : cellsToReplicateByRow.entrySet()) {
      byte[] rowKey = cellsByRow.getKey().array;
      RowMutations rowMutations = new RowMutations(rowKey);
      List<Cell> cellsForRow = cellsByRow.getValue();
      // TODO Make sure that there are < 100K cells per row Mutation

      MutationBuilder mutationBuilder =
          MutationBuilderFactory.getMutationBuilder(cellsForRow.get(0));
      for (Cell cell : cellsForRow) {
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

    Object[] futures = new Object[rowMutationsList.size()];
    try {
      // TODO: Decide if we want to tweak retry policies of the batch puts?
      table.batch(rowMutationsList, futures);
    } catch (Throwable t) {
      LOG.error(
          "Encountered error while replicating wal entry.", t);
      if (t.getCause() != null) {
        LOG.error("chained exception ", t.getCause());
      }
      succeeded = false;
    }
    // Make sure that there were no errors returned via futures.
    for (Object future : futures) {
      if (future != null && future instanceof Throwable) {
        LOG.error(
            "{FUTURES} Encountered error while replicating wal entry.", (Throwable) future);
        succeeded = false;
      }
    }
    return succeeded;
  }
}
