package com.google.cloud.bigtable.hbase.replication;

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * This replicator is responsible to apply the WAL logs for a single table for its peer in Cloud
 * Bigtable.
 */
public class CloudBigtableTableReplicator {

  // Not required, We can use normal HashMap with Bytes.ByteArrayComparator in org.apache.hadoop.hbase.util.
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

    boolean canAcceptMutation(Cell mutation);

    void addMutation(Cell mutation) throws IOException;

    void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException;
  }

  static class PutMutationBuilder implements MutationBuilder {

    Put put;

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

    Delete delete;

    public DeleteMutationBuilder(byte[] rowKey) {
      delete = new Delete(rowKey);
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      if (delete == null) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      return CellUtil.isDelete(cell);
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      if (delete == null) {
        throw new IllegalStateException("Can't add mutations to a closed builder");
      }
      /**
       * TODO Decide how to handle permanent errors? Maybe add a config to push such errors to
       * pubsub or a new BT table or a log file.
       * TODO We should offer toggles for timestamp delete, if customers opt in for a best effort timestamp deltes
       * we should do a get and delete. Or maybe a CheckAndMutate
       * default behaviour can be handling as per permanent failure policy (log/pubsub/separate table)
       */
      if (CellUtil.isDeleteType(cell)) {
        LOG.info("Deleting cell " + cell.toString() + " with ts: " + cell.getTimestamp());
        delete.addDeleteMarker(cell);
      } else if (CellUtil.isDeleteColumns(cell)) {
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

  private static final Logger LOG = LoggerFactory.getLogger(CloudBigtableTableReplicator.class);
  private String tableName;
  private Connection connection;
  private final Table table;

  public CloudBigtableTableReplicator(String tableName, Connection connection) throws IOException {
    this.tableName = tableName;
    this.connection = connection;
    this.table = connection.getTable(TableName.valueOf(tableName));
  }

  // Returns true if replication was successful, false otherwise.
  public boolean replicateTable(List<WAL.Entry> walEntriesToReplicate) throws IOException {
    AtomicBoolean erred = new AtomicBoolean(false);
    for (WAL.Entry walEntry : walEntriesToReplicate) {
      WALKey key = walEntry.getKey();

      // TODO Extract processWALEntry method
      LOG.error("Starting WALKey: " + walEntry.getKey().toString());
      List<Cell> cellsToReplicate = walEntry.getEdit().getCells();
      // group the data by the rowkey.
      Map<BytesKey, List<Cell>> cellsToReplicateByRow =
          cellsToReplicate.stream().collect(groupingBy(k -> new BytesKey(CellUtil.cloneRow(k))));
      LOG.warn(
          "Replicating {} rows, {} cells total.",
          cellsToReplicateByRow.size(),
          cellsToReplicate.size());
      List<RowMutations> rowMutationsList = new ArrayList<>(cellsToReplicateByRow.size());
      for (Map.Entry<BytesKey, List<Cell>> cellsByRow : cellsToReplicateByRow.entrySet()) {
        byte[] rowKey = cellsByRow.getKey().array;
        RowMutations rowMutations = new RowMutations(rowKey);
        List<Cell> cellsForRow = cellsByRow.getValue();
        // The list is never empty
        MutationBuilder mutationBuilder =
            MutationBuilderFactory.getMutationBuilder(cellsForRow.get(0));

        for (Cell cell : cellsForRow) {
          if (mutationBuilder.canAcceptMutation(cell)) {
            mutationBuilder.addMutation(cell);
          } else {
            mutationBuilder.buildAndUpdateRowMutations(rowMutations);
            mutationBuilder = MutationBuilderFactory.getMutationBuilder(cell);
          }
        } // Single row exiting.
        // finalize the last mutation which is yet to be closed.
        mutationBuilder.buildAndUpdateRowMutations(rowMutations);
        rowMutationsList.add(rowMutations);
      } // By row exiting
      Object[] futures = new Object[rowMutationsList.size()];
      try {
        LOG.error("Starting batch write");
        table.batch(rowMutationsList, futures);
        LOG.error("Finishing batch write");
      } catch (Throwable t) {
        LOG.error(
            "Encountered error while replicating wal entry: " + walEntry.getKey().toString(), t);
        if (t.getCause() != null) {
          LOG.error("chained exception ", t.getCause());
        }
        erred.set(true);
      }
      for (Object future : futures) {
        if (future != null && future instanceof Throwable) {
          LOG.error(
              "{FUTURES} Encountered error while replicating wal entry: "
                  + walEntry.getKey().toString(),
              (Throwable) future);
          erred.set(true);
        }
      }
      LOG.error("Ending WALKey: " + walEntry.getKey().toString() + " Erred: " + erred.get());
    } // By WAL.ENTRY exiting
    return !erred.get();
  }
}
