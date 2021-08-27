package com.google.cloud.bigtable.hbase.replication;

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This replicator is responsible to apply the WAL logs for a single table for its peer in Cloud
 * Bigtable.
 */
public class CloudBigtableTableReplicator {

  private final Logger LOG = LoggerFactory.getLogger(CloudBigtableTableReplicator.class);
  private String tableName;
  private Connection connection;

  public CloudBigtableTableReplicator(String tableName, Connection connection) {
    this.tableName = tableName;
    this.connection = connection;
  }

  // Returns true if replication was successful, false otherwise.
  public boolean replicateTable(List<WAL.Entry> walEntriesToReplicate) throws IOException {
    AtomicBoolean erred = new AtomicBoolean();
    BufferedMutatorParams bmParams = new BufferedMutatorParams(TableName.valueOf(tableName));
    bmParams.listener(
        new ExceptionListener() {
          @Override
          public void onException(
              RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
              throws RetriesExhaustedWithDetailsException {
            LOG.error("Failed to write in BufferedMutator", e);
            erred.set(true);
          }
        });
    BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf(tableName));
    try {
      for (WAL.Entry walEntry : walEntriesToReplicate) {
        WALKey key = walEntry.getKey();

        // TODO Extract processWALEntry method
        LOG.debug("Starting WALKey: " + walEntry.getKey().toString());
        List<Cell> cellsToReplicate = walEntry.getEdit().getCells();
        // TODO Handle multi row deletes. Basically, pause everything on the table and handle that
        // delete in isolation
        // group the data by the rowkey.
        Map<byte[], List<Cell>> cellsToReplicateByRow =
            cellsToReplicate.stream().collect(groupingBy(CellUtil::cloneRow));
        LOG.warn(
            "Replicating {} rows, {} cells total.",
            cellsToReplicateByRow.size(),
            cellsToReplicate.size());
        for (Map.Entry<byte[], List<Cell>> cellsByRow : cellsToReplicateByRow.entrySet()) {
          byte[] rowKey = cellsByRow.getKey();
          try {
            bufferedMutator.mutate(createPutsForRow(rowKey, cellsByRow.getValue()));
          } catch (IOException e) {
            erred.set(true);
          }
        } // By row exiting
        LOG.error("Ending WALKey: " + walEntry.getKey().toString());
      } // By WAL.ENTRY exiting
    } finally {
      try {
        // Flush one last time to ensure that any entries within this WAL entry are processed
        // before we start processing the next WAL entry. Guarantees that there is no re-ordering
        // of mutations between the WAL entries.
        bufferedMutator.flush();
      } catch (Exception e) {
        LOG.error("Failed to flush bufferedMutator.", e);
        erred.set(true);
      }
      bufferedMutator.close();
    }
    return !erred.get();
  }

  private List<Put> createPutsForRow(byte[] rowKey, List<Cell> cellsByRow) throws IOException {
    // Since all the cells belong to the same row, Create a put
    List<Put> putsForRow = new ArrayList<>();
    Put put = new Put(rowKey);
    // Track the cells added here, its more efficient than put.size().
    int cellsAdded = 0;
    for (Cell rowCell : cellsByRow) {
      try {
        // Put all mutations for a row from this WAL entry into a single PUT, this will commit
        // all the mutations atomically. There is no ordering guaratees across rows, they can
        // be sent concurrently to CBT.
        put.add(rowCell);
        cellsAdded++;
        if (cellsAdded % 100000 == 0) {
          LOG.error("PUT Exeeding 100K");
          // TODO MAKE IT USER CONFIGURABLE, ATOMICITY ISSUES
          putsForRow.add(put);
          // Split the mutations into multiple PUTs, this breaks the atomicity guarantee.
          put = new Put(rowKey);
        }
      } catch (IOException e) {
        LOG.error(
            "ERROR adding entry to the PUT for table: "
                + this.tableName
                + " row: "
                + Bytes.toStringBinary(rowKey)
                + " cols: "
                // TODO: Just list the column name, not value.
                + rowCell.toString(),
            e);
        throw e;
      }
    }
    putsForRow.add(put);
    return putsForRow;
  }
}
