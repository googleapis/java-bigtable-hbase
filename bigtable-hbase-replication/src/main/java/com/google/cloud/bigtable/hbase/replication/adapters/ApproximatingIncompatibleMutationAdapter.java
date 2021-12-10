package com.google.cloud.bigtable.hbase.replication.adapters;

import static org.apache.hadoop.hbase.HConstants.LATEST_TIMESTAMP;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.wal.WAL;

/**
 * Approximates the incompatible mutations to nearest compatible mutations when possible.
 * Practically, converts DeleteFamiliBeforeTimestamp to DeleteFamily when delete is requested before
 * "now".
 */
public class ApproximatingIncompatibleMutationAdapter extends IncompatibleMutationAdapter {
  // TODO rename
  private static final String DELETE_FAMILY_WRITE_THRESHOLD_KEY = "google.bigtable.deletefamily.threshold";
  private static final int DEFAULT_DELETE_FAMILY_WRITE_THRESHOLD_IN_MILLIS = 100;

  private final int deleteFamilyWriteTimeThreshold;

  public ApproximatingIncompatibleMutationAdapter(Configuration conf, MetricsSource metricsSource, Table table) {
    super(conf, metricsSource, table);

    deleteFamilyWriteTimeThreshold = conf.getInt(DELETE_FAMILY_WRITE_THRESHOLD_KEY,
        DEFAULT_DELETE_FAMILY_WRITE_THRESHOLD_IN_MILLIS);
  }

  @Override
  protected List<Cell> adaptIncompatibleMutation(WAL.Entry walEntry, int index) {
    Cell cell = walEntry.getEdit().getCells().get(index);
    if (CellUtil.isDeleteFamily(cell)) {
      // TODO Check if its epoch is millis or micros
      if (walEntry.getKey().getWriteTime() >= cell.getTimestamp() &&
          cell.getTimestamp() + 100 > walEntry.getKey().getWriteTime()) {
        return Arrays.asList(
            new KeyValue(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell), (byte[]) null,
                LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily));
      }
    }
    throw new UnsupportedOperationException("Unsupported deletes: " + cell);
  }
}
