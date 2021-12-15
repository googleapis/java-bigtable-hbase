package com.google.cloud.bigtable.hbase.replication.adapters;

import com.google.cloud.bigtable.hbase.adapters.DeleteAdapter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for adapters that translate CBT incompatible mutations into compatible mutations. See
 * <a href="https://cloud.google.com/bigtable/docs/hbase-differences#mutations_and_deletions">cbt
 * docs</a> for detailed list of incompatible mutations.
 *
 * Subclasses must expose the constructor ChildClass(Configuration, MetricSource, Table).
 */
public abstract class IncompatibleMutationAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(IncompatibleMutationAdapter.class);
  // Destination table for the adapted mutations.
  private final Table destinationTable;
  private final Configuration conf;
  private final MetricsSource metricSource;

  public static final String INCOMPATIBLE_MUTATION_METRIC_KEY = "bigtableIncompatibleMutations";
  public static final String DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY = "bigtableDroppedIncompatibleMutations";

  private void incrementDroppedIncompatibleMutations() {
    metricSource.incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }

  private void incrementIncompatibleMutations() {
    metricSource.incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }

  /**
   * Creates an IncompatibleMutationAdapter with HBase configuration, MetricSource, and CBT Table.
   *
   * All subclasses must expose this constructor.
   *
   * @param conf HBase configuration. All the configurations required by subclases should come from
   * here.
   * @param metricsSource Hadoop metric source exposed by HBase Replication Endpoint.
   * @param destinationTable CBT table taht is destination of the replicated edits. This reference
   * help the subclasses to query destination table for certain incompatible mutation.
   */
  public IncompatibleMutationAdapter(Configuration conf, MetricsSource metricsSource,
      Table destinationTable) {
    this.conf = conf;
    this.destinationTable = destinationTable;
    this.metricSource = metricsSource;
    // Make sure that the counters show up.
    metricsSource.incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    metricsSource.incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  private boolean isValidDelete(Cell delete) {
    try {
      DeleteAdapter.isValid(delete);
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
  public final List<Cell> adaptIncompatibleMutations(WAL.Entry walEntry) {
    List<Cell> cellsToAdapt = walEntry.getEdit().getCells();
    List<Cell> returnedCells = new ArrayList<>(cellsToAdapt.size());
    int index = 0;
    for (Cell cell : cellsToAdapt) {
      // All puts are valid.
      if (cell.getTypeByte() == KeyValue.Type.Put.getCode()) {
        returnedCells.add(cell);
        continue;
      }

      // Validate the delete is compatible.
      if (CellUtil.isDelete(cell)) {
        if (isValidDelete(cell)) {
          returnedCells.add(cell);
          continue;
        }

        // Try to adapt the incompatible delete
        try {
          LOG.debug("Encountered incompatible mutation: " + cell);
          incrementIncompatibleMutations();
          returnedCells.addAll(adaptIncompatibleMutation(walEntry, index));
        } catch (UnsupportedOperationException use) {
          // Drop the mutation, not dropping it will lead to stalling of replication.
          incrementDroppedIncompatibleMutations();
          LOG.error("DROPPING INCOMPATIBLE MUTATION: " + cell);
        }
        continue;
      }

      // Replication should only produce PUT and Delete mutation. Appends/Increments are converted
      // to PUTs. Log the unexpected mutation and drop it as we don't know what CBT client will do.
      LOG.error("DROPPING UNEXPECTED TYPE OF MUTATION : " + cell);
      incrementIncompatibleMutations();
      incrementDroppedIncompatibleMutations();

      index++;
    }
    return returnedCells;
  }

  /**
   * Adapts an incompatible mutation into a compatible mutation. Must throws {@link
   * UnsupportedOperationException} if it can't adapt the mutation.
   *
   * @param walEntry the WAL entry for the cell to Adapt. The wal entry provides context around the
   * cell to be adapted, things like commit timestamp and other deletes in the entry.
   * @param index The index of the cell to adapt.
   */
  protected abstract List<Cell> adaptIncompatibleMutation(WAL.Entry walEntry, int index);
}
