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

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_DELETE_FAMILY_WRITE_THRESHOLD_IN_MILLIS;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DELETE_FAMILY_WRITE_THRESHOLD_KEY;
import static org.apache.hadoop.hbase.HConstants.LATEST_TIMESTAMP;

import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Approximates the incompatible mutations to the nearest compatible mutations when possible.
 * Practically, converts DeleteFamilyBeforeTimestamp to DeleteFamily when delete is requested before
 * "now".
 */
@InterfaceAudience.Private
public class ApproximatingIncompatibleMutationAdapter extends IncompatibleMutationAdapter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ApproximatingIncompatibleMutationAdapter.class);

  private final int deleteFamilyWriteTimeThreshold;

  public ApproximatingIncompatibleMutationAdapter(
      Configuration conf, MetricsExporter metricsExporter, Connection connection) {
    super(conf, metricsExporter, connection);

    deleteFamilyWriteTimeThreshold =
        conf.getInt(
            DELETE_FAMILY_WRITE_THRESHOLD_KEY, DEFAULT_DELETE_FAMILY_WRITE_THRESHOLD_IN_MILLIS);
  }

  @Override
  protected List<Cell> adaptIncompatibleMutation(BigtableWALEntry walEntry, int index) {
    long walWriteTime = walEntry.getWalWriteTime();
    Cell cell = walEntry.getCells().get(index);
    if (CellUtil.isDeleteFamily(cell)) {
      // TODO Check if its epoch is millis or micros
      // deleteFamily is auto translated to DeleteFamilyBeforeTimestamp(NOW). the WAL write happens
      // later. So walkey.writeTime() should be >= NOW.
      if (walWriteTime >= cell.getTimestamp()
          && cell.getTimestamp() + deleteFamilyWriteTimeThreshold >= walWriteTime) {
        return Arrays.asList(
            new KeyValue(
                CellUtil.cloneRow(cell),
                CellUtil.cloneFamily(cell),
                (byte[]) null,
                LATEST_TIMESTAMP,
                KeyValue.Type.DeleteFamily));
      } else {
        LOG.warn(
            "Dropping incompatible mutation (DeleteFamilyBeforeTimestamp): "
                + cell
                + " cell time: "
                + cell.getTimestamp()
                + " walTime: "
                + walWriteTime
                + " DELTA: "
                + (walWriteTime
                    - cell.getTimestamp()
                    + " With threshold "
                    + deleteFamilyWriteTimeThreshold));
      }
    }
    // Can't convert any other type of mutation.
    throw new UnsupportedOperationException("Unsupported deletes: " + cell);
  }
}
