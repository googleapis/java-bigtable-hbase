/*
 * Copyright 2015 Google Inc.
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

import static java.util.stream.Collectors.groupingBy;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseToCloudBigtableReplicationEndpoint extends BaseReplicationEndpoint {

  private final Logger LOG = LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);
  private Connection connection;
  private Table table;

  private static final String PROJECT_ID_KEY = "google.bigtable.project_id";
  private static final String INSTANCE_ID_KEY = "google.bigtable.instance";
  private static final String TABLE_ID_KEY = "google.bigtable.table";

  /**
   * Basic endpoint that listens to CDC from HBase and replicates to Cloud Bigtable.
   * This implementation is not very efficient as it is single threaded.
   */
  public HbaseToCloudBigtableReplicationEndpoint() {
    super();
    LOG.error("Creating replication endpoint to CBT. ");
  }

  @Override
  protected void doStart() {
    LOG.error("Starting replication to CBT. ");

    String projectId = ctx.getConfiguration().get(PROJECT_ID_KEY);
    String instanceId = ctx.getConfiguration().get(INSTANCE_ID_KEY);
    String tableId = ctx.getConfiguration().get(TABLE_ID_KEY);

    LOG.error("Connecting to " + projectId + ":" + instanceId + " table: " + tableId);
    try {
      connection = BigtableConfiguration.connect(projectId, instanceId);
      table = connection.getTable(TableName.valueOf(tableId));

      LOG.error("Created a connection to CBT. ");
    } catch (IOException e) {
      LOG.error("Error creating connection ", e);
    }

    notifyStarted();
  }

  @Override
  protected void doStop() {

    LOG.error("Stopping replication to CBT. ");
    try {
      connection.close();
    } catch (IOException e) {
      LOG.error("Failed to close connection " , e);
    }
    notifyStopped();
  }

  @Override
  public UUID getPeerUUID() {
    // TODO: Should this be fixed and retained across reboots?
    return UUID.randomUUID();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {

    List<WAL.Entry> entries = replicateContext.getEntries();
    LOG.error("In CBT replicate: wal size: " + entries.size());

    final Map<String, List<WAL.Entry>> entriesByTable =
        entries.stream()
            .collect(groupingBy(entry -> entry.getKey().getTablename().getNameAsString()));
    AtomicBoolean errored = new AtomicBoolean(false);

    entriesByTable.entrySet().stream()
        .forEach(
            entry -> {
              final String tableName = entry.getKey();
              final List<WAL.Entry> tableEntries = entry.getValue();

              tableEntries.forEach(
                  tblEntry -> {
                    List<Cell> cells = tblEntry.getEdit().getCells();

                    // group the data by the rowkey.
                    Map<byte[], List<Cell>> columnsByRow =
                        cells.stream().collect(groupingBy(CellUtil::cloneRow));

                    // build the list of rows.
                    columnsByRow.entrySet().stream()
                        .forEach(
                            rowcols -> {
                              final byte[] rowkey = rowcols.getKey();
                              Put put = new Put(rowkey);
                              put.addColumn("cf".getBytes(), "table_name".getBytes(),
                                  System.currentTimeMillis(), tableName.getBytes());
                              final List<Cell> columns = rowcols.getValue();
                              LOG.warn(
                                  "Writing entry Table: "
                                      + entry.getKey()
                                      + " Row: " + Bytes.toStringBinary(rowkey) + " cols: "
                                      + columns.toString());
                              cells.stream()
                                  .forEach(
                                      cell -> {
                                        try {
                                          put.add(cell);
                                        } catch (IOException e) {
                                          errored.set(true);
                                          LOG.error(
                                              "ERROR Writing entry Table: "
                                                  + entry.getKey() + " row: " + Bytes
                                                  .toStringBinary(rowkey)
                                                  + " cols: "
                                                  + columns.toString());
                                        }
                                      });
                              try {
                                table.put(put);
                              } catch (IOException e) {
                                errored.set(true);
                                LOG.error(
                                    "ERROR Writing put: "
                                        + Bytes.toStringBinary(rowkey)
                                        + " "
                                        + columns.toString());
                              }
                            });
                  });
            });
    return !errored.get();
  }
}
