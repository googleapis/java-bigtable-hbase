/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;

public class TestSnapshots extends AbstractTestSnapshot {

  @Before
  public void setUp() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      // Setup a prefix to avoid collisions between concurrent test runs
      prefix = String.format("020%d", System.currentTimeMillis());

      // clean up stale backups
      String stalePrefix =
          String.format("020%d", System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

      for (SnapshotDescription snapshotDescription : admin.listSnapshots()) {
        int i = snapshotDescription.getName().lastIndexOf("/");
        String backupId = snapshotDescription.getName().substring(i + 1);
        if (backupId.endsWith(TEST_BACKUP_SUFFIX) && stalePrefix.compareTo(backupId) > 0) {
          LOG.info("Deleting old snapshot: " + backupId);
          admin.deleteSnapshot(backupId);
        }
      }
      values = createAndPopulateTable(tableName);
    }
  }

  @Override
  protected void snapshot(String snapshotName, TableName tableName)
      throws IOException, InterruptedException {
    try (Admin admin = getConnection().getAdmin()) {
      createBackupAndWait(admin, snapshotName, tableName);
    }
  }

  @Override
  protected int listSnapshotsSize() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.listSnapshots().size();
    }
  }

  @Override
  protected int listSnapshotsSize(String regEx) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.listSnapshots(regEx).size();
    }
  }

  @Override
  protected void deleteSnapshot(String snapshotName) throws IOException, InterruptedException {
    try (Admin admin = getConnection().getAdmin()) {
      deleteBackupAndWait(admin, snapshotName);
    }
  }

  @Override
  protected boolean tableExists(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.tableExists(tableName);
    }
  }

  @Override
  protected void disableTable(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.disableTable(tableName);
    }
  }

  @Override
  protected void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.cloneSnapshot(snapshotName, tableName);
    }
  }

  @Override
  protected int listSnapshotsSize(Pattern pattern) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.listSnapshots(pattern).size();
    }
  }

  @Override
  protected void deleteTable(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.deleteTable(tableName);
    }
  }

  protected Map<String, Long> createAndPopulateTable(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      HTableDescriptor descriptor = new HTableDescriptor(tableName);
      descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
      admin.createTable(descriptor);

      Map<String, Long> values = new HashMap<>();
      try (Table table = getConnection().getTable(tableName)) {
        List<Put> puts = new ArrayList<>();
        for (long i = 0; i < 10; i++) {
          final UUID rowKey = UUID.randomUUID();
          byte[] row = Bytes.toBytes(rowKey.toString());
          values.put(rowKey.toString(), i);
          puts.add(new Put(row).addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes(i)));
        }
        table.put(puts);
      }
      return values;
    }
  }

  protected void createBackupAndWait(Admin admin, String backupId, TableName tableName)
      throws InterruptedException, IOException {
    admin.snapshot(backupId, tableName);
    for (int i = 0; i < BACKOFF_DURATION.length; i++) {
      List<SnapshotDescription> snapshotDescriptions =
          admin.listSnapshots(Pattern.compile(backupId));
      if (!snapshotDescriptions.isEmpty()) {
        return;
      }

      Thread.sleep(BACKOFF_DURATION[i] * 1000);
    }

    fail("Creating Backup Timeout");
  }

  protected void deleteBackupAndWait(Admin admin, String backupId)
      throws InterruptedException, IOException {
    admin.deleteSnapshot(backupId);
    for (int i = 0; i < BACKOFF_DURATION.length; i++) {
      List<SnapshotDescription> snapshotDescriptions =
          admin.listSnapshots(Pattern.compile(backupId));
      if (snapshotDescriptions.isEmpty()) {
        return;
      }

      Thread.sleep(BACKOFF_DURATION[i] * 1000);
    }

    fail("Creating Backup Timeout");
  }
}
