/*
 * Copyright 2018 Google LLC
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

package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.hbase.AbstractTestSnapshot;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAsyncSnapshots extends AbstractTestSnapshot {

  @Before
  public void setUp() throws ExecutionException, InterruptedException, IOException {
    values = createAndPopulateTable(tableName);
  }

  private AsyncAdmin getAsyncAdmin() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getAdmin();
  }

  @Test
  public void testListSnapshotsWithNullAndEmptyString() throws IOException {
    Exception actualError = null;
    try {
      listSnapshotsSize((String) null);
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNotNull(actualError);
    assertTrue(actualError instanceof NullPointerException);
    actualError = null;

    try {
      listSnapshotsSize((Pattern) null);
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNotNull(actualError);
    assertTrue(actualError instanceof NullPointerException);

    assertEquals(0, listSnapshotsSize(""));
  }

  @Test
  public void listSnapshots() throws IOException, InterruptedException, ExecutionException {
    String snapshot1 = newSnapshotId();
    snapshot(snapshot1, tableName);

    String snapshot2 = newSnapshotId();
    snapshot(snapshot2, tableName);
    try {
      List<SnapshotDescription> snapshotDescriptions = getAsyncAdmin().listSnapshots().get();
      List<String> descStrings = new ArrayList<>();
      for (SnapshotDescription snapshotDescription : snapshotDescriptions) {
        descStrings.add(snapshotDescription.getName());
      }
      assertThat(descStrings, hasItems(snapshot1, snapshot2));
    } finally {
      deleteSnapshot(snapshot1);
      deleteSnapshot(snapshot2);
    }
  }

  private TableDescriptor createDescriptor(TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(SharedTestEnvRule.COLUMN_FAMILY).build())
        .build();
  }

  @Override
  protected void snapshot(String snapshotName, TableName tableName) throws IOException {
    try {
      getAsyncAdmin().snapshot(snapshotName, tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while creating snapshot: " + e.getCause());
    }
  }

  protected Map<String, Long> createAndPopulateTable(TableName tableName) throws IOException {
    try {
      getAsyncAdmin().createTable(createDescriptor(tableName)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while creating table: " + e.getCause());
    }

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

  @Override
  protected void deleteSnapshot(String snapshotName) throws IOException {
    try {
      getAsyncAdmin().deleteSnapshot(snapshotName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while deleting snapshot: " + e.getCause());
    }
  }

  @Override
  protected boolean tableExists(TableName tableName) throws IOException {
    try {
      return getAsyncAdmin().tableExists(tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while checking table exists: " + e.getCause());
    }
  }

  @Override
  protected void disableTable(TableName tableName) throws IOException {
    try {
      getAsyncAdmin().disableTable(tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while disabling table: " + e.getCause());
    }
  }

  @Override
  protected void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    try {
      getAsyncAdmin().cloneSnapshot(snapshotName, tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while creating clone of snapshot: " + e.getCause());
    }
  }

  @Override
  protected int listSnapshotsSize(String regEx) throws IOException {
    try {
      Pattern pattern = Pattern.compile(regEx);
      return getAsyncAdmin().listSnapshots(pattern).get().size();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while listing snapshots size: " + e.getCause());
    }
  }

  @Override
  protected int listSnapshotsSize(Pattern pattern) throws IOException {
    try {
      return getAsyncAdmin().listSnapshots(pattern).get().size();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while listing snapshots size: " + e.getCause());
    }
  }

  @Override
  protected void deleteTable(TableName tableName) throws IOException {
    try {
      getAsyncAdmin().deleteTable(tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while deleting table: " + e.getCause());
    }
  }
}
