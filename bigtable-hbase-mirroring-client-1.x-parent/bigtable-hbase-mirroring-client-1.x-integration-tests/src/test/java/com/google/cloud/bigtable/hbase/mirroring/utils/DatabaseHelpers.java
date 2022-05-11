/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Comparators;
import com.google.common.base.Predicate;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class DatabaseHelpers {
  private final ExecutorServiceRule executorServiceRule;
  private final ConnectionRule connectionRule;

  public DatabaseHelpers(ConnectionRule connectionRule, ExecutorServiceRule executorServiceRule) {
    this.executorServiceRule = executorServiceRule;
    this.connectionRule = connectionRule;
  }

  public enum DatabaseSelector {
    PRIMARY,
    SECONDARY
  }

  public int countCells(TableName tableName, DatabaseSelector databaseSelector) throws IOException {
    return countRowsCells(tableName, databaseSelector, new Scan()).cells;
  }

  public int countRows(TableName tableName, DatabaseSelector databaseSelector) throws IOException {
    return countRowsCells(tableName, databaseSelector, new Scan()).rows;
  }

  public int countRows(TableName tableName, DatabaseSelector databaseSelector, Scan scan)
      throws IOException {
    return countRowsCells(tableName, databaseSelector, scan).rows;
  }

  public static class RowCellsCount {
    public final int rows;
    public final int cells;

    public RowCellsCount(int rows, int cells) {
      this.rows = rows;
      this.cells = cells;
    }
  }

  public RowCellsCount countRowsCells(
      TableName tableName, DatabaseSelector databaseSelector, Scan scan) throws IOException {
    int rowsCounter = 0;
    int cellsCounter = 0;
    try (MirroringConnection mirroringConnection = createConnection()) {
      Connection connection = selectConnection(mirroringConnection, databaseSelector);
      try (Table t1 = connection.getTable(tableName)) {
        try (ResultScanner scanner = t1.getScanner(scan)) {
          for (Result r : scanner) {
            rowsCounter++;
            CellScanner cellScanner = r.cellScanner();
            do {
              if (cellScanner.current() != null) {
                cellsCounter++;
              }
            } while (cellScanner.advance());
          }
        }
      }
    }
    return new RowCellsCount(rowsCounter, cellsCounter);
  }

  public Connection selectConnection(
      MirroringConnection mirroringConnection, DatabaseSelector databaseSelector) {
    switch (databaseSelector) {
      case PRIMARY:
        return mirroringConnection.getPrimaryConnection();
      case SECONDARY:
        return mirroringConnection.getSecondaryConnection();
      default:
        throw new UnsupportedOperationException("Invalid database selector");
    }
  }

  public void fillTable(
      TableName tableName1, int numberOfRows, byte[] columnFamily, byte[]... qualifiers)
      throws IOException {
    try (MirroringConnection connection = createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int id = 0; id < numberOfRows; ) {
          List<Put> puts = new ArrayList<>();
          for (int j = 0; j < 100 && id < numberOfRows; j++) {
            byte[] rowKey = Longs.toByteArray(id);
            Put put = new Put(rowKey);
            for (byte[] qualifier : qualifiers) {
              put.addColumn(columnFamily, qualifier, id, rowKey);
            }
            puts.add(put);
            id++;
          }
          t1.put(puts);
        }
      }
    }
  }

  public void verifyTableConsistency(TableName tableName) throws IOException {
    verifyTableConsistency(
        tableName,
        new Predicate<byte[]>() {
          @Override
          public boolean apply(@NullableDecl byte[] bytes) {
            return false;
          }
        });
  }

  public void verifyTableConsistency(TableName tableName, Predicate<byte[]> secondaryErrorPredicate)
      throws IOException {
    try (MirroringConnection connection = createConnection()) {
      try (Table t1 = connection.getPrimaryConnection().getTable(tableName)) {
        try (Table t2 = connection.getSecondaryConnection().getTable(tableName)) {
          Scan scan = new Scan();
          try (ResultScanner s1 = t1.getScanner(scan)) {
            try (ResultScanner s2 = t2.getScanner(scan)) {
              Iterator<Result> i1 = s1.iterator();
              Iterator<Result> i2 = s2.iterator();
              while (i1.hasNext()) {
                Result r1 = i1.next();
                if (!secondaryErrorPredicate.apply(r1.getRow())) {
                  if (!i2.hasNext()) {
                    fail(
                        String.format(
                            "Expected element in secondary table corresponding to entry in primary table: %s",
                            r1));
                  }
                  Result r2 = i2.next();
                  assertTrue(
                      String.format("Index: %s, r1: %s, r2: %s", 0, r1, r2),
                      Comparators.resultsEqual(r1, r2));
                }
              }
              assertThat(i2.hasNext()).isFalse();
            }
          }
        }
      }
    }
  }

  public MirroringConnection createConnection() throws IOException {
    return connectionRule.createConnection(this.executorServiceRule.executorService);
  }

  public MirroringConnection createConnection(Configuration configuration) throws IOException {
    return connectionRule.createConnection(this.executorServiceRule.executorService, configuration);
  }
}
