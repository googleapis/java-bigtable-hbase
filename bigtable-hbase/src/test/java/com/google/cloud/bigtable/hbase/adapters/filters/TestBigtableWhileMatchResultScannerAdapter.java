/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters.filters;

import static org.junit.Assert.*;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.BigtableWhileMatchResultScannerAdapter;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * Unit tests for the {@link BigtableWhileMatchResultScannerAdapter}.
 */
@RunWith(JUnit4.class)
public class TestBigtableWhileMatchResultScannerAdapter {

  @Mock
  private ResponseAdapter<Row, Result> mockRowAdapter;
  
  @Mock
  com.google.cloud.bigtable.grpc.scanner.ResultScanner<Row> mockBigtableResultScanner;

  private BigtableWhileMatchResultScannerAdapter adapter;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    adapter = new BigtableWhileMatchResultScannerAdapter(mockRowAdapter);
  }

  @Test
  public void adapt_noRow() throws IOException {
    when(mockBigtableResultScanner.next()).thenReturn(null);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner);
    assertNull(scanner.next());
    verify(mockBigtableResultScanner).next();
    verifyZeroInteractions(mockRowAdapter);
  }

  @Test
  public void adapt_oneRow() throws IOException {
    Row row = Row.newBuilder().setKey(ByteString.copyFromUtf8("key")).build();
    when(mockBigtableResultScanner.next()).thenReturn(row);
    Result result = new Result();
    when(mockRowAdapter.adaptResponse(same(row))).thenReturn(result);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner);
    assertSame(result, scanner.next());
    verify(mockBigtableResultScanner).next();
    verify(mockRowAdapter).adaptResponse(same(row));
  }

  @Test
  public void adapt_oneRow_hasMatchingLabels() throws IOException {
    Row row = Row.newBuilder().setKey(ByteString.copyFromUtf8("key"))
        .addFamilies(Family.newBuilder().addColumns(Column.newBuilder()
            .addCells(Cell.newBuilder().addLabels("a-in"))
            .addCells(Cell.newBuilder().addLabels("a-out"))))
        .build();
    when(mockBigtableResultScanner.next()).thenReturn(row);
    Result result = new Result();
    when(mockRowAdapter.adaptResponse(same(row))).thenReturn(result);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner);
    assertSame(result, scanner.next());
    verify(mockBigtableResultScanner).next();
    verify(mockRowAdapter).adaptResponse(same(row));
  }

  @Test
  public void adapt_oneRow_hasNoMatchingLabels() throws IOException {
    Row row = Row.newBuilder().setKey(ByteString.copyFromUtf8("key"))
        .addFamilies(Family.newBuilder().addColumns(Column.newBuilder()
            .addCells(Cell.newBuilder().addLabels("a-in"))))
        .build();
    when(mockBigtableResultScanner.next()).thenReturn(row);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner);
    assertNull(scanner.next());
    verify(mockBigtableResultScanner).next();
    verifyZeroInteractions(mockRowAdapter);
  }
}
