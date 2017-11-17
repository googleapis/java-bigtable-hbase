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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.protobuf.ByteString;

import io.opencensus.trace.Span;

/**
 * Unit tests for the {@link BigtableWhileMatchResultScannerAdapter}.
 */
@RunWith(JUnit4.class)
public class TestBigtableWhileMatchResultScannerAdapter {

  @Mock
  private ResponseAdapter<FlatRow, Result> mockRowAdapter;

  @Mock
  com.google.cloud.bigtable.grpc.scanner.ResultScanner<FlatRow> mockBigtableResultScanner;

  @Mock
  Span mockSpan;

  private BigtableWhileMatchResultScannerAdapter adapter;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    adapter = new BigtableWhileMatchResultScannerAdapter(mockRowAdapter);
  }

  @Test
  public void adapt_noRow() throws IOException {
    when(mockBigtableResultScanner.next()).thenReturn(null);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner, mockSpan);
    assertNull(scanner.next());
    verify(mockBigtableResultScanner).next();
    verifyZeroInteractions(mockRowAdapter);
    verify(mockSpan, times(1)).end();
  }

  @Test
  public void adapt_oneRow() throws IOException {
    FlatRow row = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key")).build();
    when(mockBigtableResultScanner.next()).thenReturn(row);
    Result result = new Result();
    when(mockRowAdapter.adaptResponse(same(row))).thenReturn(result);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner, mockSpan);
    assertSame(result, scanner.next());
    verify(mockBigtableResultScanner).next();
    verify(mockRowAdapter).adaptResponse(same(row));
    verify(mockSpan, times(0)).end();
  }

  @Test
  public void adapt_oneRow_hasMatchingLabels() throws IOException {
    FlatRow row = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key"))
        .addCell("", ByteString.EMPTY, 0, ByteString.EMPTY, Arrays.asList("a-in"))
        .addCell("", ByteString.EMPTY, 0, ByteString.EMPTY, Arrays.asList("a-out"))
        .build();
    when(mockBigtableResultScanner.next()).thenReturn(row);
    Result result = new Result();
    when(mockRowAdapter.adaptResponse(same(row))).thenReturn(result);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner, mockSpan);
    assertSame(result, scanner.next());
    verify(mockBigtableResultScanner).next();
    verify(mockRowAdapter).adaptResponse(same(row));
    verify(mockSpan, times(0)).end();
  }

  @Test
  public void adapt_oneRow_hasNoMatchingLabels() throws IOException {
    FlatRow row = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key"))
        .addCell("", ByteString.EMPTY, 0, ByteString.EMPTY, Arrays.asList("a-in"))
        .build();
    when(mockBigtableResultScanner.next()).thenReturn(row);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner, mockSpan);
    assertNull(scanner.next());
    verify(mockSpan, times(1)).end();
    verify(mockBigtableResultScanner).next();
    verifyZeroInteractions(mockRowAdapter);
  }
}
