package com.google.cloud.bigtable.hbase.adapters;

import static org.junit.Assert.*;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.Row;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
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

