package com.google.cloud.bigtable.hbase.adapters;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapterContext.Action;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Unit tests for the {@link BigtableResultScannerAdapter}.
 */
@RunWith(JUnit4.class)
public class TestBigtableResultScannerAdapter {
  
  @Mock
  ResponseAdapter<Row, Result> mockRowAdapter;

  @Mock
  com.google.cloud.bigtable.grpc.scanner.ResultScanner<Row> mockBigtableResultScanner;

  private BigtableResultScannerAdapter adapter;
  
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    adapter = new BigtableResultScannerAdapter(mockRowAdapter);
  }

  @Test
  public void adapt_noRow() throws IOException {
    when(mockBigtableResultScanner.next()).thenReturn(null);
    ResponseAdapterContext context = new ResponseAdapterContext(new HashSet<String>());
    ResultScanner scanner = adapter.adapt(context, mockBigtableResultScanner);
    assertNull(scanner.next());
  }

  @Test
  public void adapt_oneRow() throws IOException {
    ResponseAdapterContext context = new ResponseAdapterContext(new HashSet<String>());
    Result result = new Result();
    intitAdapt(context, Action.NEXT, result);
    ResultScanner scanner = adapter.adapt(context, mockBigtableResultScanner);
    assertSame(result, scanner.next());
    assertEquals(Action.NEXT, context.getAction());
  }

  private void intitAdapt(ResponseAdapterContext context, final Action action, final Result result)
      throws IOException {
    Row row = Row.newBuilder().setKey(ByteString.copyFromUtf8("key")).build();
    when(mockBigtableResultScanner.next()).thenReturn(row); 
    when(mockRowAdapter.adaptResponse(context, row)).thenAnswer(new Answer<Result>() {
      @Override
      public Result answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        ResponseAdapterContext context = (ResponseAdapterContext) args[0];
        context.setAction(action);
        return result;
      }});
  }
}

