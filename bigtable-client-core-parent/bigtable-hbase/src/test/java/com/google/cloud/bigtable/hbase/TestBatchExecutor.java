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
package com.google.cloud.bigtable.hbase;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRow.Cell;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link BatchExecutor}
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBatchExecutor {

  private static Put randomPut() {
    return new Put(randomBytes(8))
        .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual"), Bytes.toBytes("SomeValue"));
  }

  private static byte[] randomBytes(int count) {
    return Bytes.toBytes(RandomStringUtils.random(count));
  }

  private static Matcher<Result> matchesRow(final Result expected) {
    return new BaseMatcher<Result>() {

      @Override
      public void describeTo(Description description) {
      }

      @Override
      public boolean matches(Object item) {
        try {
          Result.compareResults((Result) item, expected);
          return true;
        } catch (Exception e) {
          return false;
        }
      }

      @Override
      public void describeMismatch(Object item, Description mismatchDescription) {
      }
    };
  }

  @Mock
  private BigtableSession mockBigtableSession;

  @Mock
  private AsyncExecutor mockAsyncExecutor;

  @Mock
  private BulkRead mockBulkRead;

  @Mock
  private BulkMutation mockBulkMutation;

  @Mock
  private ListenableFuture mockFuture;

  private HBaseRequestAdapter requestAdapter;
  @Before
  public void setup() throws InterruptedException {
    final BigtableOptions options = BigtableOptions.builder()
        .setProjectId("projectId")
        .setInstanceId("instanceId")
        .build();
    requestAdapter =
        new HBaseRequestAdapter(options, TableName.valueOf("table"), new Configuration(false));

    MockitoAnnotations.initMocks(this);
    when(mockBulkMutation.add(any(MutateRowsRequest.Entry.class))).thenReturn(mockFuture);
    when(mockAsyncExecutor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class))).thenReturn(mockFuture);
    when(mockBigtableSession.createAsyncExecutor()).thenReturn(mockAsyncExecutor);
    when(mockBigtableSession.createBulkMutation(any(BigtableTableName.class))).thenReturn(mockBulkMutation);
    when(mockBigtableSession.createBulkRead(any(BigtableTableName.class))).thenReturn(mockBulkRead);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(0, Runnable.class).run();
        return null;
      }
    }).when(mockFuture).addListener(any(Runnable.class), any(Executor.class));
    doReturn(true).when(mockFuture).isDone();
  }

  @Test
  public void testGet() throws Exception {
    when(mockBulkRead.add(any(ReadRowsRequest.class))).thenReturn(mockFuture);
    final byte[] key = randomBytes(8);
    FlatRow response = FlatRow.newBuilder().withRowKey(ByteString.copyFrom(key)).build();
    setFuture(ImmutableList.of(response));
    Result[] results = batch(Arrays.asList(new Get(key)));
    Assert.assertTrue(matchesRow(Adapters.FLAT_ROW_ADAPTER.adaptResponse(response)).matches(results[0]));
  }

  @Test
  public void testPut() throws Exception {
    testMutation(randomPut());
  }

  @Test
  public void testDelete() throws Exception {
    testMutation(new Delete(randomBytes(8)));
  }

  @Test
  public void testAppend() throws Exception {
    testMutation(new Append(randomBytes(8)));
  }

  @Test
  public void testIncrement() throws Exception {
    testMutation(new Increment(randomBytes(8)));
  }

  @Test
  public void testRowMutations() throws Exception {
    testMutation(new RowMutations(randomBytes(8)));
  }

  @Test
  public void testShutdownService() throws Exception {
    when(mockAsyncExecutor.mutateRowAsync(any(MutateRowRequest.class)))
        .thenThrow(new IllegalStateException("closed"));
    try {
      batch(Arrays.asList(randomPut()));
    } catch (RetriesExhaustedWithDetailsException e) {
      Assert.assertEquals(1, e.getCauses().size());
      Assert.assertEquals(IOException.class, e.getCause(0).getClass());
    }
  }

  @Test
  public void testAsyncException() throws Exception {
    String message = "Something bad happened";
    when(mockAsyncExecutor.mutateRowAsync(any(MutateRowRequest.class)))
        .thenThrow(new RuntimeException(message));
    try {
      batch(Arrays.asList(randomPut()));
    } catch (RetriesExhaustedWithDetailsException e) {
      Assert.assertEquals(1, e.getCauses().size());
      Assert.assertEquals(IOException.class, e.getCause(0).getClass());
      Assert.assertEquals(RuntimeException.class, e.getCause(0).getCause().getClass());
      Assert.assertEquals(message, e.getCause(0).getCause().getMessage());
    }
  }

  @Test
  public void testPartialResults() throws Exception {
    byte[] key1 = randomBytes(8);
    byte[] key2 = randomBytes(8);
    FlatRow response1 =
        FlatRow
            .newBuilder().withRowKey(ByteString.copyFrom(key1)).addCell(new Cell("cf",
                ByteString.EMPTY, 10, ByteString.copyFromUtf8("hi!"), new ArrayList<String>()))
            .build();

    RuntimeException exception = new RuntimeException("Something bad happened");
    when(mockBulkRead.add(any(ReadRowsRequest.class)))
        .thenReturn(Futures.immediateFuture(response1))
        .thenReturn(Futures.<FlatRow> immediateFailedFuture(exception));

    List<Get> gets = Arrays.asList(new Get(key1), new Get(key2));
    Object[] results = new Object[2];

    try {
      createExecutor(BigtableOptions.getDefaultOptions()).batch(gets, results);
    } catch(RetriesExhaustedWithDetailsException ignored) {
    }
    Assert.assertTrue("first result is a result", results[0] instanceof Result);
    Assert.assertTrue(Bytes.equals(((Result)results[0]).getRow(), key1));
    Assert.assertEquals(exception, results[1]);
  }

  @Test
  public void testGetCallback() throws Exception {
    when(mockBulkRead.add(any(ReadRowsRequest.class))).thenReturn(mockFuture);
    byte[] key = randomBytes(8);
    FlatRow response = FlatRow.newBuilder().withRowKey(ByteString.copyFrom(key)).build();
    setFuture(ImmutableList.of(response));
    final Callback<Result> callback = Mockito.mock(Callback.class);
    List<Get> gets = Arrays.asList(new Get(key));
    createExecutor(BigtableOptions.getDefaultOptions()).batchCallback(gets, new Object[1], callback);

    verify(callback, times(1)).update(same(BatchExecutor.NO_REGION), same(key),
      argThat(matchesRow(Adapters.FLAT_ROW_ADAPTER.adaptResponse(response))));
  }

  @Test
  public void testBatchBulkGets() throws Exception {
    final List<Get> gets = new ArrayList<>(10);
    final List<ListenableFuture<FlatRow>> expected = new ArrayList<>(10);

    gets.add(new Get(Bytes.toBytes("key0")));
    expected.add(Futures.<FlatRow> immediateFuture(null));
    for (int i = 1; i < 10; i++) {
      byte[] row_key = randomBytes(8);
      gets.add(new Get(row_key));
      ByteString key = ByteStringer.wrap(row_key);
      ByteString cellValue = ByteString.copyFrom(randomBytes(8));
      expected.add(Futures.immediateFuture(FlatRow.newBuilder().withRowKey(key)
          .addCell("family", ByteString.EMPTY, System.nanoTime() / 1000, cellValue).build()));
    }

    // Test 10 gets, but return only 9 to test the row not found case.
    when(mockBulkRead.add(any(ReadRowsRequest.class)))
        .then(new Answer<ListenableFuture<FlatRow>>() {
          final AtomicInteger counter = new AtomicInteger();

          @Override
          public ListenableFuture<FlatRow> answer(InvocationOnMock invocation) throws Throwable {
            return expected.get(counter.getAndIncrement());
          }
        });
    ByteString key = ByteStringer.wrap(randomBytes(8));
    ByteString cellValue = ByteString.copyFrom(randomBytes(8));
    FlatRow row = FlatRow.newBuilder().withRowKey(key)
        .addCell("family", ByteString.EMPTY, System.nanoTime() / 1000, cellValue).build();
    when(mockFuture.get()).thenReturn(row);

    BatchExecutor underTest = createExecutor(BigtableOptions.getDefaultOptions());
    Result[] results = underTest.batch(gets);
    verify(mockBulkRead, times(10)).add(any(ReadRowsRequest.class));
    verify(mockBulkRead, times(1)).flush();
    Assert.assertTrue(matchesRow(Result.EMPTY_RESULT).matches(results[0]));
    for (int i = 1; i < results.length; i++) {
      Assert.assertTrue(
        "Expected " + Bytes.toString(gets.get(i).getRow()) + " but was "
            + Bytes.toString(results[i].getRow()),
        Bytes.equals(results[i].getRow(), gets.get(i).getRow()));
    }
  }

  // HELPERS

  private void testMutation(org.apache.hadoop.hbase.client.Row mutation) throws Exception {
    setFuture(Empty.getDefaultInstance());
    Result[] results = batch(Arrays.asList(mutation));
    Assert.assertTrue(matchesRow(Result.EMPTY_RESULT).matches(results[0]));
  }

  protected void setFuture(Object response) throws InterruptedException, ExecutionException {
    when(mockFuture.get()).thenReturn(response);
    when(mockFuture.isDone()).thenReturn(true);
  }

  private BatchExecutor createExecutor(BigtableOptions options) {
    when(mockBigtableSession.getOptions()).thenReturn(options);
    return new BatchExecutor(mockBigtableSession, requestAdapter);
  }

  private Result[] batch(final List<? extends org.apache.hadoop.hbase.client.Row> actions)
      throws Exception {
    return createExecutor(BigtableOptions.getDefaultOptions()).batch(actions);
  }
}
