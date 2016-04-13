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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.MutateRowsRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.BigtableZeroCopyByteStringUtil;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link BatchExecutor}
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBatchExecutor {

  BigtableOptions DEFAULT_OPTIONS = new BigtableOptions.Builder().build();

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
  private AsyncExecutor mockAsyncExecutor;
  @Mock
  private BigtableDataClient mockClient;
  @Mock
  private ListenableFuture mockFuture;

  private HBaseRequestAdapter requestAdapter =
      new HBaseRequestAdapter(new BigtableClusterName("project", "zone", "cluster"),
          TableName.valueOf("table"), new Configuration(false));

  @Before
  public void setup() throws InterruptedException {
    MockitoAnnotations.initMocks(this);
    when(mockAsyncExecutor.readRowsAsync(any(ReadRowsRequest.class))).thenReturn(mockFuture);
    when(mockAsyncExecutor.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(mockFuture);
    when(mockAsyncExecutor.mutateRowsAsync(any(MutateRowsRequest.class))).thenReturn(mockFuture);
    when(mockAsyncExecutor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class))).thenReturn(mockFuture);
    when(mockAsyncExecutor.getClient()).thenReturn(mockClient);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(0, Runnable.class).run();
        return null;
      }
    }).when(mockFuture).addListener(any(Runnable.class), any(Executor.class));
  }

  @Test
  public void testGet() throws Exception {
    final Row response = Row.getDefaultInstance();
    when(mockFuture.get()).thenReturn(ImmutableList.of(response));
    Result[] results = batch(Arrays.asList(new Get(randomBytes(8))));
    Assert.assertTrue(matchesRow(Adapters.ROW_ADAPTER.adaptResponse(response)).matches(results[0]));
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
  public void testGetCallback() throws Exception {
    Row response = Row.getDefaultInstance();
    when(mockFuture.get()).thenReturn(ImmutableList.of(response));
    final Callback<Result> callback = Mockito.mock(Callback.class);
    byte[] key = randomBytes(8);
    List<Get> gets = Arrays.asList(new Get(key));
    createExecutor(DEFAULT_OPTIONS).batchCallback(gets, new Result[1], callback);

    verify(callback, times(1)).update(same(BatchExecutor.NO_REGION), same(key),
      argThat(matchesRow(Adapters.ROW_ADAPTER.adaptResponse(response))));
  }

  @Test
  public void testBatchBulkGets() throws Exception {
    // Test 10 gets, but return only 9 to test the row not found case.
    final List<Get> gets = new ArrayList<>(10);

    gets.add(new Get(Bytes.toBytes("key0")));
    for (int i = 1; i < 10; i++) {
      byte[] row_key = randomBytes(8);
      gets.add(new Get(row_key));
    }
    ResultScanner<Row> mockScanner = Mockito.mock(ResultScanner.class);
    when(mockClient.readRows(any(ReadRowsRequest.class))).thenReturn(mockScanner);
    final AtomicInteger counter = new AtomicInteger();
    when(mockScanner.next()).then(new Answer<Row>() {
      @Override
      public Row answer(InvocationOnMock invocation) throws Throwable {
        int current = counter.incrementAndGet();
        if (current == 10) {
          return null;
        }
        ByteString key = BigtableZeroCopyByteStringUtil.wrap(gets.get(current).getRow());
        ByteString cellValue = ByteString.copyFrom(randomBytes(8));
        com.google.bigtable.v1.Cell cell = Cell.newBuilder()
            .setTimestampMicros(System.nanoTime() / 1000)
            .setValue(cellValue)
            .build();
        Family family =
            Family.newBuilder()
                .setName("family")
                .addColumns(Column.newBuilder().addCells(cell))
                .build();
        return Row.newBuilder().setKey(key).addFamilies(family).build();
      }
    });

    BulkOptions bulkOptions = new BulkOptions.Builder().setUseBulkApi(true).build();
    BigtableOptions options = new BigtableOptions.Builder().setBulkOptions(bulkOptions).build();
    BatchExecutor underTest = createExecutor(options);
    Result[] results = underTest.batch(gets);
    verify(mockClient, times(1)).readRows(any(ReadRowsRequest.class));
    Assert.assertTrue(matchesRow(Result.EMPTY_RESULT).matches(results[0]));
    for (int i = 1; i < results.length; i++) {
      Assert.assertTrue(Bytes.equals(results[i].getRow(), gets.get(i).getRow()));
    }
  }

  // HELPERS

  private void testMutation(org.apache.hadoop.hbase.client.Row mutation) throws Exception {
    when(mockFuture.get()).thenReturn(Empty.getDefaultInstance());
    Result[] results = batch(Arrays.asList(mutation));
    Assert.assertTrue(matchesRow(Result.EMPTY_RESULT).matches(results[0]));
  }

  private BatchExecutor createExecutor(BigtableOptions options) {
    return new BatchExecutor(
        mockAsyncExecutor,
        options,
        BigtableSessionSharedThreadPools.getInstance().getRetryExecutor(),
        requestAdapter);
  }

  private Result[] batch(final List<? extends org.apache.hadoop.hbase.client.Row> actions)
      throws Exception {
    return createExecutor(DEFAULT_OPTIONS).batch(actions);
  }
}
