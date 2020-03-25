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

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INSTANCE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.PROJECT_ID_KEY;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

/** Tests {@link BatchExecutor} */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBatchExecutor {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

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
      public void describeTo(Description description) {}

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
      public void describeMismatch(Object item, Description mismatchDescription) {}
    };
  }

  @Mock private BulkReadWrapper mockBulkRead;

  @Mock private BulkMutationWrapper mockBulkMutation;

  @Mock private DataClientWrapper mockDataClientWrapper;

  @Mock private BigtableApi mockBigtableApi;

  @Mock private ApiFuture mockFuture;

  private BigtableHBaseSettings settings;
  private HBaseRequestAdapter requestAdapter;

  @Before
  public void setup() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.set(PROJECT_ID_KEY, "projectId");
    configuration.set(INSTANCE_ID_KEY, "instanceId");
    settings = BigtableHBaseSettings.create(configuration);

    requestAdapter = new HBaseRequestAdapter(settings, TableName.valueOf("table"));
    when(mockBigtableApi.getDataClient()).thenReturn(mockDataClientWrapper);
    when(mockDataClientWrapper.createBulkRead("table")).thenReturn(mockBulkRead);
    when(mockDataClientWrapper.createBulkMutation(any(String.class))).thenReturn(mockBulkMutation);

    when(mockBulkMutation.add(any(RowMutationEntry.class))).thenReturn(mockFuture);
    when(mockDataClientWrapper.readModifyWriteRowAsync(any(ReadModifyWriteRow.class)))
        .thenReturn(mockFuture);
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                invocation.getArgument(0, Runnable.class).run();
                return null;
              }
            })
        .when(mockFuture)
        .addListener(any(Runnable.class), any(Executor.class));
    doReturn(true).when(mockFuture).isDone();
  }

  @Test
  public void testGet() throws Exception {
    when(mockBulkRead.add(any(Query.class))).thenReturn(mockFuture);
    final byte[] key = randomBytes(8);
    Result response =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    key,
                    Bytes.toBytes("family"),
                    Bytes.toBytes(""),
                    1000L,
                    Bytes.toBytes("value"))));
    setFuture(response);
    Result[] results = batch(ImmutableList.<Row>of(new Get(key)));
    assertTrue(matchesRow(response).matches(results[0]));
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
    when(mockFuture.get()).thenThrow(new IllegalStateException("closed"));
    try {
      batch(Arrays.asList(randomPut()));
    } catch (RetriesExhaustedWithDetailsException e) {
      Assert.assertEquals(1, e.getCauses().size());
      Assert.assertEquals(IllegalStateException.class, e.getCause(0).getClass());
    }
  }

  @Test
  public void testAsyncException() throws Exception {
    String message = "Something bad happened";
    when(mockFuture.get()).thenThrow(new RuntimeException(message));
    try {
      batch(Arrays.asList(randomPut()));
    } catch (RetriesExhaustedWithDetailsException e) {
      Assert.assertEquals(1, e.getCauses().size());
      Assert.assertEquals(RuntimeException.class, e.getCause(0).getClass());
      Assert.assertEquals(message, e.getCause(0).getMessage());
    }
  }

  @Test
  public void testPartialResults() throws Exception {
    when(mockBigtableApi.getDataClient()).thenReturn(mockDataClientWrapper);
    when(mockDataClientWrapper.createBulkRead(isA(String.class))).thenReturn(mockBulkRead);
    byte[] key1 = randomBytes(8);
    byte[] key2 = randomBytes(8);
    Result expected =
        Result.create(
            ImmutableList.<org.apache.hadoop.hbase.Cell>of(
                new RowCell(
                    key1,
                    Bytes.toBytes("cf"),
                    Bytes.toBytes(""),
                    10,
                    Bytes.toBytes("hi!"),
                    ImmutableList.<String>of())));
    RuntimeException exception = new RuntimeException("Something bad happened");
    when(mockBulkRead.add(any(Query.class)))
        .thenReturn(ApiFutures.immediateFuture(expected))
        .thenReturn(ApiFutures.<Result>immediateFailedFuture(exception));

    List<Get> gets = Arrays.asList(new Get(key1), new Get(key2));
    Object[] results = new Object[2];

    try {
      createExecutor().batch(gets, results);
    } catch (RetriesExhaustedWithDetailsException ignored) {
    }
    assertTrue("first result is a result", results[0] instanceof Result);
    assertTrue(matchesRow(expected).matches(results[0]));
    Assert.assertEquals(exception, results[1]);
  }

  @Test
  public void testGetCallback() throws Exception {
    when(mockBulkRead.add(any(Query.class))).thenReturn(mockFuture);
    byte[] key = randomBytes(8);
    Result response =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    key,
                    Bytes.toBytes("family"),
                    Bytes.toBytes(""),
                    1000L,
                    Bytes.toBytes("value"))));
    setFuture(response);
    final Callback<Result> callback = Mockito.mock(Callback.class);
    createExecutor().batchCallback(ImmutableList.<Row>of(new Get(key)), new Object[1], callback);

    verify(callback, times(1))
        .update(same(BatchExecutor.NO_REGION), same(key), argThat(matchesRow(response)));
  }

  @Test
  public void testBatchBulkGets() throws Exception {
    final List<Get> gets = new ArrayList<>(10);
    final List<ApiFuture<Result>> expected = new ArrayList<>(10);

    gets.add(new Get(Bytes.toBytes("key0")));
    expected.add(ApiFutures.<Result>immediateFuture(null));
    for (int i = 1; i < 10; i++) {
      byte[] row_key = randomBytes(8);
      gets.add(new Get(row_key));
      ByteString key = ByteStringer.wrap(row_key);
      ByteString cellValue = ByteString.copyFrom(randomBytes(8));
      expected.add(
          ApiFutures.immediateFuture(
              Result.create(
                  ImmutableList.<Cell>of(
                      new RowCell(
                          key.toByteArray(),
                          Bytes.toBytes("family"),
                          Bytes.toBytes(""),
                          System.nanoTime() / 1000,
                          cellValue.toByteArray())))));
    }

    // Test 10 gets, but return only 9 to test the row not found case.
    when(mockBulkRead.add(any(Query.class)))
        .then(
            new Answer<ApiFuture<Result>>() {
              final AtomicInteger counter = new AtomicInteger();

              @Override
              public ApiFuture<Result> answer(InvocationOnMock invocation) throws Throwable {
                return expected.get(counter.getAndIncrement());
              }
            });
    ByteString key = ByteStringer.wrap(randomBytes(8));
    ByteString cellValue = ByteString.copyFrom(randomBytes(8));
    Result row =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    key.toByteArray(),
                    Bytes.toBytes("family"),
                    Bytes.toBytes(""),
                    1000L,
                    cellValue.toByteArray())));
    when(mockFuture.get()).thenReturn(row);

    Result[] results = createExecutor().batch(gets);
    verify(mockBulkRead, times(10)).add(any(Query.class));
    verify(mockBulkRead, times(1)).flush();
    assertTrue(matchesRow(Result.EMPTY_RESULT).matches(results[0]));
    for (int i = 1; i < results.length; i++) {
      assertTrue(
          "Expected "
              + Bytes.toString(gets.get(i).getRow())
              + " but was "
              + Bytes.toString(results[i].getRow()),
          Bytes.equals(results[i].getRow(), gets.get(i).getRow()));
    }
  }

  // HELPERS

  private void testMutation(org.apache.hadoop.hbase.client.Row mutation) throws Exception {
    setFuture(Empty.getDefaultInstance());
    Result[] results = batch(Arrays.asList(mutation));
    assertTrue(matchesRow(Result.EMPTY_RESULT).matches(results[0]));
  }

  protected void setFuture(Object response) throws InterruptedException, ExecutionException {
    when(mockFuture.get()).thenReturn(response);
    when(mockFuture.isDone()).thenReturn(true);
  }

  private BatchExecutor createExecutor() {
    return new BatchExecutor(mockBigtableApi, settings, requestAdapter);
  }

  private Result[] batch(final List<? extends org.apache.hadoop.hbase.client.Row> actions)
      throws Exception {
    return createExecutor().batch(actions);
  }
}
