/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.async;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

/** Tests for {@link BulkRead}. */
@RunWith(JUnit4.class)
public class TestBulkRead {

  private static final BigtableTableName TABLE_NAME =
      new BigtableTableName("projects/SomeProject/instances/SomeInstance/tables/SomeTable");
  private static final RequestContext CONTEXT =
      RequestContext.create("SomeProject", "SomeInstance", "");

  private static final Random random = new Random(2128506);

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock BigtableDataClient mockClient;

  @Mock ExecutorService mockThreadPool;

  @Mock ResultScanner<FlatRow> mockScanner;

  private BulkRead underTest;

  @Before
  public void setup() {
    // immediately execute the Runnable.
    when(mockThreadPool.submit(any(Runnable.class)))
        .thenAnswer(
            new Answer<Void>() {

              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                invocation.getArgument(0, Runnable.class).run();
                return null;
              }
            });

    underTest = new BulkRead(mockClient, TABLE_NAME, 5, mockThreadPool);
  }

  /** Tests to make sure that a batch of one key returns results correctly. */
  @Test
  public void testBatchOfOne() throws Exception {
    when(mockClient.readFlatRows(any(ReadRowsRequest.class))).thenReturn(mockScanner);
    FlatRow row = createRow(ByteString.copyFromUtf8("Key"));
    when(mockScanner.next()).thenReturn(row).thenReturn(null);
    Future<FlatRow> future = underTest.add(createRequest(row.getRowKey()));
    underTest.flush();
    verify(mockClient, times(1)).readFlatRows(any(ReadRowsRequest.class));
    Assert.assertEquals(row, future.get(10, TimeUnit.MILLISECONDS));
  }

  /**
   * Tests to make sure that Futures for two requests of the same key both return a valid response.
   */
  @Test
  public void testDuplicateKey() throws Exception {
    when(mockClient.readFlatRows(any(ReadRowsRequest.class))).thenReturn(mockScanner);
    FlatRow row = createRow(ByteString.copyFromUtf8("Key"));
    when(mockScanner.next()).thenReturn(row).thenReturn(null);
    Query request = createRequest(row.getRowKey());
    Future<FlatRow> future1 = underTest.add(request);
    Future<FlatRow> future2 = underTest.add(request);
    underTest.flush();
    verify(mockClient, times(1)).readFlatRows(any(ReadRowsRequest.class));
    Assert.assertEquals(row, future1.get(10, TimeUnit.MILLISECONDS));
    Assert.assertEquals(row, future2.get(10, TimeUnit.MILLISECONDS));
  }

  /** Tests to make sure that a randomized set of keys are all returned as expected. */
  @Test
  public void testBatchOfOneHundred() throws Exception {
    List<ByteString> rowKeys = createRandomKeys(100);
    List<Future<FlatRow>> futures =
        addRows(
            rowKeys,
            new Answer<ResultScanner<FlatRow>>() {
              @Override
              public ResultScanner<FlatRow> answer(InvocationOnMock invocation) throws Throwable {
                ReadRowsRequest request = invocation.getArgument(0, ReadRowsRequest.class);
                List<ByteString> list = new ArrayList<>(request.getRows().getRowKeysList());
                Collections.shuffle(list);
                return createMockScanner(list.iterator());
              }
            });
    for (int i = 0; i < rowKeys.size(); i++) {
      FlatRow row = futures.get(i).get(10, TimeUnit.MILLISECONDS);
      Assert.assertNotNull(row);
      Assert.assertEquals(rowKeys.get(i), row.getRowKey());
    }
  }

  /**
   * Tests to make sure that a randomized set of keys, with some random set of missing responses,
   * are all returned as expected.
   */
  @Test
  public void testMissingResponses() throws Exception {
    List<ByteString> rowKeys = createRandomKeys(100);
    final Set<ByteString> missing = new HashSet<>();
    List<Future<FlatRow>> futures =
        addRows(
            rowKeys,
            new Answer<ResultScanner<FlatRow>>() {
              @Override
              public ResultScanner<FlatRow> answer(InvocationOnMock invocation) throws Throwable {
                ReadRowsRequest request = invocation.getArgument(0, ReadRowsRequest.class);
                List<ByteString> rowKeysList = new ArrayList<>(request.getRows().getRowKeysList());
                missing.add(rowKeysList.remove(random.nextInt(rowKeysList.size())));
                return createMockScanner(rowKeysList.iterator());
              }
            });
    for (int i = 0; i < rowKeys.size(); i++) {
      FlatRow row = futures.get(i).get(10, TimeUnit.MILLISECONDS);
      if (missing.contains(rowKeys.get(i))) {
        Assert.assertNull(row);
      } else {
        Assert.assertNotNull(row);
        Assert.assertEquals(rowKeys.get(i), row.getRowKey());
      }
    }
  }

  // /////////////// HELPERS ////////////////

  /**
   * Converts the rowKeys input into {@link MutateRowsRequest}s, calls {@link
   * BulkMutation#add(com.google.bigtable.v2.MutateRowsRequest.Entry)} and collects the resulting
   * {@link ListenableFuture}s.
   *
   * @param rowKeys The row keys to retrieve.
   * @param scannerGenerator Generates {@link ResultScanner}s that will generate FlatRows to be
   *     processed by {@link BulkRead}.
   */
  private List<Future<FlatRow>> addRows(
      List<ByteString> rowKeys, Answer<ResultScanner<FlatRow>> scannerGenerator) {
    when(mockClient.readFlatRows(any(ReadRowsRequest.class))).thenAnswer(scannerGenerator);

    List<Future<FlatRow>> futures = new ArrayList<>();
    for (ByteString key : rowKeys) {
      futures.add(underTest.add(createRequest(key)));
    }
    underTest.flush();
    verify(mockClient, times(rowKeys.size() / underTest.getBatchSizes()))
        .readFlatRows(any(ReadRowsRequest.class));
    return futures;
  }

  private static List<ByteString> createRandomKeys(int count) {
    List<ByteString> rowKeys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      // Create a random list of keys as input
      rowKeys.add(ByteString.copyFromUtf8(String.valueOf(random.nextInt(100000000))));
    }
    return rowKeys;
  }

  /**
   * Creates a mock {@link ResultScanner} that will return a {@link FlatRow} for every key in the
   * input.
   *
   * @param keyIterator An {@link Iterator} for the keys
   * @return A {@link ResultScanner} that will sequentially return {@link FlatRow} corresponding to
   *     the order of the input.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private static ResultScanner<FlatRow> createMockScanner(final Iterator<ByteString> keyIterator)
      throws IOException {
    ResultScanner<FlatRow> mock = Mockito.mock(ResultScanner.class);
    when(mock.next())
        .then(
            new Answer<FlatRow>() {
              @Override
              public FlatRow answer(InvocationOnMock invocation) throws Throwable {
                return keyIterator.hasNext() ? createRow(keyIterator.next()) : null;
              }
            });
    return mock;
  }

  /**
   * Helper to generate a {@link FlatRow} for a row key.
   *
   * @param key
   */
  private static Query createRequest(ByteString key) {
    return Query.create("table").rowKey(key);
  }

  /** Creates a random {@link FlatRow} for the input. */
  private static FlatRow createRow(ByteString key) {
    return FlatRow.newBuilder()
        .withRowKey(key)
        .addCell("family", ByteString.EMPTY, System.currentTimeMillis(), ByteString.EMPTY)
        .build();
  }
}
