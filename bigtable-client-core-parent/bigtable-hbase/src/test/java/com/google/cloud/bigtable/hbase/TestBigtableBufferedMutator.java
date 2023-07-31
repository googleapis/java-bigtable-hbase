/*
 * Copyright 2015 Google Inc.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests for {@link BigtableBufferedMutator} */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final byte[] EMPTY_BYTES = new byte[1];
  private static final Put SIMPLE_PUT =
      new Put(EMPTY_BYTES).addColumn(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES);

  @Mock private BigtableApi mockBigtableApi;

  @Mock private BulkMutationWrapper mockBulkMutation;

  @Mock private DataClientWrapper mockDataClient;

  @SuppressWarnings("rawtypes")
  private SettableApiFuture future = SettableApiFuture.create();

  @Mock private BufferedMutator.ExceptionListener listener;

  private ExecutorService executorService;

  @Before
  public void setUp() {
    when(mockBigtableApi.getDataClient()).thenReturn(mockDataClient);
    when(mockDataClient.createBulkMutation(Mockito.anyString())).thenReturn(mockBulkMutation);
    when(mockDataClient.createBulkMutation(Mockito.anyString(), Mockito.anyLong()))
        .thenReturn(mockBulkMutation);
  }

  @After
  public void tearDown() {
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  private BigtableBufferedMutator createMutator(Configuration configuration) throws IOException {
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, "project");
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, "instance");

    BigtableHBaseSettings settings = BigtableHBaseSettings.create(configuration);
    HBaseRequestAdapter adapter = new HBaseRequestAdapter(settings, TableName.valueOf("TABLE"));

    executorService = Executors.newCachedThreadPool();
    return new BigtableBufferedMutator(mockBigtableApi, settings, adapter, listener);
  }

  @Test
  public void testPut() throws IOException {
    when(mockBulkMutation.add(any(RowMutationEntry.class))).thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    underTest.mutate(SIMPLE_PUT);
    verify(mockBulkMutation, times(1)).add(any(RowMutationEntry.class));
  }

  @Test
  public void testDelete() throws IOException {
    when(mockBulkMutation.add(any(RowMutationEntry.class))).thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    underTest.mutate(new Delete(EMPTY_BYTES));
    verify(mockBulkMutation, times(1)).add(any(RowMutationEntry.class));
  }

  @Test
  public void testIncrement() throws IOException {
    when(mockDataClient.readModifyWriteRowAsync(any(ReadModifyWriteRow.class))).thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    underTest.mutate(new Increment(EMPTY_BYTES).addColumn(EMPTY_BYTES, EMPTY_BYTES, 1));
    verify(mockDataClient, times(1)).readModifyWriteRowAsync(any(ReadModifyWriteRow.class));
  }

  @Test
  public void testAppend() throws IOException {
    when(mockDataClient.readModifyWriteRowAsync(any(ReadModifyWriteRow.class))).thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    underTest.mutate(new Append(EMPTY_BYTES).add(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES));
    verify(mockDataClient, times(1)).readModifyWriteRowAsync(any(ReadModifyWriteRow.class));
  }

  @Test
  public void testInvalidPut() throws Exception {
    when(mockBulkMutation.add(any(RowMutationEntry.class)))
        .thenReturn(ApiFutures.<Void>immediateFailedFuture(new RuntimeException()));
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    underTest.mutate(SIMPLE_PUT);
    // Leave some time for the async worker to handle the request.
    verify(listener, times(0))
        .onException(any(RetriesExhaustedWithDetailsException.class), same(underTest));
    underTest.mutate(SIMPLE_PUT);
    verify(listener, times(1))
        .onException(any(RetriesExhaustedWithDetailsException.class), same(underTest));
  }

  @Test
  public void testBulkSingleRequests() throws IOException, InterruptedException {
    Configuration config = new Configuration(false);
    when(mockBulkMutation.add(any(RowMutationEntry.class))).thenReturn(future);
    final BigtableBufferedMutator underTest = createMutator(config);
    underTest.mutate(SIMPLE_PUT);
    verify(mockBulkMutation, times(1)).add(any(RowMutationEntry.class));
    underTest.flush();
    verify(mockBulkMutation, times(1)).flush();
  }

  @Test
  public void testBulkMultipleRequests() throws IOException, InterruptedException {
    Configuration config = new Configuration(false);
    when(mockBulkMutation.add(any(RowMutationEntry.class))).thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(config);
    int count = 30;
    for (int i = 0; i < count; i++) {
      underTest.mutate(SIMPLE_PUT);
    }
    verify(mockBulkMutation, times(count)).add(any(RowMutationEntry.class));
    underTest.flush();
    verify(mockBulkMutation, times(1)).flush();
  }

  @Test
  public void testClose() throws IOException {
    Configuration config = new Configuration(false);
    doNothing().when(mockBulkMutation).close();
    when(mockBulkMutation.add(any(RowMutationEntry.class))).thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(config);
    underTest.mutate(SIMPLE_PUT);

    underTest.close();

    verify(mockBulkMutation).add(any(RowMutationEntry.class));
    verify(mockBulkMutation).close();
  }
}
