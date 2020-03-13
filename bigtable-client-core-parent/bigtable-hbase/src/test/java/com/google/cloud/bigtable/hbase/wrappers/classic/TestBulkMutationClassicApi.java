/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class TestBulkMutationClassicApi {

  private BulkMutation mockDelegate;
  private BulkMutationWrapper bulkMutationWrapper;

  @Before
  public void setUp() {
    mockDelegate = Mockito.mock(BulkMutation.class);
    bulkMutationWrapper = new BulkMutationClassicApi(mockDelegate);
  }

  @Test
  public void testAddMutate() throws ExecutionException, InterruptedException {
    RowMutationEntry rowMutation = RowMutationEntry.create("key");
    MutateRowsRequest.Entry requestProto = rowMutation.toProto();
    when(mockDelegate.add(requestProto))
        .thenReturn(Futures.immediateFuture(MutateRowResponse.getDefaultInstance()));
    bulkMutationWrapper.add(rowMutation).get();

    verify(mockDelegate).add(requestProto);
  }

  @Test
  public void testFlush() throws InterruptedException {
    doNothing().when(mockDelegate).flush();
    bulkMutationWrapper.flush();
    verify(mockDelegate).flush();
  }

  @Test
  public void testSendUnsent() {
    doNothing().when(mockDelegate).sendUnsent();
    bulkMutationWrapper.sendUnsent();
    verify(mockDelegate).sendUnsent();
  }

  @Test
  public void testClose() throws IOException {
    bulkMutationWrapper.close();
    try {
      bulkMutationWrapper.add(RowMutationEntry.create("key"));
      Assert.fail("bulk mutation should be closed");
    } catch (IllegalStateException actualEx) {
      assertEquals("can't mutate when the bulk mutation is closed.", actualEx.getMessage());
    }
  }
}
