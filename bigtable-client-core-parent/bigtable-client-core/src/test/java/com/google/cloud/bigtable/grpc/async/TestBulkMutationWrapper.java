/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class TestBulkMutationWrapper {

  private BulkMutation mockDelegate;
  private RequestContext requestContext =
      RequestContext.create("ProjectId", "instanceId", "appProfileId");
  private IBulkMutation bulkWrapper;

  @Before
  public void setUp() {
    mockDelegate = Mockito.mock(BulkMutation.class);
    bulkWrapper = new BulkMutationWrapper(mockDelegate, requestContext);
  }

  @Test
  public void testFlush() throws InterruptedException {
    doNothing().when(mockDelegate).flush();
    bulkWrapper.flush();
    verify(mockDelegate).flush();
  }

  @Test
  public void testSendUnsend() {
    doNothing().when(mockDelegate).sendUnsent();
    bulkWrapper.sendUnsent();
    verify(mockDelegate).sendUnsent();
  }

  @Test
  public void testAddMutate() {
    RowMutation rowMutation = RowMutation.create("tableId", "key");
    MutateRowsRequest.Entry requestProto = rowMutation.toBulkProto(requestContext).getEntries(0);
    when(mockDelegate.add(requestProto))
        .thenReturn(Futures.immediateFuture(MutateRowResponse.getDefaultInstance()));
    Future<Void> response = bulkWrapper.add(rowMutation);
    try {
      response.get();
    } catch (Exception ex) {
      throw new AssertionError("Assertion failed for BulkMutationWrapper#add(RowMutation)");
    }
    verify(mockDelegate).add(requestProto);
  }

  @Test
  public void testIsClosed() throws IOException {
    bulkWrapper.close();
    Exception actualEx = null;
    try {
      bulkWrapper.add(RowMutation.create("tableId", "key"));
    } catch (Exception e) {
      actualEx = e;
    }
    assertTrue(actualEx instanceof IllegalStateException);
    assertEquals("can't mutate when the bulk mutation is closed.", actualEx.getMessage());
  }
}
