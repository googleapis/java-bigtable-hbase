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
package com.google.cloud.bigtable.grpc.io;

import static org.mockito.Mockito.when;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.bigtable.grpc.io.CallCompletionStatusInterceptor;
import com.google.cloud.bigtable.grpc.io.CallCompletionStatusInterceptor.CompletionStatusGatheringCall;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Empty;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.Metadata;
import io.grpc.Status;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class CallCompletionStatusInterceptorTest {

  @Mock
  private Channel channelStub;
  @Mock
  private ClientCall<MutateRowRequest, Empty> callStub;
  @Mock
  private ClientCall.Listener<Empty> responseListenerStub;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void callCompletionStatusesAreRecorded() throws InterruptedException {
    CallCompletionStatusInterceptor interceptor =
        new CallCompletionStatusInterceptor(MoreExecutors.newDirectExecutorService());

    when(
      channelStub.newCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT))
        .thenReturn(callStub);

    CompletionStatusGatheringCall<MutateRowRequest, Empty> wrappedCall =
        interceptor.interceptCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT, channelStub);

    Listener<Empty> statusGatheringListener =
        wrappedCall.createGatheringListener(responseListenerStub);

    statusGatheringListener.onClose(Status.INTERNAL, new Metadata.Trailers());

    CallCompletionStatusInterceptor.CallCompletionStatus expectedStatusEntry =
        new CallCompletionStatusInterceptor.CallCompletionStatus(
            BigtableServiceGrpc.METHOD_MUTATE_ROW, Status.INTERNAL);

    Assert.assertEquals(1, interceptor.getCallCompletionStatuses().count(expectedStatusEntry));
  }
}
