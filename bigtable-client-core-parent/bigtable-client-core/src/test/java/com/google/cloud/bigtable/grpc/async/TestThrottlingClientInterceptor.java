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
package com.google.cloud.bigtable.grpc.async;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

/** Tests for {@link ThrottlingClientInterceptor} */
@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestThrottlingClientInterceptor {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final ReadRowsRequest request =
      ReadRowsRequest.newBuilder().setTableName("Some/Table/Name").build();

  @Mock Channel mockChannel;
  @Mock ResourceLimiter mockResourceLimiter;
  @Mock ClientCall.Listener mockListener;
  @Mock ClientCall mockClientCall;

  private MethodDescriptor methodDescriptor = BigtableGrpc.getReadRowsMethod();
  private ExecutorService executorService;

  @Before
  public void setup() {
    executorService = Executors.newCachedThreadPool();
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  public void testThrottled() throws Exception {
    final SettableFuture registerInvoked = SettableFuture.create();
    final SettableFuture registerWaiter = SettableFuture.create();
    final long id = 1L;
    when(mockResourceLimiter.registerOperationWithHeapSize(anyLong()))
        .then(
            new Answer<Long>() {
              @Override
              public Long answer(InvocationOnMock invocation) throws Throwable {
                registerInvoked.set("");
                registerWaiter.get(1, TimeUnit.SECONDS);
                return id;
              }
            });
    final ThrottlingClientInterceptor underTest = createClientInterceptor(true);
    final int numMessages = 5;
    Future<?> future =
        executorService.submit(
            new Runnable() {
              @Override
              public void run() {
                ClientCall call =
                    underTest.interceptCall(methodDescriptor, CallOptions.DEFAULT, mockChannel);
                call.start(mockListener, new Metadata());
                call.request(numMessages);
                // This should block
                call.sendMessage(request);
              }
            });
    registerInvoked.get(1, TimeUnit.SECONDS);
    verify(mockResourceLimiter, times(1))
        .registerOperationWithHeapSize(eq((long) request.getSerializedSize()));
    verify(mockChannel, times(0)).newCall(any(MethodDescriptor.class), any(CallOptions.class));

    registerWaiter.set("");
    future.get(1, TimeUnit.SECONDS);
    verify(mockChannel, times(1)).newCall(any(MethodDescriptor.class), any(CallOptions.class));
    verify(mockClientCall, times(1)).request(eq(numMessages));

    ArgumentCaptor<ClientCall.Listener> listenerArgumentCaptor =
        ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockClientCall, times(1)).start(listenerArgumentCaptor.capture(), any(Metadata.class));

    Status status = Status.OK;
    Metadata trailers = new Metadata();
    listenerArgumentCaptor.getValue().onClose(status, trailers);
    verify(mockResourceLimiter, times(1)).markCanBeCompleted(eq(id));
    verify(mockListener, times(1)).onClose(same(status), same(trailers));
  }

  @Test
  public void testInterrupted() throws Exception {
    when(mockResourceLimiter.registerOperationWithHeapSize(anyLong()))
        .thenThrow(new InterruptedException("Fake interrupted error"));

    final ThrottlingClientInterceptor underTest = createClientInterceptor(false);

    ClientCall call = underTest.interceptCall(methodDescriptor, CallOptions.DEFAULT, mockChannel);

    call.start(mockListener, new Metadata());
    call.sendMessage(request);
    call.halfClose();
    call.request(1);

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);

    verify(mockListener, times(1)).onClose(statusCaptor.capture(), any(Metadata.class));
    Assert.assertEquals(statusCaptor.getValue().getCode(), Code.INTERNAL);
  }

  @Test
  public void testCallProxy() {
    final ThrottlingClientInterceptor underTest = createClientInterceptor(true);

    ClientCall call = underTest.interceptCall(methodDescriptor, CallOptions.DEFAULT, mockChannel);

    call.start(mockListener, new Metadata());
    call.sendMessage(request);
    call.halfClose();
    call.request(1);

    verify(mockClientCall).start(any(ClientCall.Listener.class), any(Metadata.class));
    verify(mockClientCall).sendMessage(request);
    verify(mockClientCall).halfClose();
    verify(mockClientCall).request(1);
  }

  @Test
  public void testCancel() {
    final ThrottlingClientInterceptor underTest = createClientInterceptor(true);

    ClientCall call = underTest.interceptCall(methodDescriptor, CallOptions.DEFAULT, mockChannel);

    call.start(mockListener, new Metadata());
    call.sendMessage(request);
    call.cancel("fake cancel", null);

    verify(mockClientCall).cancel(eq("fake cancel"), (Throwable) any());
  }

  @Test
  public void testEarlyCancel() {
    final ThrottlingClientInterceptor underTest = createClientInterceptor(false);

    ClientCall call = underTest.interceptCall(methodDescriptor, CallOptions.DEFAULT, mockChannel);

    call.start(mockListener, new Metadata());
    call.cancel("fake cancel", null);

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);

    verify(mockListener, times(1)).onClose(statusCaptor.capture(), any(Metadata.class));
    Assert.assertEquals(statusCaptor.getValue().getCode(), Code.CANCELLED);
  }

  private ThrottlingClientInterceptor createClientInterceptor(boolean shouldChannelCallMocked) {
    if (shouldChannelCallMocked) {
      when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
          .thenReturn(mockClientCall);
    }
    return new ThrottlingClientInterceptor(mockResourceLimiter);
  }
}
