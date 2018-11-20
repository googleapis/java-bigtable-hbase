package com.google.cloud.bigtable.grpc.async;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.common.util.concurrent.SettableFuture;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/** Tests for {@link ThrottlingClientInterceptor}
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestThrottlingClientInterceptor {
  private static final ReadRowsRequest request =
      ReadRowsRequest.newBuilder().setTableName("Some/Table/Name").build();

  @Mock Channel mockChannel;
  @Mock ResourceLimiter mockResourceLimiter;
  MethodDescriptor methodDescriptor = BigtableGrpc.getReadRowsMethod();
  @Mock ClientCall.Listener mockListener;
  @Mock ClientCall mockClientCall;
  ExecutorService executorService;


  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    executorService = Executors.newCachedThreadPool();
    when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class))).thenReturn(mockClientCall);
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  public void testThrottled() throws Exception {
    final SettableFuture registerInvoked = SettableFuture.create();
    final SettableFuture registerWaiter = SettableFuture.create();
    final long id = 1l;
    when(mockResourceLimiter.registerOperationWithHeapSize(anyLong())).then(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        registerInvoked.set("");
        registerWaiter.get(1, TimeUnit.SECONDS);
        return id;
      }
    });
    final ThrottlingClientInterceptor underTest = new ThrottlingClientInterceptor(mockResourceLimiter);
    final int numMessages = 5;
    Future<?> future = executorService.submit(new Runnable() {
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
}
