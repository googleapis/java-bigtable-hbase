package com.google.cloud.bigtable.grpc;

import static org.mockito.Mockito.when;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.bigtable.grpc.CallCompletionStatusInterceptor.CompletionStatusGatheringCall;
import com.google.protobuf.Empty;

import io.grpc.Call;
import io.grpc.Call.Listener;
import io.grpc.Channel;
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
  private Call<MutateRowRequest, Empty> callStub;
  @Mock
  private Call.Listener<Empty> responseListenerStub;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void callCompletionStatusesAreRecorded() throws InterruptedException {
    CallCompletionStatusInterceptor interceptor = new CallCompletionStatusInterceptor();

    when(channelStub.newCall(BigtableServiceGrpc.CONFIG.mutateRow)).thenReturn(callStub);

    CompletionStatusGatheringCall<MutateRowRequest, Empty> wrappedCall =
        interceptor.interceptCall(BigtableServiceGrpc.CONFIG.mutateRow, channelStub);

    Listener<Empty> statusGatheringListener =
        wrappedCall.createGatheringListener(responseListenerStub);

    statusGatheringListener.onClose(Status.INTERNAL, new Metadata.Trailers());

    CallCompletionStatusInterceptor.CallCompletionStatus expectedStatusEntry =
        new CallCompletionStatusInterceptor.CallCompletionStatus(
            BigtableServiceGrpc.CONFIG.mutateRow, Status.INTERNAL);

    Assert.assertEquals(1, interceptor.getCallCompletionStatuses().count(expectedStatusEntry));
  }
}
