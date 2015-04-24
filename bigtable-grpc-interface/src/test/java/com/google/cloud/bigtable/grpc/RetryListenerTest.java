package com.google.cloud.bigtable.grpc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.grpc.Call;
import io.grpc.Metadata.Headers;
import io.grpc.Metadata.Trailers;
import io.grpc.Status;

import java.util.Set;

@RunWith(JUnit4.class)
public class RetryListenerTest {

  @Mock
  private RetryingCall<MutateRowRequest, Empty> mockRetryingCall;
  @Mock
  private Call.Listener<Empty> mockResponseListener;

  private final MutateRowRequest request =
      MutateRowRequest.newBuilder()
          .setRowKey(ByteString.copyFromUtf8("rowKey"))
          .build();

  private final Empty response = Empty.newBuilder().build();

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void successfulCallsAreNotRetried() {
    RetryListener<MutateRowRequest, Empty> listener =
        new RetryListener<>(
            mockRetryingCall,
            request,
            new Headers.Headers(),
            true, // always retriable for testing
            mockResponseListener);

    listener.onHeaders(new Headers.Headers());
    listener.onPayload(response);
    listener.onClose(Status.OK, new Trailers.Trailers());

    // Validate that the listener did not attempt to start a new call on the channel:
    verifyNoMoreInteractions(mockRetryingCall);

    // Verify that the mockResponseListener was informed of the payload and closed:
    verify(mockResponseListener, times(1)).onPayload(eq(response));
    verify(mockResponseListener, times(1)).onClose(eq(Status.OK), any(Trailers.Trailers.class));
  }

  @Test
  public void internalErrorsAreRetried() {
    Headers headers = new Headers.Headers();
    RetryListener<MutateRowRequest, Empty> listener =
        new RetryListener<>(
            mockRetryingCall,
            request,
            headers,
            true, // always retriable for testing
            mockResponseListener);

    // Indicate that retry has begun:
    when(
        mockRetryingCall
            .retryCallAfterBackoff(
                eq(request), eq(headers), eq(listener)))
        .thenReturn(true);

    listener.onClose(Status.INTERNAL, new Trailers.Trailers());

    // Validate that the listener is starting a new attempt
    verify(mockRetryingCall, times(1))
        .retryCallAfterBackoff(eq(request), eq(headers), eq(listener));

    // Verify that the mockResponseListener has not received any messages about the call:
    verifyNoMoreInteractions(mockResponseListener);
  }

  @Test
  public void failuresAfterHeadersAreReceivedIsNotRetried() {
    Headers requestHeaders = new Headers.Headers();
    RetryListener<MutateRowRequest, Empty> listener =
        new RetryListener<>(
            mockRetryingCall,
            request,
            requestHeaders,
            true, // always retriable for testing
            mockResponseListener);

    Headers responseHeaders = new Headers.Headers();
    listener.onHeaders(responseHeaders);
    listener.onPayload(response);
    listener.onClose(Status.INTERNAL, new Trailers.Trailers());

    // Validate that the listener did not attempt to start a new call on the channel:
    verifyNoMoreInteractions(mockRetryingCall);

    // Verify that the mockResponseListener was informed of the payload and closed:
    verify(mockResponseListener, times(1)).onHeaders(eq(responseHeaders));
    verify(mockResponseListener, times(1)).onPayload(eq(response));
    verify(mockResponseListener, times(1)).onClose(
        eq(Status.INTERNAL), any(Trailers.Trailers.class));
  }

  @Test
  public void isRetriableStatus() {
    Set<Status.Code> retriableSet = ImmutableSet.of(Status.Code.INTERNAL, Status.Code.UNAVAILABLE);
    for (Status.Code retriable : retriableSet) {
      assertTrue(RetryListener.isRetriableStatus(retriable));
    }

    Set<Status.Code> nonRetriableSet = Sets.complementOf(retriableSet);
    for (Status.Code nonRetriable : nonRetriableSet) {
      assertFalse(RetryListener.isRetriableStatus(nonRetriable));
    }
  }
}
