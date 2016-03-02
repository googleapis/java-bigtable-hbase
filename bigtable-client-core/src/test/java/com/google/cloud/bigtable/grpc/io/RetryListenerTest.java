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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.bigtable.grpc.io.RetryListener;
import com.google.cloud.bigtable.grpc.io.RetryingCall;
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

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;

import java.util.Set;

@RunWith(JUnit4.class)
public class RetryListenerTest {

  @Mock
  private RetryingCall<MutateRowRequest, Empty> mockRetryingCall;
  @Mock
  private ClientCall.Listener<Empty> mockResponseListener;

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
            new Metadata(),
            mockResponseListener);

    listener.onHeaders(new Metadata());
    listener.onMessage(response);
    listener.onClose(Status.OK, new Metadata());

    // Validate that the listener did not attempt to start a new call on the channel:
    verifyNoMoreInteractions(mockRetryingCall);

    // Verify that the mockResponseListener was informed of the payload and closed:
    verify(mockResponseListener, times(1)).onMessage(eq(response));
    verify(mockResponseListener, times(1)).onClose(eq(Status.OK), any(Metadata.class));
  }

  @Test
  public void internalErrorsAreRetried() {
    Metadata headers = new Metadata();
    RetryListener<MutateRowRequest, Empty> listener =
        new RetryListener<>(
            mockRetryingCall,
            request,
            headers,
            mockResponseListener);

    // Indicate that retry has begun:
    when(
        mockRetryingCall
            .retryCallAfterBackoff(
                eq(request), eq(headers), eq(listener)))
        .thenReturn(true);

    listener.onClose(Status.INTERNAL, new Metadata());

    // Validate that the listener is starting a new attempt
    verify(mockRetryingCall, times(1))
        .retryCallAfterBackoff(eq(request), eq(headers), eq(listener));

    // Verify that the mockResponseListener has not received any messages about the call:
    verifyNoMoreInteractions(mockResponseListener);
  }

  @Test
  public void failuresAfterHeadersAreReceivedIsNotRetried() {
    Metadata requestHeaders = new Metadata();
    RetryListener<MutateRowRequest, Empty> listener =
        new RetryListener<>(
            mockRetryingCall,
            request,
            requestHeaders,
            mockResponseListener);

    Metadata responseHeaders = new Metadata();
    listener.onHeaders(responseHeaders);
    listener.onMessage(response);
    listener.onClose(Status.INTERNAL, new Metadata());

    // Validate that the listener did not attempt to start a new call on the channel:
    verifyNoMoreInteractions(mockRetryingCall);

    // Verify that the mockResponseListener was informed of the payload and closed:
    verify(mockResponseListener, times(1)).onHeaders(eq(responseHeaders));
    verify(mockResponseListener, times(1)).onMessage(eq(response));
    verify(mockResponseListener, times(1)).onClose(
        eq(Status.INTERNAL), any(Metadata.class));
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
