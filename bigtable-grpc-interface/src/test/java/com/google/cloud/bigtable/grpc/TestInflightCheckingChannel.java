/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.grpc.Call;
import io.grpc.MethodDescriptor;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigtable.grpc.InflightCheckingChannel.TrackingCall;

/**
 * Tests for {@link InflightCheckingChannel}.
 */
public class TestInflightCheckingChannel {

  @Mock
  private CloseableChannel channel;

  @Mock
  private Call<?, ?> mockCall;

  private InflightCheckingChannel underTest;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    underTest = new InflightCheckingChannel(channel);
    when(channel.newCall(any(MethodDescriptor.class))).thenReturn(mockCall);
  }

  @Test
  public void testNewCall() {
    Call<?, ?> call = underTest.newCall(null);
    Assert.assertEquals(TrackingCall.class, call.getClass());
    TrackingCall<?, ?> trackingCall = (TrackingCall<?, ?>) call;
    call.start(null, null);
    verify(channel, times(1)).newCall(null);
    Assert.assertTrue(underTest.outstandingRequests.contains(trackingCall.getId()));
    Assert.assertFalse(underTest.canClose());
    trackingCall.untrack();
    Assert.assertFalse(underTest.outstandingRequests.contains(trackingCall.getId()));
    Assert.assertTrue(underTest.canClose());
  }

  @Test
  public void testNewCallCancel() {
    Call<?, ?> call = underTest.newCall(null);
    Assert.assertEquals(TrackingCall.class, call.getClass());
    TrackingCall<?, ?> trackingCall = (TrackingCall<?, ?>) call;
    call.start(null, null);
    verify(channel, times(1)).newCall(null);
    Assert.assertTrue(underTest.outstandingRequests.contains(trackingCall.getId()));
    Assert.assertFalse(underTest.canClose());
    trackingCall.cancel();
    verify(mockCall, times(1)).cancel();
    Assert.assertFalse(underTest.outstandingRequests.contains(trackingCall.getId()));
    Assert.assertTrue(underTest.canClose());
  }

  // TODO(sduskis): add a test for onClose(), which is a bit more difficult to do than the others.

  @Test
  public void testClose() throws IOException {
    underTest.close();
    verify(channel, times(1)).close();
  }
}
