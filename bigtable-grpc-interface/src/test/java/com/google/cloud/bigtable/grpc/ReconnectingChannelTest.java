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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.grpc.ReconnectingChannel.TrackingCall;

import io.grpc.Call;
import io.grpc.Metadata.Headers;
import io.grpc.MethodDescriptor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * Tests {@link ReconnectingChannel}
 */
@RunWith(JUnit4.class)
public class ReconnectingChannelTest {

  private static final long REFRESH_MS = 500;
  @Mock
  private ReconnectingChannel.Factory factory;

  @Mock
  private CloseableChannel channel;

  @Mock
  private Call call;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void test() throws IOException {
    when(factory.create()).thenReturn(channel);
    when(channel.newCall(any(MethodDescriptor.class))).thenReturn(call);
    ReconnectingChannel test =
        new ReconnectingChannel(REFRESH_MS, Executors.newFixedThreadPool(1), factory);
    Mockito.verify(factory, times(1)).create();

    Call<Object, Object> call1 = test.newCall(null);
    Mockito.verify(channel, times(1)).newCall(any(MethodDescriptor.class));

    ((TrackingCall) call1).untrack();

    try {
      Thread.sleep(REFRESH_MS);
    } catch (InterruptedException ignored) {
      // Do nothing on interrupt.
    }

    Call<Object, Object> call2 = test.newCall(null);
    Mockito.verify(channel, times(2)).newCall(any(MethodDescriptor.class));
    ((TrackingCall) call1).untrack();

    try {
      Thread.sleep(REFRESH_MS);
    } catch (InterruptedException ignored) {
      // Do nothing on interrupt.
    }

    Mockito.verify(factory, atLeast(2)).create();
    Mockito.verify(channel, times(1)).close();

    test.close();
    Mockito.verify(channel, times(2)).close();

    try {
      test.newCall(null);
      Assert.fail("Expected IllegalStateException on a closed channel");
    } catch (IllegalStateException expected) {
      // expected
    }
  }
}
