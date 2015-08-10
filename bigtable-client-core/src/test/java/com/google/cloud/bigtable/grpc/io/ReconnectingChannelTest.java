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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;

import java.io.Closeable;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigtable.grpc.io.ReconnectingChannel;

/**
 * Tests {@link ReconnectingChannel}
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class ReconnectingChannelTest {

  private static final long REFRESH_MS = 500;
  @Mock
  private ReconnectingChannel.Factory mockFactory;

  @Mock
  private Channel mockChannel;

  @Mock
  private Closeable mockCloseable;

  @Mock
  private Call<?, ?> mockCall;

  @Before
  public void setup() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(mockFactory.createChannel()).thenReturn(mockChannel);
    when(mockFactory.createClosable(any(Channel.class))).thenReturn(mockCloseable);
    when(mockChannel.newCall(any(MethodDescriptor.class))).thenReturn(mockCall);
  }

  @Test
  public void test() throws IOException{
    ReconnectingChannel underTest = new ReconnectingChannel(REFRESH_MS, mockFactory);

    Mockito.verify(mockFactory, times(1)).createChannel();

    underTest.newCall(null).start(null, null);
    Mockito.verify(mockChannel, times(1)).newCall(any(MethodDescriptor.class));

    try {
      Thread.sleep(REFRESH_MS);
    } catch (InterruptedException ignored) {
      // Do nothing on interrupt.
    }

    underTest.newCall(null).start(null, null);
    Mockito.verify(mockChannel, times(2)).newCall(any(MethodDescriptor.class));

    try {
      Thread.sleep(REFRESH_MS);
    } catch (InterruptedException ignored) {
      // Do nothing on interrupt.
    }

    Mockito.verify(mockFactory, atLeast(2)).createChannel();
    Mockito.verify(mockCloseable, times(1)).close();

    underTest.close();
    Mockito.verify(mockCloseable, times(2)).close();

    try {
      underTest.newCall(null).start(null, null);
      Assert.fail("Expected IllegalStateException on a closed channel");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void testZeroRefreshMs() throws IOException {
    ReconnectingChannel underTest = new ReconnectingChannel(0, mockFactory);
    Assert.assertFalse(underTest.requiresRefresh());
    underTest.close();
  }
}
