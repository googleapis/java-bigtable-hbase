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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ChannelPoolTest {

  private static class MockChannelFactory implements ChannelPool.ChannelFactory {
    List<ManagedChannel> channels = new ArrayList<>();

    @Override
    public ManagedChannel create() throws IOException {
      final ManagedChannel channel = mock(ManagedChannel.class);
      final AtomicBoolean isShutdown = new AtomicBoolean();
      ClientCall callStub = mock(ClientCall.class);
      when(channel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
          .thenReturn(callStub);
      when(channel.authority()).thenReturn("");
      when(channel.shutdown()).thenAnswer(new Answer<ManagedChannel>() {
        @Override
        public ManagedChannel answer(InvocationOnMock invocation) throws Throwable {
          isShutdown.set(true);
          return channel;
        }
      });
      when(channel.isShutdown()).then(isShutdownAnswer(isShutdown));
      when(channel.isTerminated()).then(isShutdownAnswer(isShutdown));
      channels.add(channel);
      return channel;
    }

    protected Answer<Boolean> isShutdownAnswer(final AtomicBoolean isShutdown) {
      return new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          return isShutdown.get();
        }
      };
    }
  }

  @Test
  public void testInterceptorIsCalled() throws Exception {
    MethodDescriptor descriptor = mock(MethodDescriptor.class);
    HeaderInterceptor interceptor = mock(HeaderInterceptor.class);
    ChannelPool pool =
        new ChannelPool(Collections.singletonList(interceptor), new MockChannelFactory());
    ClientCall call = pool.newCall(descriptor, CallOptions.DEFAULT);
    Metadata headers = new Metadata();
    call.start(null, headers);
    verify(interceptor, times(1)).updateHeaders(same(headers));
  }

  @Test
  public void testChannelsAreRoundRobinned() throws IOException {
    MockChannelFactory factory = new MockChannelFactory();
    MethodDescriptor descriptor = mock(MethodDescriptor.class);
    MockitoAnnotations.initMocks(this);
    ChannelPool pool = new ChannelPool(null, factory);
    pool.ensureChannelCount(2);
    pool.newCall(descriptor, CallOptions.DEFAULT);
    verify(factory.channels.get(0), times(1)).newCall(same(descriptor), same(CallOptions.DEFAULT));
    verify(factory.channels.get(1), times(0)).newCall(same(descriptor), same(CallOptions.DEFAULT));
    pool.newCall(descriptor, CallOptions.DEFAULT);
    verify(factory.channels.get(0), times(1)).newCall(same(descriptor), same(CallOptions.DEFAULT));
    verify(factory.channels.get(1), times(1)).newCall(same(descriptor), same(CallOptions.DEFAULT));
}

  @Test
  public void testEnsureCapcity() throws IOException {
    MockChannelFactory factory = new MockChannelFactory();
    ChannelPool pool = new ChannelPool(null, factory);
    pool.ensureChannelCount(4);
    Assert.assertEquals(4, factory.channels.size());
    Assert.assertEquals(4, pool.size());
  }

  @Test
  public void testShutdown() throws IOException {
    MockChannelFactory factory = new MockChannelFactory();
    new ChannelPool(null, factory).shutdown();
    for (ManagedChannel managedChannel : factory.channels) {
      verify(managedChannel, times(1)).shutdown();
    }
  }

  @Test
  public void testShutdownNow() throws IOException {
    MockChannelFactory factory = new MockChannelFactory();
    new ChannelPool(null, factory).shutdownNow();
    for (ManagedChannel managedChannel : factory.channels) {
      verify(managedChannel, times(1)).shutdownNow();
    }
  }

  @Test
  public void testAwaitTermination() throws IOException, InterruptedException {
    MockChannelFactory factory = new MockChannelFactory();
    ChannelPool pool = new ChannelPool(null, factory);
    pool.ensureChannelCount(5);
    for (ManagedChannel managedChannel : factory.channels) {
      when(managedChannel.isTerminated()).thenReturn(false);
    }
    pool.awaitTermination(500, TimeUnit.MILLISECONDS);
    for (ManagedChannel managedChannel : factory.channels) {
      verify(managedChannel, times(1)).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
    }
  }
}
