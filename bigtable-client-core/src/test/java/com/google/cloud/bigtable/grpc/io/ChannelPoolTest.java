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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.util.Collections;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigtable.grpc.io.ChannelPool.PooledChannel;

@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ChannelPoolTest {

  @Test
  public void testInterceptorIsCalled() throws Exception {
    Channel channel = mock(Channel.class);
    MethodDescriptor descriptor = mock(MethodDescriptor.class);
    ClientCall callStub = mock(ClientCall.class);
    HeaderInterceptor interceptor = mock(HeaderInterceptor.class);
    when(channel.newCall(same(descriptor), same(CallOptions.DEFAULT))).thenReturn(
      callStub);
    ChannelPool pool =
        new ChannelPool(new Channel[] { channel }, Collections.singletonList(interceptor));
    ClientCall call = pool.newCall(descriptor, CallOptions.DEFAULT);
    Metadata headers = new Metadata();
    call.start(null, headers);
    verify(interceptor, times(1)).updateHeaders(same(headers));
  }

  @Test
  public void testChannelsAreRoundRobinned() {
    Channel channel1 = mock(Channel.class);
    Channel channel2 = mock(Channel.class);
    MethodDescriptor descriptor = mock(MethodDescriptor.class);
    MockitoAnnotations.initMocks(this);
    ChannelPool pool = new ChannelPool(new Channel[] { channel1, channel2 }, null);
    pool.newCall(descriptor, CallOptions.DEFAULT);
    verify(channel1, times(1)).newCall(same(descriptor), same(CallOptions.DEFAULT));
    verify(channel2, times(0)).newCall(same(descriptor), same(CallOptions.DEFAULT));
    pool.newCall(descriptor, CallOptions.DEFAULT);
    verify(channel1, times(1)).newCall(same(descriptor), same(CallOptions.DEFAULT));
    verify(channel2, times(1)).newCall(same(descriptor), same(CallOptions.DEFAULT));
  }
  
  @Test
  public void testReturnToPool() {
    ChannelPool pool = new ChannelPool(new Channel[] { null, null }, null);
    assertEquals(2, pool.size());
    PooledChannel reserved = pool.reserveChannel();
    assertEquals(1, pool.size());
    reserved.returnToPool();
    assertEquals(2, pool.size());
  }

  @Test
  public void testReserveNeverExhaustsPool() {
    ChannelPool pool = new ChannelPool(new Channel[] { null, null }, null);
    for (int i = 0; i < 10; i++) {
      pool.reserveChannel();
      assertEquals(1, pool.size());
    }
  }
}
