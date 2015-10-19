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
import static org.mockito.Matchers.eq;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ChannelPoolTest {

  @Mock
  private Channel channel;
  @Mock
  private MethodDescriptor descriptor;
  @Mock
  private ClientCall callStub;
  @Mock
  private ClientCall.Listener responseListenerStub;
  @Mock
  private HeaderInterceptor interceptor;

  @Test
  public void testInterceptorIsCalled() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(channel.newCall(any(MethodDescriptor.class), any(CallOptions.class))).thenReturn(
      callStub);
    ChannelPool pool =
        new ChannelPool(new Channel[] { channel }, Collections.singletonList(interceptor));
    ClientCall call = pool.newCall(descriptor, CallOptions.DEFAULT);
    Metadata headers = new Metadata();
    call.start(null, headers);
    verify(interceptor, times(1)).updateHeaders(eq(headers));
  }
}
