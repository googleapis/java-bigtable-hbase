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

import static org.mockito.Mockito.when;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.bigtable.grpc.io.UserAgentInterceptor;
import com.google.common.net.HttpHeaders;
import com.google.protobuf.Empty;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Channel;
import io.grpc.Metadata;

@RunWith(JUnit4.class)
public class UserAgentUpdaterInterceptorTest {

  private static final String userAgent = "project/version";
  
  @Mock
  private Channel channelStub;
  @Mock
  private ClientCall<MutateRowRequest, Empty> callStub;
  @Mock
  private ClientCall.Listener<Empty> responseListenerStub;

  private UserAgentInterceptor interceptor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    interceptor = new UserAgentInterceptor(userAgent);
  }

  @Test
  public void interceptCall_addHeader() {
    when(channelStub.newCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT))
        .thenReturn(callStub);

    ClientCall<MutateRowRequest, Empty> wrappedCall =
        interceptor.interceptCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT,
          channelStub);
   Metadata headers = new Metadata();
    wrappedCall.start(responseListenerStub, headers);

    Metadata.Key<String> key =
        Metadata.Key.of(HttpHeaders.USER_AGENT, Metadata.ASCII_STRING_MARSHALLER);
    Assert.assertEquals(userAgent, headers.get(key));
  }

  @Test
  public void interceptCall_appendHeader() {
    when(channelStub.newCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT))
        .thenReturn(callStub);

    ClientCall<MutateRowRequest, Empty> wrappedCall =
        interceptor.interceptCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT,
          channelStub);
    Metadata headers = new Metadata();
    Metadata.Key<String> key =
        Metadata.Key.of(HttpHeaders.USER_AGENT, Metadata.ASCII_STRING_MARSHALLER);
    headers.put(key, "dummy");
    wrappedCall.start(responseListenerStub, headers);

    Assert.assertEquals("dummy " + userAgent, headers.get(key));
  }
}
