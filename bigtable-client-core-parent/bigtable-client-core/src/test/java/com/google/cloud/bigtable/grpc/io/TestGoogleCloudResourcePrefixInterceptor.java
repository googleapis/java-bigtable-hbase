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

import static com.google.cloud.bigtable.grpc.io.GoogleCloudResourcePrefixInterceptor.GRPC_RESOURCE_PREFIX_KEY;
import static org.mockito.Matchers.any;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestGoogleCloudResourcePrefixInterceptor {

  public static String HEADER_VALUE = "fully-qualified-name";
  @Mock
  ManagedChannel channel;
  @Mock
  ClientCall call;
  private ChannelPool cp;

  @Before
  public void setup() throws IOException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(channel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
        .thenReturn(call);

    cp = new ChannelPool(
        Arrays.<HeaderInterceptor> asList(new GoogleCloudResourcePrefixInterceptor(HEADER_VALUE)),
        Collections.singletonList(channel));
  }

  @Test
  public void testNewHeader() {
    new GoogleCloudResourcePrefixInterceptor(HEADER_VALUE);
    ClientCall<MutateRowRequest, MutateRowResponse> call =
        cp.newCall(BigtableGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT);

    Metadata headers = new Metadata();
    call.start(null, headers);
    Assert.assertEquals(HEADER_VALUE, headers.get(GRPC_RESOURCE_PREFIX_KEY));
  }

  @Test
  public void testExistingHeader() {
    new GoogleCloudResourcePrefixInterceptor(HEADER_VALUE);
    ClientCall<MutateRowRequest, MutateRowResponse> call =
        cp.newCall(BigtableGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT);

    Metadata headers = new Metadata();
    String overrideValue = "override-value";
    headers.put(GRPC_RESOURCE_PREFIX_KEY, overrideValue);
    call.start(null, headers);
    Assert.assertEquals(overrideValue, headers.get(GRPC_RESOURCE_PREFIX_KEY));
  }
}
