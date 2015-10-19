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


import com.google.cloud.bigtable.grpc.io.UserAgentInterceptor;
import com.google.common.net.HttpHeaders;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import io.grpc.Metadata;

@RunWith(JUnit4.class)
public class UserAgentUpdaterInterceptorTest {

  private static final String userAgent = "project/version";
  
  private UserAgentInterceptor interceptor = new UserAgentInterceptor(userAgent);

  @Test
  public void interceptCall_addHeader() throws Exception {
    Metadata headers = new Metadata();
    interceptor.updateHeaders(headers);

    Metadata.Key<String> key =
        Metadata.Key.of(HttpHeaders.USER_AGENT, Metadata.ASCII_STRING_MARSHALLER);
    Assert.assertEquals(userAgent, headers.get(key));
  }

  @Test
  public void interceptCall_appendHeader() throws Exception {
    Metadata headers = new Metadata();
    Metadata.Key<String> key =
        Metadata.Key.of(HttpHeaders.USER_AGENT, Metadata.ASCII_STRING_MARSHALLER);
    headers.put(key, "dummy");
    interceptor.updateHeaders(headers);

    Assert.assertEquals("dummy " + userAgent, headers.get(key));
  }
}
