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
import io.grpc.Metadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGoogleCloudResourcePrefixInterceptor {

  public static String HEADER_VALUE = "fully-qualified-name";
  private GoogleCloudResourcePrefixInterceptor underTest;

  @Before
  public void setup() {
    underTest = new GoogleCloudResourcePrefixInterceptor(HEADER_VALUE);
  }

  @Test
  public void testNewHeader() {
    Metadata headers = new Metadata();
    underTest.updateHeaders(headers);
    Assert.assertEquals(HEADER_VALUE, headers.get(GRPC_RESOURCE_PREFIX_KEY));
  }

  @Test
  public void testExistingHeader() {
    Metadata headers = new Metadata();
    String overrideValue = "override-value";
    headers.put(GRPC_RESOURCE_PREFIX_KEY, overrideValue);
    underTest.updateHeaders(headers);
    Assert.assertEquals(overrideValue, headers.get(GRPC_RESOURCE_PREFIX_KEY));
  }
}
