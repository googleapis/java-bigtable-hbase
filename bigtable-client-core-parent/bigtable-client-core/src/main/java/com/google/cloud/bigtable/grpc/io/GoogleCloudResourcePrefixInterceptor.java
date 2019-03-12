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

import io.grpc.Metadata;

/**
 * Adds a header ("google-cloud-resource-prefix") that usually contains a fully qualified instance
 * or table name.
 *
 * @author sduskis
 * @version $Id: $Id
 * @since 0.9.2
 */
public class GoogleCloudResourcePrefixInterceptor extends HeaderInterceptor {

  /** Constant <code>GRPC_RESOURCE_PREFIX_KEY</code> */
  public static final Metadata.Key<String> GRPC_RESOURCE_PREFIX_KEY =
      Metadata.Key.of("google-cloud-resource-prefix", Metadata.ASCII_STRING_MARSHALLER);

  /**
   * <p>Constructor for GoogleCloudResourcePrefixInterceptor.</p>
   *
   * @param defaultValue a {@link java.lang.String} object.
   */
  public GoogleCloudResourcePrefixInterceptor(String defaultValue) {
    super(GRPC_RESOURCE_PREFIX_KEY, defaultValue);
  }
}
