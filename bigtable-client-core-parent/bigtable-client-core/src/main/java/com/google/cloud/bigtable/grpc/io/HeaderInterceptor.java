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

import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.StatusException;

/**
 * Adds a header ("google-cloud-resource-prefix") that usually contains a fully qualified instance
 * or table name.
 *
 * @author sduskis
 * @version $Id: $Id
 * @since 0.9.2
 */
public class HeaderInterceptor implements ClientInterceptor {

  private final Metadata.Key<String> key;
  private final String defaultValue;

  /**
   * <p>Constructor for GoogleCloudResourcePrefixInterceptor.</p>
   *
   * @param defaultValue a {@link String} object.
   */
  public HeaderInterceptor(Metadata.Key<String> key, String defaultValue) {
    this.key = key;
    this.defaultValue = defaultValue;
  }

  /** {@inheritDoc} */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    return new CheckedForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      protected void checkedStart(Listener<RespT> responseListener, Metadata headers) {
        updateHeaders(headers);
        delegate().start(responseListener, headers);
      }
    };
  }

  @VisibleForTesting
  public void updateHeaders(Metadata headers) {
    if (!headers.containsKey(key)) {
      headers.put(key, defaultValue);
    }
  }

  @Override
  public String toString() {
    return String.format("%s: (key='%s' value='%s')", getClass().getName(), key.name(), defaultValue);
  }
}
