/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.auth.Credentials;
import com.google.auth.RequestMetadataCallback;
import com.google.cloud.bigtable.hbase.BigtableOAuthCredentials;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * A wrapper that implements @{@link Credentials} interface and delegates calls to the
 * underlying @{@link BigtableOAuthCredentials} object. This is required to decouple the users of
 * shaded bigtable from the @{@link Credentials} class.
 */
public final class BigtableCredentialsWrapper extends Credentials {

  public BigtableCredentialsWrapper(BigtableOAuthCredentials btCredentials) {
    this.bigtableCredentials = btCredentials;
  }

  private final BigtableOAuthCredentials bigtableCredentials;

  @Override
  public String getAuthenticationType() {
    return "OAuth2";
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
    try {
      return bigtableCredentials.getRequestMetadata(uri, MoreExecutors.directExecutor()).get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch credentials.", e);
    }
  }

  public void getRequestMetadata(
      final URI uri, Executor executor, final RequestMetadataCallback callback) {
    executor.execute(() -> blockingGetToCallback(uri, callback));
  }

  @Override
  public boolean hasRequestMetadata() {
    return true;
  }

  @Override
  public boolean hasRequestMetadataOnly() {
    return true;
  }

  @Override
  public void refresh() throws IOException {
    // No-op. btCredentials should refresh internally as required.
  }

  public BigtableOAuthCredentials getBigtableCredentials() {
    return bigtableCredentials;
  }
}
