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

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.hbase.BigtableOAuth2Credentials;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * A wrapper that implements @{@link Credentials} interface and delegates calls to the
 * underlying @{@link BigtableOAuth2Credentials} object. This is required to decouple the users of
 * shaded bigtable from the @{@link Credentials} class.
 */
@InternalApi("For internal usage only")
public final class BigtableCredentialsWrapper extends Credentials {

  private final BigtableOAuth2Credentials bigtableCredentials;

  public BigtableCredentialsWrapper(BigtableOAuth2Credentials btCredentials) {
    this.bigtableCredentials = btCredentials;
  }

  @Override
  public String getAuthenticationType() {
    // Taken from com.google.auth.oauth2.OAuth2Credentials
    return "OAuth2";
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
    try {
      return bigtableCredentials.getRequestMetadata(uri);
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch credentials.", e);
    }
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
}
