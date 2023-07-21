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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.hadoop.conf.Configuration;

/**
 * A public interface that should be implemented by the users who want to inject a custom {@link
 * com.google.auth.Credentials} implementation for auth purposes. Clients can't directly override
 * the {@link com.google.auth.Credentials} class as it is shaded by Cloud Bigtable client.
 *
 * <p>Hence, customers should implement this class, which will be used for authentication. The
 * authentication should be based on OAuth and must work by just including request metadata with
 * each request at transport layer.
 */
public abstract class BigtableOAuthCredentials {

  /**
   * All subclasses must implement this constructor and populate the @configuration. The
   * configuration should be used to initialize the credentials.
   *
   * @param configuration The HBase configuration
   */
  public BigtableOAuthCredentials(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Get the current request metadata.
   *
   * <p>This should be called by the transport layer on each request, and the data should be
   * populated in headers or other context.
   *
   * <p>The convention for handling binary data is for the key in the returned map to end with
   * {@code "-bin"} and for the corresponding values to be base64 encoded.
   *
   * <p>This class should handle caching and refeshing of the metadata associated with the request.
   *
   * @param uri URI of the entry point for the request.
   */
  public abstract CompletableFuture<Map<String, List<String>>> getRequestMetadata(
      URI uri, Executor executor) throws IOException;

  /** Returns the HBase configuration used to create this object. */
  public Configuration getConfiguration() {
    return configuration;
  }

  protected Configuration configuration;
}
