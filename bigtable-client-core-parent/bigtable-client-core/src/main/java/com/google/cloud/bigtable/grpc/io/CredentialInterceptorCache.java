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

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.CredentialOptions.CredentialType;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.util.ThreadUtil;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ClientInterceptor;
import io.grpc.auth.ClientAuthInterceptor;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Caches {@link OAuth2CredentialsInterceptor} for default authorization cases. In other types of
 * authorization, such as file based Credentials, it will create a new one.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class CredentialInterceptorCache {
  private static CredentialInterceptorCache instance = new CredentialInterceptorCache();

  /**
   * Getter for the field <code>instance</code>.
   *
   * @return a {@link com.google.cloud.bigtable.grpc.io.CredentialInterceptorCache} object.
   */
  public static CredentialInterceptorCache getInstance() {
    return instance;
  }

  private final ExecutorService executor =
      Executors.newCachedThreadPool(ThreadUtil.getThreadFactory("Credentials-Refresh-%d", true));

  private ClientInterceptor defaultCredentialInterceptor;

  private CredentialInterceptorCache() {}

  /**
   * Given {@link com.google.cloud.bigtable.config.CredentialOptions} that define how to look up
   * credentials, do the following:
   *
   * <ol>
   *   <li>Look up the credentials
   *   <li>If there are credentials, create a gRPC interceptor that gets OAuth2 security tokens and
   *       add the credentials to {@link io.grpc.CallOptions}. <br>
   *       NOTE: {@link OAuth2Credentials} ensures that the token stays fresh. It does token lookups
   *       asynchronously so that the calls themselves take as little performance penalty as
   *       possible.
   *   <li>Cache the interceptor in step #2 if the {@link
   *       com.google.cloud.bigtable.config.CredentialOptions} uses <a
   *       href="https://developers.google.com/identity/protocols/application-default-credentials">
   *       default application credentials </a>
   * </ol>
   *
   * @param credentialOptions Defines how credentials should be achieved
   * @param retryOptions a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   * @return a HeaderInterceptor
   * @throws java.io.IOException if any.
   * @throws java.security.GeneralSecurityException if any.
   */
  public synchronized ClientInterceptor getCredentialsInterceptor(
      CredentialOptions credentialOptions, RetryOptions retryOptions)
      throws IOException, GeneralSecurityException {
    // Default credentials is the most likely CredentialType. It's also the only CredentialType
    // that can be safely cached.
    boolean isDefaultCredentials =
        credentialOptions.getCredentialType() == CredentialType.DefaultCredentials;

    if (isDefaultCredentials && defaultCredentialInterceptor != null) {
      return defaultCredentialInterceptor;
    }

    Credentials credentials = CredentialFactory.getCredentials(credentialOptions);

    if (credentials == null) {
      return null;
    }

    if (credentials instanceof OAuth2Credentials) {
      OAuth2CredentialsInterceptor oauth2Interceptor =
          new OAuth2CredentialsInterceptor((OAuth2Credentials) credentials);
      if (isDefaultCredentials) {
        defaultCredentialInterceptor = oauth2Interceptor;
      }
      return oauth2Interceptor;
    }

    // Normal path
    ClientInterceptor jwtAuthInterceptor =
        new ClientAuthInterceptor(credentials, MoreExecutors.directExecutor());

    if (isDefaultCredentials) {
      defaultCredentialInterceptor = jwtAuthInterceptor;
    }

    return jwtAuthInterceptor;
  }
}
