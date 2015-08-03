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
package com.google.cloud.bigtable.grpc;

import com.google.api.client.util.Preconditions;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;

import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors.CheckedForwardingCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Client interceptor that authenticates all calls by binding header data provided by a credential.
 * Typically this will populate the Authorization header but other headers may also be filled out.
 *
 * <p> Uses the new and simplified Google auth library:
 * https://github.com/google/google-auth-library-java</p>
 *
 * <p> TODO: COPIED FROM io.grpc.auth.ClientAuthInterceptor.  The logic added here for
 * initialization and locking could be moved back to gRPC.  This implementation takes advantage
 * of the fact that all of the Bigtable endpoints are OAuth2 based.  It uses the OAuth AccessToken
 * to get the token value and next refresh time.  The refresh is scheduled asynchronously.</p>
 */
public class RefreshingOAuth2CredentialsInterceptor implements ClientInterceptor {

  /**
   * <p>This enum describes the states of the OAuth header.</p>
   *
   * <ol>
   *   <li> Good - fine to use, and does not need to be refreshed.
   *   <li> Stale - fine to use, but requires an async refresh
   *   <li> Expired - Cannot be used.  Wait for a new token to be loaded
   * </ol>
   */
  private enum CacheState {
    Good,
    Stale,
    Expired
  }

  private static class HeaderCacheElement {
    final IOException exception;
    final String header;
    final long staleTimeMs;
    final long expiresTimeMs;

    public HeaderCacheElement(AccessToken token) {
      this.exception = null;

      this.header = "Bearer " + token.getTokenValue();
      long tokenExpiresTime = token.getExpirationTime().getTime();
      this.staleTimeMs = tokenExpiresTime - TOKEN_STALENESS_MS;
      // Block until refresh at this point.
      this.expiresTimeMs = tokenExpiresTime - TOKEN_EXPIRES_MS;

    }

    public HeaderCacheElement(IOException exception) {
      this.exception = exception;
      this.header = null;
      this.staleTimeMs = -1;
      this.expiresTimeMs = -1;
    }

    public CacheState getCacheState() {
      long now = System.currentTimeMillis();
      if (now > expiresTimeMs) {
        return CacheState.Expired;
      } else if (now > staleTimeMs) {
        return CacheState.Stale;
      } else {
        return CacheState.Good;
      }
    }
  }

  /**
   * This specifies how far in advance of a header expiration do we consider the token stale.
   */
  private static final int TOKEN_STALENESS_MS = 60 * 1000;

  /**
   * Wait until last second before an AccessToken is expired to block gRPC calls.
   */
  private static final int TOKEN_EXPIRES_MS = 1 * 1000;


  private static final Metadata.Key<String> AUTHORIZATION_HEADER_KEY =
      Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);


  private final AtomicReference<HeaderCacheElement> headerCache = new AtomicReference<>();
  private final AtomicBoolean isRefreshing = new AtomicBoolean(false);
  private final ExecutorService executor;
  private final OAuth2Credentials credentials;

  public RefreshingOAuth2CredentialsInterceptor(ExecutorService scheduler,
      OAuth2Credentials credentials) {
    this.executor = Preconditions.checkNotNull(scheduler);
    this.credentials = Preconditions.checkNotNull(credentials);
  }

  @Override
  public <ReqT, RespT> Call<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                       Channel next) {
    // TODO(ejona86): If the call fails for Auth reasons, this does not properly propagate info that
    // would be in WWW-Authenticate, because it does not yet have access to the header.
    return new CheckedForwardingCall<ReqT, RespT>(next.newCall(method)) {
      @Override
      protected void checkedStart(Listener<RespT> responseListener, Metadata.Headers headers)
          throws Exception {
        headers.put(AUTHORIZATION_HEADER_KEY, getHeader());
        delegate().start(responseListener, headers);
      }
    };
  }

  public void asyncRefresh() {
    if (!isRefreshing.get()) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          doRefresh();
        }
      });
    }
  }

  public void syncRefresh() throws IOException {
    synchronized (isRefreshing) {
      if (!isRefreshing.get()) {
        doRefresh();
      } else {
        while (isRefreshing.get()) {
          try {
            isRefreshing.wait(250);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      }
    }
  }


  /**
   * Get the http credential header we need from a new oauth2 AccessToken.
   */
  private String getHeader() throws IOException {
    HeaderCacheElement headerCache = getCachedHeader();
    CacheState state = (headerCache == null) ? CacheState.Expired : headerCache.getCacheState();
    if (state == CacheState.Expired) {
      syncRefresh();
      headerCache = getCachedHeader();
    } else if (state == CacheState.Stale) {
      asyncRefresh();
    }
    return headerCache.header;
  }

  private HeaderCacheElement getCachedHeader() throws IOException {
    HeaderCacheElement headerCache = this.headerCache.get();
    if (headerCache != null && headerCache.exception != null) {
      throw headerCache.exception;
    }
    return headerCache;
  }

  /**
   * Perform a credentials refresh.
   */
  private void doRefresh() {
    boolean requiresRefresh = false;
    synchronized (isRefreshing) {
      if (!isRefreshing.get()) {
        isRefreshing.set(true);
        requiresRefresh = true;
        HeaderCacheElement headerCache = this.headerCache.get();
        CacheState state = (headerCache == null) ? CacheState.Expired : headerCache.getCacheState();
        requiresRefresh = state != CacheState.Good;
      }
    }
    if (!requiresRefresh) {
      return;
    }
    HeaderCacheElement cacheElement = null;
    try {
      AccessToken newToken = credentials.refreshAccessToken();
      if (newToken == null) {
        // The current implementations of refreshAccessToken() throw an IOException or return
        // a valid token. This handling is future proofing.
        cacheElement = new HeaderCacheElement(
            new IOException("Could not load the token for credentials: " + credentials));
      } else {
        cacheElement = new HeaderCacheElement(newToken);
      }
    } catch (IOException e) {
      cacheElement = new HeaderCacheElement(e);
    } catch (Exception e) {
      cacheElement = new HeaderCacheElement(new IOException("Could not read headers", e));
    } finally {
      synchronized (isRefreshing) {
        this.headerCache.set(cacheElement);
        isRefreshing.set(false);
        isRefreshing.notifyAll();
      }
    }
  }
}
