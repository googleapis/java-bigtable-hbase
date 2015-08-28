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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.api.client.util.Clock;
import com.google.api.client.util.Preconditions;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.common.annotations.VisibleForTesting;

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
  @VisibleForTesting
  enum CacheState {
    Good,
    Stale,
    Expired,
    Exception
  }

  private static final Metadata.Key<String> AUTHORIZATION_HEADER_KEY =
      Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);

  @VisibleForTesting
  static Clock clock = Clock.SYSTEM;

  @VisibleForTesting
  static class HeaderCacheElement {
    /**
     * This specifies how far in advance of a header expiration do we consider the token stale.
     * The Stale state indicates that the interceptor needs to do an asynchronous refresh.
     */
    public static final int TOKEN_STALENESS_MS = 75 * 1000;

    /**
     * After the token is "expired," the interceptor blocks gRPC calls. The Expired state indicates
     * that the interceptor needs to do a synchronous refresh.
     */
    public static final int TOKEN_EXPIRES_MS = 45 * 1000;

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
      Preconditions.checkState(staleTimeMs < expiresTimeMs);
    }

    public HeaderCacheElement(IOException exception) {
      this.exception = exception;
      this.header = null;
      this.staleTimeMs = -1;
      this.expiresTimeMs = -1;
    }

    public CacheState getCacheState() {
      if (exception != null) {
        return CacheState.Exception;
      }
      long now = clock.currentTimeMillis();
      if (now < staleTimeMs){
        return CacheState.Good;
      } else if (now < expiresTimeMs) {
        return CacheState.Stale;
      } else {
        return CacheState.Expired;
      }
    }
  }

  @VisibleForTesting
  final AtomicReference<HeaderCacheElement> headerCache = new AtomicReference<>();

  @VisibleForTesting
  final AtomicBoolean isRefreshing = new AtomicBoolean(false);
  private final ExecutorService executor;
  private final OAuth2Credentials credentials;

  public RefreshingOAuth2CredentialsInterceptor(ExecutorService scheduler,
      OAuth2Credentials credentials) {
    this.executor = Preconditions.checkNotNull(scheduler);
    this.credentials = Preconditions.checkNotNull(credentials);
  }


  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    // TODO(sduskis): If the call fails for Auth reasons, this does not properly propagate info that
    // would be in WWW-Authenticate, because it does not yet have access to the header.
    return new CheckedForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      protected void checkedStart(Listener<RespT> responseListener, Metadata.Headers headers)
          throws Exception {
        headers.put(AUTHORIZATION_HEADER_KEY, getHeader());
        delegate().start(responseListener, headers);
      }
    };
  }

  public void asyncRefresh() {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        doRefresh();
      }
    });
  }

  public void syncRefresh() throws IOException {
    synchronized (isRefreshing) {
      if (!isRefreshing.get()) {
        doRefresh();
      } else {
        while (isRefreshing.get() && getCacheState(this.headerCache.get()) != CacheState.Good) {
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
  @VisibleForTesting
  String getHeader() throws IOException {
    HeaderCacheElement headerCache = getCachedHeader();
    CacheState state = getCacheState(headerCache);
    switch (state) {
      case Good:
        break;
      case Stale:
        asyncRefresh();
        break;
      case Expired:
        syncRefresh();
        headerCache = getCachedHeader();
        break;
      default:
        throw new IllegalStateException("Could not process state: " + state);
    }
    return headerCache.header;
  }

  @VisibleForTesting
  static CacheState getCacheState(HeaderCacheElement headerCache) {
    return (headerCache == null) ? CacheState.Expired : headerCache.getCacheState();
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
  @VisibleForTesting
  boolean doRefresh() {
    boolean requiresRefresh = false;
    synchronized (isRefreshing) {
      if (!isRefreshing.get() && getCacheState(this.headerCache.get()) != CacheState.Good) {
        isRefreshing.set(true);
        requiresRefresh = true;
      }
    }
    if (!requiresRefresh) {
      return false;
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
        headerCache.set(cacheElement);
        isRefreshing.set(false);
        isRefreshing.notifyAll();
      }
    }
    return true;
  }
}
