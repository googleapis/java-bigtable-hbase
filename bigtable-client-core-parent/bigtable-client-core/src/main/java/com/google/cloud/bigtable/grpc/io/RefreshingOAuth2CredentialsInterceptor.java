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

import com.google.api.client.util.Clock;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigtable.config.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

/**
 * Client interceptor that authenticates all calls by binding header data provided by a credential.
 * Typically this will populate the Authorization header but other headers may also be filled out.
 * <p>
 * Uses the new and simplified Google auth library:
 * https://github.com/google/google-auth-library-java
 * </p>
 * <p>
 * TODO: COPIED FROM io.grpc.auth.ClientAuthInterceptor. The logic added here for initialization and
 * locking could be moved back to gRPC. This implementation takes advantage of the fact that all of
 * the Bigtable endpoints are OAuth2 based. It uses the OAuth AccessToken to get the token value and
 * next refresh time. The refresh is scheduled asynchronously.
 * </p>
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class RefreshingOAuth2CredentialsInterceptor implements ClientInterceptor {

  /**
   * <p>
   * This enum describes the states of the OAuth header.
   * </p>
   * <ol>
   * <li>Good - fine to use, and does not need to be refreshed.
   * <li>Stale - fine to use, but requires an async refresh
   * <li>Expired - Cannot be used. Wait for a new token to be loaded
   * </ol>
   */
  @VisibleForTesting
  enum CacheState {
    Good, Stale, Expired, Exception
  }

  private static final Logger LOG = new Logger(RefreshingOAuth2CredentialsInterceptor.class);
  private static final Metadata.Key<String> AUTHORIZATION_HEADER_KEY = Metadata.Key.of(
      "Authorization", Metadata.ASCII_STRING_MARSHALLER);

  @VisibleForTesting
  static Clock clock = Clock.SYSTEM;


  @VisibleForTesting
  static class HeaderCacheElement {

    /**
     * This specifies how far in advance of a header expiration do we consider the token stale. The
     * Stale state indicates that the interceptor needs to do an asynchronous refresh.
     */
    static final int TOKEN_STALENESS_MS = 75 * 1000;

    /**
     * After the token is "expired," the interceptor blocks gRPC calls. The Expired state indicates
     * that the interceptor needs to do a synchronous refresh.
     */
    static final int TOKEN_EXPIRES_MS = 15 * 1000;

    final Status status;
    final String header;
    final long actualExpirationTimeMs;

    HeaderCacheElement(AccessToken token) {
      this.status = Status.OK;
      if (token.getExpirationTime() == null) {
        actualExpirationTimeMs = Long.MAX_VALUE;
      } else {
        actualExpirationTimeMs = token.getExpirationTime().getTime();
      }
      header = "Bearer " + token.getTokenValue();
    }

    HeaderCacheElement(String header, long actualExpirationTimeMs) {
      this.status = Status.OK;
      this.header = header;
      this.actualExpirationTimeMs = actualExpirationTimeMs;
    }

    HeaderCacheElement(Status errorStatus) {
      Preconditions.checkArgument(!errorStatus.isOk(), "Error status can't be OK");
      this.status = errorStatus;
      this.header = null;
      this.actualExpirationTimeMs = 0;
    }

    CacheState getCacheState() {
      long now = clock.currentTimeMillis();

      if (!status.isOk()) {
        return CacheState.Exception;
      } else if (actualExpirationTimeMs - TOKEN_EXPIRES_MS <= now) {
        return CacheState.Expired;
      } else if (actualExpirationTimeMs - TOKEN_STALENESS_MS <= now) {
        return CacheState.Stale;
      } else {
        return CacheState.Good;
      }
    }
  }

  private static final HeaderCacheElement EMPTY_HEADER = new HeaderCacheElement(null, 0);

  private final ListeningExecutorService executor;

  @VisibleForTesting
  final RateLimiter rateLimiter;

  private final boolean isAppEngine;

  private final OAuth2Credentials credentials;

  final Object lock = new Object();

  @VisibleForTesting
  @GuardedBy("lock")
  HeaderCacheElement headerCache = EMPTY_HEADER;

  @GuardedBy("lock")
  boolean refreshing = false;

  @GuardedBy("lock")
  private ListenableFuture<HeaderCacheElement> futureToken = null;


  /**
   * <p>Constructor for RefreshingOAuth2CredentialsInterceptor.</p>
   *
   * @param scheduler a {@link ExecutorService} object.
   * @param credentials a {@link OAuth2Credentials} object.
   */
  public RefreshingOAuth2CredentialsInterceptor(ExecutorService scheduler,
      OAuth2Credentials credentials) {
    this.executor = MoreExecutors.listeningDecorator(Preconditions.checkNotNull(scheduler));
    this.credentials = Preconditions.checkNotNull(credentials);

    // From MoreExecutors
    this.isAppEngine = System.getProperty("com.google.appengine.runtime.environment") != null;

    rateLimiter = RateLimiter.create(1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        HeaderCacheElement headerCache = getHeaderSafe();

        if (!headerCache.status.isOk()) {
          responseListener.onClose(headerCache.status, new Metadata());
          return;
        }

        headers.put(AUTHORIZATION_HEADER_KEY, headerCache.header);

        delegate().start(new UnAuthResponseListener<>(responseListener, headerCache), headers);
      }
    };
  }

  private HeaderCacheElement getHeaderSafe() {
    try {
      return getHeader();
    } catch (Exception e) {
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Unexpected failure get auth token")
              .withCause(e)
      );
    }
  }

  /**
   * Get the http credential header we need from a new oauth2 AccessToken.
   */
  private HeaderCacheElement getHeader() throws ExecutionException, InterruptedException {
    final Future<HeaderCacheElement> deferredResult;

    synchronized (lock) {
      CacheState state = headerCache.getCacheState();

      if (state == CacheState.Good || state == CacheState.Exception) {
        return headerCache;
        // AppEngine doesn't allow threads to outlive the request, so disable background refresh
      } else if (!isAppEngine && state == CacheState.Stale) {
        asyncRefresh();
        return headerCache;
      } else if (state == CacheState.Expired) {
        // defer the future resolution (asyncRefresh will spin up a thread that will try to acquire the lock)
        deferredResult = asyncRefresh();
      } else {
        return new HeaderCacheElement(
            Status.UNAUTHENTICATED
                .withCause(new IllegalStateException("Could not process state: " + state))
        );
      }
    }
    return deferredResult.get();
  }

  /**
   * Refresh the credentials and block. Will return an error if the credentials haven't
   * been refreshed.
   *
   * This method should not be called while holding the refresh lock
   */
  HeaderCacheElement syncRefresh() {
    try {
      return asyncRefresh().get(250, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withCause(e)
      );
    }
  }

  /**
   * Refreshes the OAuth2 token asynchronously. This method will only start an async refresh if
   * there isn't a currently running asynchronous refresh.
   */
  ListenableFuture<HeaderCacheElement> asyncRefresh() {
    synchronized (lock) {
      if (refreshing) {
        return futureToken;
      }
      refreshing = true;

      try {
        this.futureToken = executor.submit(new Callable<HeaderCacheElement>() {
          @Override
          public HeaderCacheElement call() throws Exception {
            HeaderCacheElement newToken = refreshCredentials();
            return updateToken(newToken);
          }
        });
      } catch (RuntimeException e) {
        refreshing = false;
        throw e;
      }

      return this.futureToken;
    }
  }

  private HeaderCacheElement updateToken(HeaderCacheElement newToken) {
    synchronized (lock) {
      try {
        // Update the token only if the new token is good or the old token is bad
        CacheState newState = newToken.getCacheState();
        boolean newTokenOk = newState == CacheState.Good || newState == CacheState.Stale;
        CacheState oldCacheState = headerCache.getCacheState();
        boolean oldTokenOk = oldCacheState == CacheState.Good || oldCacheState == CacheState.Stale;

        if (newTokenOk || !oldTokenOk) {
          headerCache = newToken;
        } else {
          LOG.warn("Failed to refresh the access token. Falling back to existing token. "
              + "New token state: {}, status: {}", newState, newToken.status);
        }
        return headerCache;
      } finally {
        futureToken = null;
        refreshing = false;
      }
    }
  }

  /**
   * Calls {@link com.google.auth.oauth2.OAuth2Credentials#refreshAccessToken()}.
   *
   * @return HeaderCacheElement containing either a valid {@link com.google.auth.oauth2.AccessToken} or an exception.
   */
  protected HeaderCacheElement refreshCredentials() {
    if (!rateLimiter.tryAcquire()) {
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Authentication rate limit has been exceeded, failing fast")
      );
    }

    try {
      LOG.info("Refreshing the OAuth token");
      AccessToken newToken = credentials.refreshAccessToken();
      return new HeaderCacheElement(newToken);
    } catch (Exception e) {
      LOG.warn("Got an unexpected exception while trying to refresh google credentials.", e);
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Unexpected error trying to authenticate")
              .withCause(e)
      );
    }
  }

  void revokeUnauthToken(HeaderCacheElement oldToken) {
    synchronized (lock) {
      if (headerCache == oldToken) {
        headerCache = EMPTY_HEADER;
      } else {
        LOG.info("Skipping revoke, since the revoked token has already changed");
      }
    }
  }

  class UnAuthResponseListener<RespT> extends SimpleForwardingClientCallListener<RespT> {

    private final HeaderCacheElement origToken;

    UnAuthResponseListener(Listener<RespT> delegate, HeaderCacheElement origToken) {
      super(delegate);
      this.origToken = origToken;
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
      if (status == Status.UNAUTHENTICATED) {
        LOG.warn("Got unauthenticated response from server, revoking the current token");
        revokeUnauthToken(origToken);
      }
      super.onClose(status, trailers);
    }
  }
}
