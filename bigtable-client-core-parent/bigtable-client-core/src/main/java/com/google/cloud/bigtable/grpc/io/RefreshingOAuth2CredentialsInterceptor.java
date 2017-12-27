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
import java.util.concurrent.TimeoutException;
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

  private static final Logger LOG = new Logger(RefreshingOAuth2CredentialsInterceptor.class);

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
    static final long TOKEN_STALENESS_MS = TimeUnit.MINUTES.toMillis(6);

    /**
     * After the token is "expired," the interceptor blocks gRPC calls. The Expired state indicates
     * that the interceptor needs to do a synchronous refresh.
     */
    // 5 minutes as per https://github.com/google/google-auth-library-java/pull/95
    static final long TOKEN_EXPIRES_MS = TimeUnit.MINUTES.toMillis(5);

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

  private final ExecutorService executor;

  @VisibleForTesting
  RateLimiter rateLimiter;

  private final OAuth2Credentials credentials;

  @VisibleForTesting
  final Object lock = new Object();

  // Note that the cache is volatile to allow us to peek for a Good value
  @VisibleForTesting
  @GuardedBy("lock")
  volatile HeaderCacheElement headerCache = EMPTY_HEADER;

  @VisibleForTesting
  @GuardedBy("lock")
  boolean isRefreshing = false;

  @VisibleForTesting
  @GuardedBy("lock")
  Future<HeaderCacheElement> futureToken = null;


  /**
   * <p>Constructor for RefreshingOAuth2CredentialsInterceptor.</p>
   *
   * @param scheduler a {@link ExecutorService} object.
   * @param credentials a {@link OAuth2Credentials} object.
   */
  public RefreshingOAuth2CredentialsInterceptor(ExecutorService scheduler, OAuth2Credentials credentials) {
    this.executor = Preconditions.checkNotNull(scheduler);
    this.credentials = Preconditions.checkNotNull(credentials);

    // Retry every 10 seconds.
    rateLimiter = RateLimiter.create(0.1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

      /**
       * If the header is invalid, this will be set to true. This flag will indicate that
       * delegate().start() was not called. If start() is not called, then don't call
       * delegate().request(), delegate().sendMessage() or delegate().halfClose();
       */
      private boolean unauthorized = false;

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        HeaderCacheElement headerCache = getHeaderSafe();

        if (!headerCache.status.isOk()) {
          responseListener.onClose(headerCache.status, new Metadata());
          unauthorized = true;
          return;
        }

        headers.put(AUTHORIZATION_HEADER_KEY, headerCache.header);

        delegate().start(new UnAuthResponseListener<>(responseListener, headerCache), headers);
      }

      @Override
      public void request(int numMessages) {
        if (!unauthorized) {
          delegate().request(numMessages);
        }
      }

      @Override
      public void sendMessage(ReqT message) {
        if (!unauthorized) {
          delegate().sendMessage(message);
        }
      }

      @Override
      public void halfClose() {
        if (!unauthorized) {
          delegate().halfClose();
        }
      }

      @Override
      public void cancel(String message, Throwable cause) {
        if (!unauthorized) {
          delegate().cancel(message, cause);
        }
      }
    };
  }

  @VisibleForTesting
  HeaderCacheElement getHeaderSafe() {
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
  @VisibleForTesting
  HeaderCacheElement getHeader() throws ExecutionException, InterruptedException, TimeoutException {
    final Future<HeaderCacheElement> deferredResult;

    // Optimize for the common case: do a volatile read to peek for a Good cache value
    HeaderCacheElement headerCacheUnsync = this.headerCache;
    if (headerCacheUnsync.getCacheState() == CacheState.Good) {
      return headerCacheUnsync;
    }

    // TODO(igorbernstein2): figure out how to make this work with appengine request scoped threads
    synchronized (lock) {
      CacheState state = headerCache.getCacheState();

      switch (state) {
        case Good:
          return headerCache;
        case Stale:
          asyncRefresh();
          return headerCache;
        case Expired:
        case Exception:
          // defer the future resolution (asyncRefresh will spin up a thread that will try to acquire the lock)
          deferredResult = asyncRefresh();
          break;
        default:
          return new HeaderCacheElement(
              Status.UNAUTHENTICATED
                  .withCause(new IllegalStateException("Could not process state: " + state))
          );
      }
    }
    return deferredResult.get(5, TimeUnit.SECONDS);
  }

  /**
   * Refresh the credentials and block. Will return an error if the credentials haven't
   * been refreshed.
   * This method should not be called while holding the refresh lock
   */
  HeaderCacheElement syncRefresh() {
    try {
      return asyncRefresh().get(5, TimeUnit.SECONDS);
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
  Future<HeaderCacheElement> asyncRefresh() {
    LOG.trace("asyncRefresh");

    synchronized (lock) {
      if (isRefreshing) {
        LOG.trace("asyncRefresh is already in progress");
        return futureToken;
      }
      isRefreshing = true;
      LOG.trace("asyncRefresh taking ownership");

      try {
        this.futureToken = executor.submit(new Callable<HeaderCacheElement>() {
          @Override
          public HeaderCacheElement call() throws Exception {
            try {
              HeaderCacheElement newToken = refreshCredentials();
              return updateToken(newToken);
            } finally {
              futureToken = null;
              isRefreshing = false;
            }
          }
        });
      } catch (RuntimeException e) {
        isRefreshing = false;
        throw e;
      }

      return this.futureToken;
    }
  }

  private HeaderCacheElement updateToken(HeaderCacheElement newToken) {
    synchronized (lock) {
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
    }
  }

  /**
   * Calls {@link com.google.auth.oauth2.OAuth2Credentials#refreshAccessToken()}.
   *
   * @return HeaderCacheElement containing either a valid {@link com.google.auth.oauth2.AccessToken} or an exception.
   */
  private HeaderCacheElement refreshCredentials() {
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

  private void revokeUnauthToken(HeaderCacheElement oldToken) {
    if (!rateLimiter.tryAcquire()) {
      LOG.trace("Rate limited");
      return;
    }

    synchronized (lock) {
      if (headerCache == oldToken) {
        LOG.warn("Got unauthenticated response from server, revoking the current token");
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
        revokeUnauthToken(origToken);
      }
      super.onClose(status, trailers);
    }
  }
}
