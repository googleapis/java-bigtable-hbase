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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
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

import javax.annotation.Nullable;
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

  public static int TIMEOUT_SECONDS = 15;

  private static final Logger LOG = new Logger(RefreshingOAuth2CredentialsInterceptor.class);
  private static final HeaderCacheElement EMPTY_HEADER = new HeaderCacheElement(null, 0);

  private static final Metadata.Key<String> AUTHORIZATION_HEADER_KEY = Metadata.Key.of(
      "Authorization", Metadata.ASCII_STRING_MARSHALLER);

  @VisibleForTesting
  static Clock clock = Clock.SYSTEM;

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
    Good(true), Stale(true), Expired(true), Exception(true);

    final boolean isValid;

    CacheState(boolean isValid) {
      this.isValid = isValid;
    }

    public boolean isValid() {
      return isValid;
    }
  }

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
      Preconditions.checkNotNull(errorStatus, "Error status can't be null");
      Preconditions.checkArgument(!errorStatus.isOk(), "Error status can't be OK");
      this.status = errorStatus;
      this.header = null;
      this.actualExpirationTimeMs = 0;
    }

    CacheState getCacheState() {
      if (!status.isOk()) {
        return CacheState.Exception;
      }

      long timeLeft = actualExpirationTimeMs - clock.currentTimeMillis();

      if (timeLeft > TOKEN_STALENESS_MS) {
        return CacheState.Good;
      } else if (timeLeft > TOKEN_EXPIRES_MS) {
        return CacheState.Stale;
      } else {
        return CacheState.Expired;
      }
    }
  }

  @GuardedBy("lock")
  @Nullable
  private Future<HeaderCacheElement> futureToken = null;

  // Note that the cache is volatile to allow us to peek for a Good value
  @GuardedBy("lock")
  private volatile HeaderCacheElement headerCache = EMPTY_HEADER;

  private final Object lock = new Object();
  private final OAuth2Credentials credentials;
  private final ExecutorService executor;
  private final RateLimiter rateLimiter;


  /**
   * <p>Constructor for RefreshingOAuth2CredentialsInterceptor.</p>
   *
   * @param executor a {@link ExecutorService} object.
   * @param credentials a {@link OAuth2Credentials} object.
   */
  public RefreshingOAuth2CredentialsInterceptor(ExecutorService executor,
      OAuth2Credentials credentials) {
    this.executor = Preconditions.checkNotNull(executor);
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
        final HeaderCacheElement headerCache = getHeaderSafe();

        if (!headerCache.status.isOk()) {
          unauthorized = true;
          try {
            responseListener.onClose(headerCache.status, new Metadata());
          } catch (Exception e) {
            LOG.warn("Unexpected exception in responseListener.onClose()", e);
          }
          return;
        }

        headers.put(AUTHORIZATION_HEADER_KEY, headerCache.header);

        SimpleForwardingClientCallListener<RespT> listenerWrapper =
            new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onClose(Status status, Metadata trailers) {
                if (status == Status.UNAUTHENTICATED) {
                  revokeUnauthToken(headerCache);
                }
                delegate().onClose(status, trailers);
              }
            };

        delegate().start(listenerWrapper, headers);
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

  /**
   * Get the http credential header we need from a new oauth2 AccessToken.
   */
  @VisibleForTesting
  HeaderCacheElement getHeaderSafe() {
    try {
      return getHeader();
    } catch (Exception e) {
      LOG.warn("Unexpected exception in getHeaderSafe()", e);
      return new HeaderCacheElement(
        Status.UNAUTHENTICATED
            .withCause(e)
            .withDescription("Unexpected exception in getHeaderSafe()"));
    }
  }

  private HeaderCacheElement getHeader() {
    // Optimize for the common case: do a volatile read to peek for a Good cache value

    HeaderCacheElement headerCacheUnsync = this.headerCache;
    CacheState state = headerCacheUnsync.getCacheState();

    switch (state) {
      case Good:
        return headerCacheUnsync;
      case Stale:
        asyncRefresh();
        return headerCacheUnsync;
      case Expired:
      case Exception:
        return syncRefresh();
      default:
        return new HeaderCacheElement(
            Status.UNAUTHENTICATED
                .withDescription("Unexpected state in getHeader()")
                .withCause(new IllegalStateException("Could not process state: " + state))
        );
    }
  }

  @VisibleForTesting
  HeaderCacheElement syncRefresh() {
    try {
      return asyncRefresh().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Authentication retrieval was interrupted in getFromFuture().");
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Authentication was interrupted in getFromFuture().")
              .withCause(e)
      );
    } catch (ExecutionException e) {
      LOG.warn("Could not retrieve a value from the future.", e);
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Authentication ExecutionException.")
              .withCause(e)
      );
    } catch (TimeoutException e) {
      LOG.warn("Authentication timed out after %d seconds."
          + "  Consider setting RefreshingOAuth2CredentialsInterceptor.TIMEOUT_SECONDS"
          + "to a higher number.",
        e, TIMEOUT_SECONDS);
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Authentication timed out after " + TIMEOUT_SECONDS + " seconds.")
              .withCause(e)
      );
    }
  }

  @VisibleForTesting
  protected CacheState getCacheState() {
    return headerCache.getCacheState();
  }

  /**
   * Refreshes the OAuth2 token asynchronously. This method will only start an async refresh if
   * there isn't a currently running asynchronous refresh.
   */
  Future<HeaderCacheElement> asyncRefresh() {
    LOG.trace("asyncRefresh");

    synchronized (lock) {
      try {
        if (this.futureToken != null) {
          LOG.trace("asyncRefresh is already in progress");
          return this.futureToken;
        }
        LOG.trace("asyncRefresh taking ownership");
        Future<HeaderCacheElement> future = executor.submit(new Callable<HeaderCacheElement>() {
          @Override
          public HeaderCacheElement call() throws Exception {
            return updateToken(refreshAccessToken());
          }
        });

        // future.isDone() can happen if the executor is a directExecutor.
        if (future == null || !future.isDone()) {
          this.futureToken = future;
        }
        return future;
      } catch (RuntimeException e) {
        LOG.warn("Unexpected exception in asyncRefresh()", e);
        if (this.futureToken != null) {
          this.futureToken.cancel(true);
        }
        this.futureToken = null;
        return Futures.immediateFuture(new HeaderCacheElement(
            Status.UNAUTHENTICATED
                .withCause(e)
                .withDescription("Unexpected exception in asyncRefresh()")));
      }
    }
  }

  @VisibleForTesting
  boolean isRefreshing() {
    synchronized (lock) {
      return futureToken != null;
    }
  }

  private HeaderCacheElement updateToken(HeaderCacheElement newToken) {
    try {
      synchronized (lock) {
        CacheState newState = newToken.getCacheState();
        futureToken = null;
        // Update the token only if the new token is good or the old token is bad
        if (newState.isValid() || !headerCache.getCacheState().isValid()) {
          headerCache = newToken;
        } else {
          LOG.warn("Failed to refresh the access token. Falling back to existing token. "
              + "New token state: {}, status: {}", newState, newToken.status);
        }
      } 
    } catch (Exception e) {
      LOG.warn("Got an unexpected exception in updateToken.  Falling back to existing token.", e);
    }

    return headerCache;
  }

  /**
   * Calls {@link com.google.auth.oauth2.OAuth2Credentials#refreshAccessToken()}.
   *
   * @return HeaderCacheElement containing either a valid {@link com.google.auth.oauth2.AccessToken} or an exception.
   */
  private HeaderCacheElement refreshAccessToken() {
    LOG.info("Refreshing the OAuth token");
    try {
      return new HeaderCacheElement(credentials.refreshAccessToken());
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

}
