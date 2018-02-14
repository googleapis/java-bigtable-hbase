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
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.Closeable;
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

  public static int TIMEOUT_SECONDS = 15;

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
    Good(true), Stale(true),
    Expired(false), Exception(false);

    private boolean isValid;

    CacheState(boolean isValid) {
      this.isValid = isValid;
    }

    public boolean isValid() {
      return isValid;
    }
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

    boolean isValid() {
      return getCacheState().isValid();
    }
  }

  private static final HeaderCacheElement EMPTY_HEADER = new HeaderCacheElement(null, 0);

  private final ExecutorService executor;
  private final RateLimiter rateLimiter;
  private final OAuth2Credentials credentials;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private Future<HeaderCacheElement> futureToken = null;

  // Note that the cache is volatile to allow us to peek for a Good value
  @VisibleForTesting
  @GuardedBy("lock")
  volatile HeaderCacheElement headerCache = EMPTY_HEADER;


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
      private volatile boolean unauthorized = false;

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        HeaderCacheElement headerCache = getHeaderSafe();

        if (!headerCache.status.isOk()) {
          unauthorized = true;
          responseListener.onClose(headerCache.status, new Metadata());
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
      LOG.warn("Got an unexpected exception while trying to refresh google credentials.", e);
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
  HeaderCacheElement getHeader() {

    // Optimize for the common case: do a volatile read to peek for a Good cache value
    HeaderCacheElement headerCacheUnsync = this.headerCache;

    // TODO(igorbernstein2): figure out how to make this work with appengine request scoped threads
    switch (headerCacheUnsync.getCacheState()) {
      case Good:
        return headerCacheUnsync;
      case Stale:
        asyncRefresh();
        return headerCacheUnsync;
      case Expired:
      case Exception:
        // defer the future resolution (asyncRefresh will spin up a thread that will try to acquire the lock)
        return syncRefresh();
      default:
        String message = "Could not process state: " + headerCacheUnsync.getCacheState();
        LOG.warn(message);
        return new HeaderCacheElement(
            Status.UNAUTHENTICATED
                .withCause(new IllegalStateException(message)));
    }
  }

  /**
   * Refresh the credentials and block. Will return an error if the credentials haven't
   * been refreshed.
   * This method should not be called while holding the refresh lock
   */
  HeaderCacheElement syncRefresh() {
    try (Closeable ss = Tracing.getTracer().spanBuilder("CredentialsRefresh").startScopedSpan()) {
      return asyncRefresh().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while trying to refresh google credentials.", e);
      Thread.currentThread().interrupt();
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Authentication was interrupted.")
              .withCause(e)
      );
    } catch (ExecutionException e) {
      LOG.warn("ExecutionException while trying to refresh google credentials.", e);
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("ExecutionException during Authentication.")
              .withCause(e)
      );
    } catch (TimeoutException e) {
      LOG.warn("TimeoutException while trying to refresh google credentials.", e);
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("TimeoutException during Authentication.")
              .withCause(e)
      );
    } catch (Exception e) {
      LOG.warn("Unexpected execption while trying to refresh google credentials.", e);
      return new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Unexpected execption during Authentication.")
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
      try {
        if (futureToken != null) {
          return futureToken;
        }
        if (headerCache.getCacheState() == CacheState.Good) {
          return Futures.immediateFuture(headerCache);
        }

        Future<HeaderCacheElement> future = executor.submit(new Callable<HeaderCacheElement>() {
          @Override
          public HeaderCacheElement call() throws Exception {
            return updateToken();
          }
        });

        if (!future.isDone()) {
          this.futureToken = future;
        }
        return future;
      } catch (RuntimeException e) {
        futureToken = null;
        LOG.warn("Got an unexpected exception while trying to refresh google credentials.", e);
        return Futures.immediateFuture(new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Unexpected error trying to authenticate")
              .withCause(e)));
      }
    }
  }

  private HeaderCacheElement updateToken() {
    HeaderCacheElement newToken;
    try {
      LOG.info("Refreshing the OAuth token");
      newToken = new HeaderCacheElement(credentials.refreshAccessToken());
    } catch (Exception e) {
      LOG.warn("Got an unexpected exception while trying to refresh google credentials.", e);
      newToken = new HeaderCacheElement(
          Status.UNAUTHENTICATED
              .withDescription("Unexpected error trying to authenticate")
              .withCause(e)
      );
    }
    synchronized (lock) {
      // Update the token only if the new token is good or the old token is bad
      if (newToken.isValid() || !headerCache.isValid()) {
        headerCache = newToken;
      } else {
        LOG.warn("Failed to refresh the access token. Falling back to existing token. "
            + "New token state: {}, status: {}", newToken.getCacheState(), newToken.status);
      }
      futureToken = null;

      return headerCache;
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

  @VisibleForTesting
  boolean isRefreshing() {
    synchronized(lock) {
      return futureToken != null;
    }
  }
}
