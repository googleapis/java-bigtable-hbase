/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
import io.opencensus.trace.Tracing;
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
 * This class caches calls to {@link OAuth2Credentials#refreshAccessToken()}.  It asynchronously refreshes
 * the token when it becomes stale.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class OAuthCredentialsCache {

  private static final Logger LOG = new Logger(OAuthCredentialsCache.class);
  private static final HeaderCacheElement EMPTY_HEADER = new HeaderCacheElement(null, 0);

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

  // TODO: this should split into cache type operations and a token used by RefreshingOAuth2CredentialsInterceptor

  static class HeaderToken {
    private final Status status;
    private final String header;

    public HeaderToken(Status status, String header) {
      this.status = status;
      this.header = header;
    }

    Status getStatus() {
      return status;
    }

    String getHeader() {
      return header;
    }
  }

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

    private final HeaderToken token;
    private final long actualExpirationTimeMs;

    private HeaderCacheElement(AccessToken token) {
      if (token.getExpirationTime() == null) {
        actualExpirationTimeMs = Long.MAX_VALUE;
      } else {
        actualExpirationTimeMs = token.getExpirationTime().getTime();
      }
      this.token = new HeaderToken(Status.OK, "Bearer " + token.getTokenValue());
    }

    private HeaderCacheElement(String header, long actualExpirationTimeMs) {
      this.token = new HeaderToken(Status.OK, header);
      this.actualExpirationTimeMs = actualExpirationTimeMs;
    }

    private HeaderCacheElement(Status errorStatus) {
      Preconditions.checkArgument(!errorStatus.isOk(), "Error status can't be OK");
      this.token = new HeaderToken(errorStatus, null);
      this.actualExpirationTimeMs = 0;
    }

    CacheState getCacheState() {
      long now = clock.currentTimeMillis();

      if (!token.status.isOk()) {
        return CacheState.Exception;
      } else if (actualExpirationTimeMs - TOKEN_EXPIRES_MS <= now) {
        return CacheState.Expired;
      } else if (actualExpirationTimeMs - TOKEN_STALENESS_MS <= now) {
        return CacheState.Stale;
      } else {
        return CacheState.Good;
      }
    }

    private boolean isValid() {
      return getCacheState().isValid();
    }

    HeaderToken getToken() {
      return token;
    }
  }

  private final ExecutorService executor;
  private final OAuth2Credentials credentials;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private Future<HeaderCacheElement> futureToken = null;

  // Note that the cache is volatile to allow us to peek for a Good value
  @GuardedBy("lock")
  private volatile HeaderCacheElement headerCache = EMPTY_HEADER;

  /**
   * <p>Constructor for RefreshingOAuth2CredentialsInterceptor.</p>
   *
   * @param scheduler a {@link ExecutorService} object.
   * @param credentials a {@link OAuth2Credentials} object.
   */
  public OAuthCredentialsCache(ExecutorService scheduler, OAuth2Credentials credentials) {
    this.executor = Preconditions.checkNotNull(scheduler);
    this.credentials = Preconditions.checkNotNull(credentials);
  }

  @VisibleForTesting
  HeaderCacheElement getHeaderCache() {
    return headerCache;
  }

  HeaderToken getHeader(int timeoutSeconds) {
    try {
      return getHeaderUnsafe(timeoutSeconds).getToken();
    } catch (Exception e) {
      LOG.warn("Got an unexpected exception while trying to refresh google credentials.", e);
      Status status = Status.UNAUTHENTICATED
              .withDescription("Unexpected failure get auth token")
              .withCause(e);
      return new HeaderToken(status, null);
    }
  }

  /**
   * Get the http credential header we need from a new oauth2 AccessToken.
   * @param timeoutSeconds
   */
  private HeaderCacheElement getHeaderUnsafe(int timeoutSeconds) {
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
        return syncRefresh(timeoutSeconds);
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
  private HeaderCacheElement syncRefresh(int timeoutSeconds) {
    try (Closeable ss = Tracing.getTracer().spanBuilder("CredentialsRefresh").startScopedSpan()) {
      return asyncRefresh().get(timeoutSeconds, TimeUnit.SECONDS);
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
   * there isn't a currently running asynchronous refresh and the current token is not "Good".
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

    /**
     * Create a new token via {@link OAuth2Credentials#refreshAccessToken()}, and then update the cache.
     * @return the new token
     */
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
            + "New token state: {}, status: {}", newToken.getCacheState(), newToken.getToken().status);
      }
      futureToken = null;

      return headerCache;
    }
  }

    /**
     * Clear the cache.
     *
     * @param oldToken for a comparison.  Only revoke the cache if the oldToken matches the current one.
     */
  void revokeUnauthToken(HeaderToken oldToken) {
    synchronized (lock) {
      if (headerCache.getToken() == oldToken) {
        LOG.warn("Got unauthenticated response from server, revoking the current token");
        headerCache = EMPTY_HEADER;
      } else {
        LOG.info("Skipping revoke, since the revoked token has already changed");
      }
    }
  }

  @VisibleForTesting
  boolean isRefreshing() {
    synchronized(lock) {
      return futureToken != null;
    }
  }
}
