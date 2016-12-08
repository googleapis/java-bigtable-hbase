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

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Clock;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Sleeper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.common.annotations.VisibleForTesting;

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
public class RefreshingOAuth2CredentialsInterceptor implements HeaderInterceptor {

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

  enum RetryState {
    PerformRetry, RetriesExhausted, Interrupted
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
    public static final int TOKEN_STALENESS_MS = 75 * 1000;

    /**
     * After the token is "expired," the interceptor blocks gRPC calls. The Expired state indicates
     * that the interceptor needs to do a synchronous refresh.
     */
    public static final int TOKEN_EXPIRES_MS = 15 * 1000;

    final IOException exception;
    final String header;

    /**
     * Defines the amount of time in ms when the header is considered "stale" and should be
     * refreshed in the near future. A {@code null} value means that the header does not become stale.
     */
    final @Nullable Long staleTimeMs;

    /**
     * Defines the amount of time in ms when the header is considered "expired" and must be
     * refreshed. A {@code null} value means that the header does not expire.
     */
    final @Nullable Long expiresTimeMs;

    public HeaderCacheElement(AccessToken token) {
      this.exception = null;
      this.header = "Bearer " + token.getTokenValue();
      Date expirationTime = token.getExpirationTime();
      if (expirationTime != null) {
        long tokenExpiresTime = expirationTime.getTime();
        this.staleTimeMs = tokenExpiresTime - TOKEN_STALENESS_MS;
        // Block until refresh at this point.
        this.expiresTimeMs = tokenExpiresTime - TOKEN_EXPIRES_MS;
        Preconditions.checkState(staleTimeMs < expiresTimeMs);
      } else {
        this.staleTimeMs = null;
        this.expiresTimeMs = null;
      }
    }

    public HeaderCacheElement(IOException exception) {
      this.exception = exception;
      this.header = null;
      this.staleTimeMs = null;
      this.expiresTimeMs = null;
    }

    public CacheState getCacheState() {
      if (exception != null) {
        return CacheState.Exception;
      }
      long now = clock.currentTimeMillis();
      if (staleTimeMs == null || now < staleTimeMs) {
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

  @VisibleForTesting
  Sleeper sleeper = Sleeper.DEFAULT;

  private final ExecutorService executor;
  private final RetryOptions retryOptions;
  private final Logger logger;
  private final CredentialOptions credentialOptions;
  private final boolean isAppEngine;

  private OAuth2Credentials credentials;

  /**
   * <p>Constructor for RefreshingOAuth2CredentialsInterceptor.</p>
   *
   * @param scheduler a {@link java.util.concurrent.ExecutorService} object.
   * @param credentials a {@link com.google.auth.oauth2.OAuth2Credentials} object.
   * @param credentialOptions a {@link com.google.cloud.bigtable.config.CredentialOptions} object.
   * @param retryOptions a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   */
  public RefreshingOAuth2CredentialsInterceptor(ExecutorService scheduler,
      OAuth2Credentials credentials, CredentialOptions credentialOptions,
      RetryOptions retryOptions) {
    this(scheduler, credentials, credentialOptions, retryOptions, LOG);
  }

  @VisibleForTesting
  RefreshingOAuth2CredentialsInterceptor(ExecutorService scheduler, OAuth2Credentials credentials,
      CredentialOptions credentialOptions, RetryOptions retryOptions, Logger logger) {
    this.executor = Preconditions.checkNotNull(scheduler);
    this.credentials = Preconditions.checkNotNull(credentials);
    this.credentialOptions = Preconditions.checkNotNull(credentialOptions);
    this.retryOptions = Preconditions.checkNotNull(retryOptions);
    this.logger = Preconditions.checkNotNull(logger);

    // com.google.auth.oauth2.AppEngineCredentials is package private, so we don't have direct
    // visibility to it. This is a way to detect that this application runs on AppEngine so that
    // we can always get credentials synchronously, which must be done for AppEngineCredentials
    // which relies on a ThreadLocal.
    this.isAppEngine = credentials.getClass().getName().contains("AppEngineCredentials");
  }

  /** {@inheritDoc} */
  @Override
  public void updateHeaders(Metadata headers) throws Exception {
    headers.put(AUTHORIZATION_HEADER_KEY, getHeader());
  }

  /**
   * Refreshes the OAuth2 token asynchronously. This method will only start an async refresh if
   * there isn't a currently running asynchronous refresh or the credentials are for AppEngine.
   * AppEngine has some credentials related state in a {@link ThreadLocal} which prevents it from
   * running asynchronously. In the AppEngine case, this method will defer to {@link #syncRefresh()}
   *
   * @throws IOException
   */
  public void asyncRefresh() throws IOException {
    if (isAppEngine) {
      syncRefresh();
    } else if (canRefresh()) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          doRefresh();
        }
      });
    }
  }

  private boolean canRefresh() {
    return !isRefreshing.get() && getCacheState(this.headerCache.get()) != CacheState.Good;
  }

  @VisibleForTesting
  boolean isRefreshing() {
    return isRefreshing.get();
  }

  /**
   * <p>syncRefresh.</p>
   *
   * @throws java.io.IOException if any.
   */
  public void syncRefresh() throws IOException {
    synchronized (isRefreshing) {
      if (!isRefreshing.get()) {
        doRefresh();
      } else {
        while (isRefreshing.get() && getCacheState(this.headerCache.get()) != CacheState.Good) {
          try {
            isRefreshing.wait(250);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
  String getHeader()  {
    HeaderCacheElement headerCache;
    try {
      headerCache = getCachedHeader();
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
        case Exception:
          asyncRefresh();
          throw asUnauthenticatedException(headerCache.exception);
        default:
          throw asUnauthenticatedException(
              new IllegalStateException("Could not process state: " + state));
      }
    } catch(IOException e) {
      throw asUnauthenticatedException(e);
    }
    return headerCache.header;
  }

  private static StatusRuntimeException asUnauthenticatedException(Exception e) {
    return Status.UNAUTHENTICATED.withCause(e).asRuntimeException();
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
    if (!canRefresh()) {
      return false;
    }
    synchronized (isRefreshing) {
      if (!canRefresh()) {
        return false;
      }
      isRefreshing.set(true);
    }
    try {
      HeaderCacheElement cacheElement = refreshCredentialsWithRetry();
      synchronized (isRefreshing) {
        headerCache.set(cacheElement);
      }
    } finally {
      synchronized (isRefreshing) {
        isRefreshing.set(false);
        isRefreshing.notifyAll();
      }
    }
    return true;
  }

  /**
   * Calls {@link com.google.auth.oauth2.OAuth2Credentials#refreshAccessToken()}. In case of an
   * IOException, retry the call as per the {@link com.google.api.client.util.BackOff} policy
   * defined by {@link com.google.cloud.bigtable.config.RetryOptions#createBackoff()}.
   *
   * <p>This method retries until one of the following conditions occurs:
   *
   * <ol>
   * <li>An OAuth request was completed. If the value is null, return an exception.
   * <li>A non-IOException Exception is thrown - return an error status
   * <li>All retries have been exhausted, i.e. when the Backoff.nextBackOffMillis() returns
   *     BackOff.STOP
   * <li>An interrupt occurs.
   * </ol>
   *
   * @return HeaderCacheElement containing either a valid {@link com.google.auth.oauth2.AccessToken}
   *     or an exception.
   */
  protected HeaderCacheElement refreshCredentialsWithRetry() {
    BackOff backoff = null;
    while (true) {
      try {
        logger.info("Refreshing the OAuth token");
        AccessToken newToken = credentials.refreshAccessToken();
        if (newToken == null) {
          // The current implementations of refreshAccessToken() throws an IOException or return
          // a valid token. This handling is future proofing just in case credentials returns null for
          // some reason. This case was caught by a poorly coded unit test.
          logger.info("Refreshed the OAuth token");
          return new HeaderCacheElement(new IOException("Could not load the token for credentials: "
              + credentials));
        } else {
          // Success!
          return new HeaderCacheElement(newToken);
        }
      } catch (IOException exception) {
        logger.warn("Got an unexpected IOException when refreshing google credentials.", exception);
        // An IOException occurred. Retry with backoff.
        if (backoff == null) {
          // initialize backoff.
          backoff = retryOptions.createBackoff();
        }
        // Given the backoff, either sleep for a short duration, or terminate if the backoff has
        // reached its configured timeout limit.
        try {
          RetryState retryState = getRetryState(backoff);
          if (retryState != RetryState.PerformRetry) {
            return new HeaderCacheElement(exception);
          } // else Retry.

          refreshCredentials();
        } catch (IOException e) {
          logger.warn("Got an exception while trying to run backoff.nextBackOffMillis()", e);
          return new HeaderCacheElement(exception);
        }
      } catch (Exception e) {
        logger.warn("Got an unexpected exception while trying to refresh google credentials.", e);
        return new HeaderCacheElement(new IOException("Could not read headers", e));
      }
    }
  }

  /**
   * Sleep and/or determine if the backoff has timed out.
   *
   * @param backoff a {@link com.google.api.client.util.BackOff} object.
   * @return RetryState indicating the current state of the retry logic.
   * @throws java.io.IOException in some cases from {@link
   *     com.google.api.client.util.BackOff#nextBackOffMillis()}
   */
  protected RetryState getRetryState(BackOff backoff) throws IOException {
    long nextBackOffMillis = backoff.nextBackOffMillis();
    if (nextBackOffMillis == BackOff.STOP) {
      logger.warn("Exhausted the number of retries for credentials refresh after "
          + this.retryOptions.getMaxElaspedBackoffMillis() + " milliseconds.");
      return RetryState.RetriesExhausted;
    }
    try {
      sleeper.sleep(nextBackOffMillis);
      // Try to perform another call.
      return RetryState.PerformRetry;
    } catch (InterruptedException e) {
      logger.warn("Interrupted while trying to refresh credentials.");
      Thread.currentThread().interrupt();
      // If the thread is interrupted, terminate immediately.
      return RetryState.Interrupted;
    }
  }

  /**
   * Call {@link CredentialFactory#clearHttpTransport()} and retrieve a new {@link OAuth2Credentials}.
   */
  @VisibleForTesting
  void refreshCredentials() {
    try {
      this.credentials = (OAuth2Credentials) CredentialFactory.getCredentials(credentialOptions);
    } catch (IOException | GeneralSecurityException e1) {
      logger.warn("Could not retrieve new credentials", e1);
    }
  }
}
