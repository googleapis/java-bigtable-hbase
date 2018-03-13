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

import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.io.OAuthCredentialsStore.HeaderCacheElement;
import com.google.common.annotations.VisibleForTesting;
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

import java.util.concurrent.ExecutorService;

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

  public static int TIMEOUT_SECONDS = 15;

  @VisibleForTesting
  static final Metadata.Key<String> AUTHORIZATION_HEADER_KEY = Metadata.Key.of(
      "Authorization", Metadata.ASCII_STRING_MARSHALLER);

  private final OAuthCredentialsStore store;
  private final RateLimiter rateLimiter;

  private class UnAuthResponseListener<RespT> extends SimpleForwardingClientCallListener<RespT> {

    private final HeaderCacheElement origToken;

    private UnAuthResponseListener(Listener<RespT> delegate, HeaderCacheElement origToken) {
      super(delegate);
      this.origToken = origToken;
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
      if (status == Status.UNAUTHENTICATED) {
        if (rateLimiter.tryAcquire()) {
          store.revokeUnauthToken(origToken);
        } else {
          LOG.trace("UnAuthResponseListener rate limited");
        }
      }
      super.onClose(status, trailers);
    }
  }

  /**
   * <p>Constructor for RefreshingOAuth2CredentialsInterceptor.</p>
   *
   * @param scheduler a {@link ExecutorService} object.
   * @param credentials a {@link OAuth2Credentials} object.
   */
  public RefreshingOAuth2CredentialsInterceptor(ExecutorService scheduler, OAuth2Credentials credentials) {
    this(new OAuthCredentialsStore(scheduler, credentials));
  }

  @VisibleForTesting
  RefreshingOAuth2CredentialsInterceptor(OAuthCredentialsStore store) {
    this.store = store;

    // Revoke credentials no more than every 10 seconds fo UNAUTHENTICATED statuses
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
        HeaderCacheElement headerCache = store.getHeader(TIMEOUT_SECONDS);

        if (!headerCache.getStatus().isOk()) {
          unauthorized = true;
          responseListener.onClose(headerCache.getStatus(), new Metadata());
          return;
        }

        headers.put(AUTHORIZATION_HEADER_KEY, headerCache.getHeader());

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

  /**
   * Refreshes the OAuth2 token asynchronously. This method will only start an async refresh if
   * there isn't a currently running asynchronous refresh.
   */
  void asyncRefresh() {
    store.asyncRefresh();
  }
}
