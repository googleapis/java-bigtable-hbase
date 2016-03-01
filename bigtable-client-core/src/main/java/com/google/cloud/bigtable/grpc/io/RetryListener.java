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

import com.google.cloud.bigtable.config.Logger;
import com.google.common.annotations.VisibleForTesting;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;

/**
 * A ClientCall.Listener that listens for lower-level ClientCall failures and if detected, attempts
 * to retry the call if the call supports retrying.
 */
class RetryListener<RequestT, ResponseT> extends ClientCall.Listener<ResponseT> {
  private static final Logger LOG = new Logger(RetryListener.class);

  private final RetryingCall<RequestT, ResponseT> retryingCall;
  private final RequestT payload;
  private final Metadata requestHeaders;
  private final ClientCall.Listener<ResponseT> delegate;
  private boolean stateSignalledToListener = false;
  private int failedCount = 0;

  public RetryListener(
      RetryingCall<RequestT, ResponseT> call,
      RequestT payload,
      Metadata requestHeaders,
      ClientCall.Listener<ResponseT> delegate) {
    this.retryingCall = call;
    this.delegate = delegate;
    this.payload = payload;
    this.requestHeaders = requestHeaders;
  }

  @Override
  public void onMessage(ResponseT message) {
    stateSignalledToListener = true;
    delegate.onMessage(message);
  }

  @Override
  public void onHeaders(Metadata headers) {
    stateSignalledToListener = true;
    delegate.onHeaders(headers);
  }

  @Override
  public void onClose(Status status, Metadata trailers) {
    if (isRetriableStatus(status.getCode())
        && !stateSignalledToListener) {
      if (retryingCall.retryCallAfterBackoff(payload, requestHeaders, this)) {
        // A retryable error.
        failedCount += 1;
        LOG.info("Retrying failed call. Failure #" + failedCount + ", got: " + status, status.getCause());
        return;
      }
    }
    delegate.onClose(status, trailers);
  }

  @VisibleForTesting
  static boolean isRetriableStatus(Status.Code code) {
    switch (code) {
      case INTERNAL:
      case UNAVAILABLE:
        return true;
      default:
        return false;
    }
  }
}
