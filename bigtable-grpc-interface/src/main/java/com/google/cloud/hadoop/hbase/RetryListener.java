package com.google.cloud.hadoop.hbase;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import io.grpc.Call;
import io.grpc.Metadata;
import io.grpc.Status;

import java.util.Set;

/**
 * A Call.Listener that listens for lower-level Call failures and if detected, attempts
 * to retry the call if the call supports retrying.
 */
class RetryListener<RequestT, ResponseT> extends Call.Listener<ResponseT> {

  public static final Set<Status.Code> RETRIABLE_STATUSES =
      ImmutableSet.of(Status.Code.INTERNAL, Status.Code.UNAVAILABLE);

  private final RetryingCall<RequestT, ResponseT> retryingCall;
  private final RequestT payload;
  private final Metadata.Headers requestHeaders;
  private final boolean isRetriableCall;
  private final Call.Listener<ResponseT> delegate;
  private boolean stateSignalledToListener = false;

  public RetryListener(
      RetryingCall<RequestT, ResponseT> call,
      RequestT payload,
      Metadata.Headers requestHeaders,
      boolean isRetriableCall,
      Call.Listener<ResponseT> delegate) {
    this.retryingCall = call;
    this.delegate = delegate;
    this.payload = payload;
    this.requestHeaders = requestHeaders;
    this.isRetriableCall = isRetriableCall;
  }

  @Override
  public void onPayload(ResponseT payload) {
    stateSignalledToListener = true;
    delegate.onPayload(payload);
  }

  @Override
  public void onHeaders(Metadata.Headers headers) {
    stateSignalledToListener = true;
    delegate.onHeaders(headers);
  }

  @Override
  public void onClose(Status status, Metadata.Trailers trailers) {
    if (isRetriableStatus(status.getCode())
        && isRetriableCall
        && !stateSignalledToListener) {
      if (retryingCall.retryCallAfterBackoff(payload, requestHeaders, this)) {
        // We are retrying
        return;
      }
    }
    delegate.onClose(status, trailers);
  }

  @VisibleForTesting
  static boolean isRetriableStatus(Status.Code code) {
    return RETRIABLE_STATUSES.contains(code);
  }
}
