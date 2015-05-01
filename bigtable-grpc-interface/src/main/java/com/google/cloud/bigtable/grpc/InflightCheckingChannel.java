/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import io.grpc.Call;
import io.grpc.ForwardingCallListener;
import io.grpc.Metadata;
import io.grpc.Metadata.Headers;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

/**
 * <p>
 * A ClosableChannel that doesn't call close until all rpcs are complete
 * </p>
 * <p>
 * TODO: This class was introduced because of a temporary issue in gRPC. It should be handling
 * .shutdown() calls gracefully, but isn't due to a bug. Once they fix the bug, remove TrackingCall
 * and CountingChannel. Also, uncomment ReconnectingChannelTest.
 * </p>
 * <p>
 * NOTE: if InflightCheckingChannel.start() or .canceled() are not called on a Call, this Channel
 * can hang forever during .close().
 * </p>
 */
public class InflightCheckingChannel implements CloseableChannel {

  @VisibleForTesting
  class TrackingCall<RequestT, ResponseT> extends Call<RequestT, ResponseT> {

    private Call<RequestT, ResponseT> delegateCall;
    private long id;

    protected TrackingCall(Call<RequestT, ResponseT> delegateCall) {
      this.delegateCall = delegateCall;
      id = counter.incrementAndGet();
      outstandingRequests.add(id);
    }

    @Override
    public void start(Call.Listener<ResponseT> responseListener, Headers headers) {
      Call.Listener<ResponseT> wrapped =
          new ForwardingCallListener.SimpleForwardingCallListener<ResponseT>(responseListener) {
            @Override
            public void onClose(Status status, Metadata.Trailers trailers) {
              super.onClose(status, trailers);
              untrack();
            }
          };
      delegateCall.start(wrapped, headers);
    }

    @Override
    public void cancel() {
      delegateCall.cancel();
      untrack();
    }

    @VisibleForTesting
    void untrack() {
      outstandingRequests.remove(id);
      if (outstandingRequests.isEmpty()) {
        synchronized (outstandingRequests) {
          outstandingRequests.notify();
        }
      }
    }

    public Long getId() {
      return id;
    }

    @Override
    public void request(int numMessages) {
      delegateCall.request(numMessages);
    }

    @Override
    public void halfClose() {
      delegateCall.halfClose();
    }

    @Override
    public void sendPayload(RequestT payload) {
      delegateCall.sendPayload(payload);
    }
  }

  // nextRefresh and delegate need to be protected by delegateLock.
  private final CloseableChannel delegateChannel;
  private boolean closed = false;

  @VisibleForTesting
  final Set<Long> outstandingRequests = Collections.synchronizedSet(new HashSet<Long>());
  private final AtomicLong counter = new AtomicLong();

  public InflightCheckingChannel(CloseableChannel channel) {
    this.delegateChannel = channel;
  }

  @Override
  public <RequestT, ResponseT> Call<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor) {
    if (closed) {
      throw new IllegalStateException("Cannot create a new call after .close() is called");
    }
    return new TrackingCall<>(delegateChannel.newCall(methodDescriptor));
  }

  @Override
  public void close() throws IOException {
    closed = true;
    synchronized (outstandingRequests) {
      try {
        while (!canClose()) {
          outstandingRequests.wait();
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
      delegateChannel.close();
    }
  }

  boolean canClose() {
    return outstandingRequests.isEmpty();
  }
}
