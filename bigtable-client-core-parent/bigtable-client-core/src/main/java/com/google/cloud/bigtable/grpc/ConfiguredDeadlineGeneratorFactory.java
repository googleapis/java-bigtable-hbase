/*
 * Copyright 2020 Google LLC
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

import static com.google.cloud.bigtable.grpc.DeadlineGenerator.UNARY_DEADLINE_MINUTES;

import com.google.api.core.InternalApi;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.common.base.Optional;
import io.grpc.CallOptions;
import io.grpc.Context;
import io.grpc.Deadline;
import java.util.concurrent.TimeUnit;

/**
 * A {@link DeadlineGeneratorFactory} instance based on the configurations from {@link
 * CallOptionsConfig}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ConfiguredDeadlineGeneratorFactory implements DeadlineGeneratorFactory {
  private final CallOptionsConfig config;

  public ConfiguredDeadlineGeneratorFactory(CallOptionsConfig config) {
    this.config = config;
  }

  @Override
  public <RequestT> DeadlineGenerator getRequestDeadlineGenerator(
      RequestT request, boolean retriable) {
    if (isStreamingRead(request)) {
      return new DeadlineGeneratorImpl(
          config.isUseTimeout(),
          config.getReadStreamRpcTimeoutMs(),
          config.getReadStreamRpcAttemptTimeoutMs(),
          retriable,
          // Streaming reads do not need to specify a fallback deadline, since the Watchdog will
          // take affect and ensure that unresponsiveness does not occur. See
          // com.google.cloud.bigtable.grpc.io.Watchdog, which handles unresponsiveness for
          // streaming reads.
          Optional.<Long>absent());
    } else if (request instanceof MutateRowsRequest) {
      return new DeadlineGeneratorImpl(
          config.isUseTimeout(),
          config.getMutateRpcTimeoutMs(),
          config.getMutateRpcAttemptTimeoutMs(),
          retriable,
          // Calls should fail after 6 minutes, if there isn't any response from the server.
          Optional.of(TimeUnit.MINUTES.toMillis(UNARY_DEADLINE_MINUTES)));
    } else {
      return new DeadlineGeneratorImpl(
          config.isUseTimeout(),
          config.getShortRpcTimeoutMs(),
          config.getShortRpcAttemptTimeoutMs(),
          retriable,
          // Calls should fail after 6 minutes, if there isn't any response from the server.
          Optional.of(TimeUnit.MINUTES.toMillis(UNARY_DEADLINE_MINUTES)));
    }
  }

  private static <RequestT> boolean isStreamingRead(RequestT request) {
    return request instanceof ReadRowsRequest && !isGet((ReadRowsRequest) request);
  }

  private static boolean isGet(ReadRowsRequest request) {
    RowSet rowSet = request.getRows();
    return rowSet != null && rowSet.getRowRangesCount() == 0 && rowSet.getRowKeysCount() == 1;
  }

  /**
   * Provide a {@link CallOptions} object to be used in a single RPC attempt, where an RPC operation
   * may have multiple RPC attempts. {@link CallOptions} can contain state, specifically start time
   * with an expiration is set; in cases when timeouts are used, implementations should create a new
   * CallOptions each time this method is called.
   */
  private static class DeadlineGeneratorImpl implements DeadlineGenerator {
    private final boolean useTimeout;
    private final int requestTimeoutMs;
    private final Optional<Integer> requestAttemptTimeoutMs;
    /** A fail safe RPC deadline to make sure that operations don't stop responding. */
    private final Optional<Long> nonTimeoutFallbackDeadlineMs;

    private final Optional<Deadline> operationDeadline;

    DeadlineGeneratorImpl(
        boolean useTimeout,
        int requestTimeoutMs,
        Optional<Integer> requestAttemptTimeoutMs,
        boolean retriable,
        Optional<Long> nonTimeoutFallbackDeadlineMs) {
      this.useTimeout = useTimeout;
      this.requestTimeoutMs = requestTimeoutMs;
      this.requestAttemptTimeoutMs =
          retriable ? requestAttemptTimeoutMs : Optional.<Integer>absent();
      this.nonTimeoutFallbackDeadlineMs = nonTimeoutFallbackDeadlineMs;
      // Note: operationDeadline is always non-null if useTimeout == true
      this.operationDeadline = createOperationDeadline();
    }

    /**
     * Creates a {@link Deadline} for the operation, decided in the following order:
     *
     * <ol>
     *   <li>If a user set a {@link Context} deadline (see {@link Deadline}), use that
     *   <li>If a user configured deadlines via {@link CallOptionsConfig}, use it.
     *   <li>Otherwise, no deadline is specified.
     * </ol>
     */
    private Optional<Deadline> createOperationDeadline() {
      Optional<Deadline> deadline = Optional.fromNullable(Context.current().getDeadline());
      if (useTimeout) {
        deadline =
            deadline.or(Optional.of(Deadline.after(requestTimeoutMs, TimeUnit.MILLISECONDS)));
      }
      return deadline;
    }

    @Override
    public Optional<Long> getOperationTimeoutMs() {
      return operationDeadline.isPresent()
          ? Optional.of(operationDeadline.get().timeRemaining(TimeUnit.MILLISECONDS))
          : Optional.<Long>absent();
    }

    @Override
    public Optional<Deadline> getRpcAttemptDeadline() {
      // Check for RPC Attempt Timeouts first for backward-compatibility, so that
      // the existing logic (based mostly on the operation timeout) is preserved.
      if (useTimeout && requestAttemptTimeoutMs.isPresent()) {
        Deadline attemptDeadline =
            Deadline.after(requestAttemptTimeoutMs.get(), TimeUnit.MILLISECONDS);
        // Ensure we don't exceed the absolute operation deadline by taking the min of the attempt
        // and operation deadlines.
        if (operationDeadline.isPresent()) {
          return Optional.of(attemptDeadline.minimum(operationDeadline.get()));
        } else {
          // This branch should never be exercised, since if useTimeout == true, then we've set the
          // operationDeadline in the constructor. To avoid any compile-time syntax questions and to
          // add documentation, we keep this code.
          return Optional.of(attemptDeadline);
        }
      } else if (operationDeadline.isPresent()) {
        // If the user set a deadline, honor it.
        return operationDeadline;
      } else if (nonTimeoutFallbackDeadlineMs.isPresent()) {
        return Optional.of(
            Deadline.after(nonTimeoutFallbackDeadlineMs.get(), TimeUnit.MILLISECONDS));
      } else {
        return Optional.absent();
      }
    }
  }
}
