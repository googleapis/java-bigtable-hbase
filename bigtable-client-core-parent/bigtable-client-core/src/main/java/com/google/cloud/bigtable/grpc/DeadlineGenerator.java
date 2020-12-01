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

import com.google.common.base.Optional;
import io.grpc.CallOptions;
import io.grpc.Context;
import io.grpc.Deadline;

/**
 * A provider for RPC timeouts and deadlines for a given Bigtable client operation, governing both
 * the full operation and retry attempts within an operation. A single {@link DeadlineGenerator}
 * object should be used for a client operation instance and reused across retry attempts.
 */
public interface DeadlineGenerator {
  /**
   * The server-side has a 5 minute timeout. Unary operations should be timed-out on the client side
   * after 6 minutes.
   */
  long UNARY_DEADLINE_MINUTES = 6L;

  /**
   * The default instance will return: - {@link CallOptions#DEFAULT} with any {@link
   * Context#current()}'s {@link Context#getDeadline()} applied to it. - The operation timeout of 6
   * minutes.
   *
   * <p>For internal use only - public for technical reasons.
   */
  DeadlineGenerator DEFAULT =
      new DeadlineGenerator() {
        @Override
        public Optional<Long> getOperationTimeoutMs() {
          return Optional.absent();
        }

        @Override
        public Optional<Deadline> getRpcAttemptDeadline() {
          return Optional.fromNullable(Context.current().getDeadline());
        }
      };

  /**
   * Returns the operation timeout (if configured) in millisecond to use for this RPC, covering all
   * retry attempts. Single attempt timeouts are governed by {@link #getRpcAttemptDeadline()} ()}.
   */
  Optional<Long> getOperationTimeoutMs();

  /**
   * Provides a {@link Deadline} object to be used in a single RPC attempt. {@link Deadline} can
   * contain state, specifically start time with an expiration is set; in cases when timeouts are
   * used, implementations should invoke this method each time this method is called.
   */
  Optional<Deadline> getRpcAttemptDeadline();
}
