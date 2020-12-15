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
package com.google.cloud.bigtable.grpc;

import com.google.api.core.InternalApi;

/**
 * A factory that creates {@link DeadlineGenerator} for use in {@link BigtableDataClient} RPCs.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface DeadlineGeneratorFactory {
  DeadlineGeneratorFactory DEFAULT =
      new DeadlineGeneratorFactory() {
        @Override
        public <RequestT> DeadlineGenerator getRequestDeadlineGenerator(
            RequestT request, boolean retriable) {
          return DeadlineGenerator.DEFAULT;
        }
      };

  /**
   * Returns a {@link DeadlineGenerator} instance to use for the given RPC operation. This should
   * only be called once per operation and be used for the duration of the operation.
   */
  <RequestT> DeadlineGenerator getRequestDeadlineGenerator(RequestT request, boolean retriable);
}
