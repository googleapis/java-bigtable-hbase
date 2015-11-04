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
package com.google.cloud.bigtable.config;

import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_BACKOFF_MULTIPLIER;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_INITIAL_BACKOFF_MILLIS;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_STREAMING_BUFFER_SIZE;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_STREAMING_BATCH_SIZE;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.NanoClock;

/**
 * Testing utility for creating RetryOptions
 */
public class RetryOptionsUtil {
  public static RetryOptions createTestRetryOptions(final NanoClock nanoClock) {
    return new RetryOptions(true, true, DEFAULT_INITIAL_BACKOFF_MILLIS, DEFAULT_BACKOFF_MULTIPLIER,
        DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS, DEFAULT_STREAMING_BUFFER_SIZE,
        DEFAULT_STREAMING_BATCH_SIZE, DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS) {
      @Override
      protected ExponentialBackOff.Builder createBackoffBuilder() {
        return super.createBackoffBuilder().setNanoClock(nanoClock);
      }
    };
  }
}
