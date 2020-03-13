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
package com.google.cloud.bigtable.hbase.util;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.HConstants;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class TimestampConverter {
  // The difference between milliseconds and microseconds is a factor of 1000
  private static final long FACTOR = 1000l;

  /**
   * Maximum timestamp (in usecs) that bigtable can handle while preserving lossless conversion to
   * hbase timestamps in ms)
   */
  @VisibleForTesting
  static final long BIGTABLE_MAX_TIMESTAMP = Long.MAX_VALUE - (Long.MAX_VALUE % FACTOR);

  /**
   * Maximum timestamp that hbase can send to bigtable in ms. This limitation exists because
   * bigtable operates on usecs, while hbase operates on ms.
   */
  @VisibleForTesting
  static final long HBASE_EFFECTIVE_MAX_TIMESTAMP = BIGTABLE_MAX_TIMESTAMP / FACTOR;

  public static long hbase2bigtable(long timestamp) {
    if (timestamp < HBASE_EFFECTIVE_MAX_TIMESTAMP) {
      return timestamp * FACTOR;
    } else {
      return BIGTABLE_MAX_TIMESTAMP;
    }
  }

  public static long bigtable2hbase(long timestamp) {
    if (timestamp >= BIGTABLE_MAX_TIMESTAMP) {
      return HConstants.LATEST_TIMESTAMP;
    } else {
      return timestamp / FACTOR;
    }
  }
}
