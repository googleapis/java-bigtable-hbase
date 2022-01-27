/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;

/**
 * {@code System#currentTimeMillis()} is not monotonic and using it as a source for {@link
 * org.apache.hadoop.hbase.client.Mutation} timestamps can result in confusion and unexpected
 * reordering of written versions.
 *
 * <p>This class provides a monotonically increasing value that is related to wall time.
 *
 * <p>Guava's {@link Stopwatch} is monotonic because it uses {@link System#nanoTime()} to measure
 * passed time.
 */
public class MonotonicTimer {
  private final long startingTimestampMillis;
  private final Stopwatch stopwatch;

  public MonotonicTimer() {
    this.startingTimestampMillis = System.currentTimeMillis();
    this.stopwatch = Stopwatch.createStarted();
  }

  public long getCurrentTimeMillis() {
    return this.startingTimestampMillis + this.stopwatch.elapsed(TimeUnit.MILLISECONDS);
  }
}
