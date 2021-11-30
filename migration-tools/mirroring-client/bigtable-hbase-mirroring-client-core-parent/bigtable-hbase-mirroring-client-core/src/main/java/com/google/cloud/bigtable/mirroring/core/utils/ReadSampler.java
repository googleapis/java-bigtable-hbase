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
package com.google.cloud.bigtable.mirroring.core.utils;

import java.util.concurrent.ThreadLocalRandom;

public class ReadSampler {
  private final int sampleProbabilityPercent;

  public ReadSampler(int sampleProbabilityPercent) {
    if (sampleProbabilityPercent < 0 || sampleProbabilityPercent > 100) {
      throw new IllegalArgumentException("Invalid verification interval");
    }
    this.sampleProbabilityPercent = sampleProbabilityPercent;
  }

  /** Should next read operation be sampled. */
  public boolean shouldNextReadOperationBeSampled() {
    return ThreadLocalRandom.current().nextInt(100) < this.sampleProbabilityPercent;
  }
}
