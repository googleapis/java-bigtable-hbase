/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflow;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;

public class AggregatorWithState {

  private long state;
  private final Aggregator<Long, Long> aggregator;

  public AggregatorWithState(Aggregator<Long, Long> aggregator) {
    super();
    this.aggregator = aggregator;
  }

  public synchronized void set(long value) {
    long diff = value - state;
    aggregator.addValue(diff);
    state = value;
  }
}
