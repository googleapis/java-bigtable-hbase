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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics;

import com.google.api.core.InternalApi;
import io.opencensus.stats.Stats;
import io.opencensus.tags.Tags;
import io.opencensus.trace.Tracing;

@InternalApi("For internal usage only")
public class MirroringTracer {
  public final MirroringSpanFactory spanFactory;
  public final MirroringMetricsRecorder metricsRecorder;

  public MirroringTracer(
      MirroringSpanFactory spanFactory, MirroringMetricsRecorder metricsRecorder) {
    this.spanFactory = spanFactory;
    this.metricsRecorder = metricsRecorder;
  }

  public MirroringTracer() {
    this.metricsRecorder = new MirroringMetricsRecorder(Tags.getTagger(), Stats.getStatsRecorder());
    this.spanFactory = new MirroringSpanFactory(Tracing.getTracer(), this.metricsRecorder);
  }
}
