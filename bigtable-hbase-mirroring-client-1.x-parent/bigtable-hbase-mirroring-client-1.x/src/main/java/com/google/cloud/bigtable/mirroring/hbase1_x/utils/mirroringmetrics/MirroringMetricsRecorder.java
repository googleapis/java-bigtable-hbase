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

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.OPERATION_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.READ_MISMATCHES;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.WRITE_MISMATCHES;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.MeasureMap;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagContextBuilder;
import io.opencensus.tags.Tagger;

@InternalApi("For internal usage only")
public class MirroringMetricsRecorder {
  private final Tagger tagger;
  private final StatsRecorder statsRecorder;

  public MirroringMetricsRecorder(Tagger tagger, StatsRecorder statsRecorder) {
    this.tagger = tagger;
    this.statsRecorder = statsRecorder;
  }

  public void recordOperation(
      HBaseOperation operation,
      MeasureLong latencyMeasure,
      long latencyMs,
      MeasureLong errorMeasure,
      boolean failed) {
    TagContext tagContext = getTagContext(operation);

    MeasureMap map = statsRecorder.newMeasureMap();
    map.put(latencyMeasure, latencyMs);
    if (failed) {
      map.put(errorMeasure, 1);
    }
    map.record(tagContext);
  }

  private TagContext getTagContext(HBaseOperation operation) {
    TagContextBuilder builder = tagger.emptyBuilder();
    builder.putLocal(OPERATION_KEY, operation.getTagValue());
    return builder.build();
  }

  public void recordOperation(
      HBaseOperation operation, MeasureLong latencyMeasure, long latencyMs) {
    recordOperation(operation, latencyMeasure, latencyMs, null, false);
  }

  public void recordReadMismatches(HBaseOperation operation, int numberOfMismatches) {
    TagContext tagContext = getTagContext(operation);
    MeasureMap map = statsRecorder.newMeasureMap();
    map.put(READ_MISMATCHES, numberOfMismatches);
    map.record(tagContext);
  }

  public void recordWriteMismatches(HBaseOperation operation, int numberOfMismatches) {
    TagContext tagContext = getTagContext(operation);
    MeasureMap map = statsRecorder.newMeasureMap();
    map.put(WRITE_MISMATCHES, numberOfMismatches);
    map.record(tagContext);
  }
}
