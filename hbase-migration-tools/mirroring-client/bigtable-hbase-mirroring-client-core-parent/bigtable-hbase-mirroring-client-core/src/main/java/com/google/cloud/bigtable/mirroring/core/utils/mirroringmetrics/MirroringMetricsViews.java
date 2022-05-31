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
package com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics;

import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.FLOW_CONTROL_LATENCY;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.MIRRORING_LATENCY;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.OPERATION_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.PRIMARY_ERRORS;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.PRIMARY_LATENCY;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.READ_MATCHES;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.READ_MISMATCHES;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_ERRORS;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_LATENCY;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_WRITE_ERRORS;
import static com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_WRITE_ERROR_HANDLER_LATENCY;

import com.google.api.core.InternalApi;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Aggregation.Count;
import io.opencensus.stats.Aggregation.Distribution;
import io.opencensus.stats.Aggregation.Sum;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Stats;
import io.opencensus.stats.View;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagKey;
import java.util.ArrayList;

@InternalApi("For internal usage only")
public class MirroringMetricsViews {

  private static final Aggregation COUNT = Count.create();

  private static final Aggregation SUM = Sum.create();

  // Bucket boundaries copied from java-bigtable's RpcViewConstants.
  private static final Aggregation AGGREGATION_WITH_MILLIS_HISTOGRAM =
      Distribution.create(
          BucketBoundaries.create(
              ImmutableList.of(
                  0.0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0,
                  13.0, 16.0, 20.0, 25.0, 30.0, 40.0, 50.0, 65.0, 80.0, 100.0, 130.0, 160.0, 200.0,
                  250.0, 300.0, 400.0, 500.0, 650.0, 800.0, 1000.0, 2000.0, 5000.0, 10000.0,
                  20000.0, 50000.0, 100000.0)));

  /** {@link View} for Mirroring client's primary database operations latency. */
  private static final View PRIMARY_OPERATION_LATENCY_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/primary_op_latency"),
          "Primary database operation latency in milliseconds.",
          PRIMARY_LATENCY,
          AGGREGATION_WITH_MILLIS_HISTOGRAM,
          ImmutableList.of(OPERATION_KEY));

  /** {@link View} for Mirroring client's primary database operations errors. */
  private static final View PRIMARY_OPERATION_ERROR_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/primary_op_errors"),
          "Primary database operation error count.",
          PRIMARY_ERRORS,
          COUNT,
          ImmutableList.of(OPERATION_KEY));

  /** {@link View} for Mirroring client's secondary database operations latency. */
  private static final View SECONDARY_OPERATION_LATENCY_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/secondary_op_latency"),
          "Secondary database operation latency in milliseconds.",
          SECONDARY_LATENCY,
          AGGREGATION_WITH_MILLIS_HISTOGRAM,
          ImmutableList.of(OPERATION_KEY));

  /** {@link View} for Mirroring client's secondary database operations errors. */
  private static final View SECONDARY_OPERATION_ERROR_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/secondary_op_errors"),
          "Secondary database operation error count.",
          SECONDARY_ERRORS,
          COUNT,
          ImmutableList.of(OPERATION_KEY));

  /** {@link View} for Mirroring client's mirroring operations latency. */
  private static final View MIRRORING_OPERATION_LATENCY_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/mirroring_op_latency"),
          "Mirroring operation latency in milliseconds.",
          MIRRORING_LATENCY,
          AGGREGATION_WITH_MILLIS_HISTOGRAM,
          ImmutableList.of(OPERATION_KEY));

  /** {@link View} for Mirroring client's secondary read mismatches. */
  private static final View READ_MISMATCH_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/read_mismatch"),
          "Detected read mismatches count.",
          READ_MISMATCHES,
          SUM,
          ImmutableList.of(OPERATION_KEY));

  /** {@link View} for Mirroring client's secondary database write errors. */
  private static final View SECONDARY_WRITE_ERROR_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/secondary_write_error"),
          "Secondary database write error count.",
          SECONDARY_WRITE_ERRORS,
          SUM,
          ImmutableList.of(OPERATION_KEY));

  /** {@link View} for Mirroring client's secondary read mismatches. */
  private static final View READ_MATCH_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/read_match"),
          "Detected read matches count.",
          READ_MATCHES,
          SUM,
          ImmutableList.of(OPERATION_KEY));

  /** {@link View} for Mirroring client's secondary write error handling latency. */
  private static final View FLOW_CONTROL_LATENCY_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/flow_control_latency"),
          "Distribution of latency of acquiring flow controller resources.",
          FLOW_CONTROL_LATENCY,
          AGGREGATION_WITH_MILLIS_HISTOGRAM,
          new ArrayList<TagKey>());

  /** {@link View} for Mirroring client's secondary write error handling latency. */
  private static final View SECONDARY_WRITE_ERROR_HANDLER_LATENCY_VIEW =
      View.create(
          View.Name.create("cloud.google.com/java/mirroring/secondary_write_error_handler_latency"),
          "Distribution of secondary write error handling latency.",
          SECONDARY_WRITE_ERROR_HANDLER_LATENCY,
          AGGREGATION_WITH_MILLIS_HISTOGRAM,
          new ArrayList<TagKey>());

  // TODO: Add a new view "Mirroring operation failed" that tells you if a high level mirroring
  //  operation failed. It could fail due to a primary or secondary failure (for say concurrent
  //  writes).

  private static final ImmutableSet<View> MIRRORING_CLIENT_VIEWS_SET =
      ImmutableSet.of(
          PRIMARY_OPERATION_LATENCY_VIEW,
          PRIMARY_OPERATION_ERROR_VIEW,
          SECONDARY_OPERATION_LATENCY_VIEW,
          SECONDARY_OPERATION_ERROR_VIEW,
          MIRRORING_OPERATION_LATENCY_VIEW,
          READ_MATCH_VIEW,
          READ_MISMATCH_VIEW,
          SECONDARY_WRITE_ERROR_VIEW,
          FLOW_CONTROL_LATENCY_VIEW,
          SECONDARY_WRITE_ERROR_HANDLER_LATENCY_VIEW);

  /** Registers all Mirroring client views to OpenCensus View. */
  public static void registerMirroringClientViews() {
    ViewManager viewManager = Stats.getViewManager();
    for (View view : MIRRORING_CLIENT_VIEWS_SET) {
      viewManager.registerView(view);
    }
  }
}
