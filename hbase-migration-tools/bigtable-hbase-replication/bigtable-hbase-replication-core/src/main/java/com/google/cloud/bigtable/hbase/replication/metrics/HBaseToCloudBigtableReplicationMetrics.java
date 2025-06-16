/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.bigtable.hbase.replication.metrics;

/**
 * Metrics exported by the Cloud Bigtable replication endpoint. Please note that custom replication
 * metrics are only supported by HBase 1.4 and newer. Please see
 * https://github.com/googleapis/java-bigtable-hbase/issues/3596 for more details.
 */
public class HBaseToCloudBigtableReplicationMetrics {

  // Static class for listing all the metrics
  private HBaseToCloudBigtableReplicationMetrics() {}

  public static final String INCOMPATIBLE_MUTATION_METRIC_KEY = "bigtableIncompatibleMutations";
  public static final String DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY =
      "bigtableDroppedIncompatibleMutations";

  public static final String INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY =
      "bigtableIncompatibleDeleteMutations";
  public static final String INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY =
      "bigtableIncompatibleTimestampOverflowMutation";
  public static final String PUTS_IN_FUTURE_METRIC_KEY = "bigtablePutsInFutureMutations";

  /**
   * WAL entries/mutations exported if bidirectional replication is not enabled. Note that no
   * filtering logic is present in one way replication, so there are no oneWayReplIneligible*
   * metrics.
   */
  public static final String ONE_WAY_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY =
      "oneWayReplEligibleWALEntries";

  public static final String ONE_WAY_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY =
      "oneWayReplEligibleMutations";

  /** WAL entries/mutations exported if bidirectional replication is enabled. */
  public static final String BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY =
      "bidirectionalReplEligibleWALEntries";

  public static final String BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY =
      "bidirectionalReplEligibleMutations";

  /**
   * WAL entries/mutations not eligible for replication if bidirectional replication is enabled,
   * i.e. they come from a Bigtable-Hbase replicator.
   */
  public static final String BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY =
      "bidirectionalReplIneligibleWALEntries";

  public static final String BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY =
      "bidirectionalReplIneligibleMutations";
}
