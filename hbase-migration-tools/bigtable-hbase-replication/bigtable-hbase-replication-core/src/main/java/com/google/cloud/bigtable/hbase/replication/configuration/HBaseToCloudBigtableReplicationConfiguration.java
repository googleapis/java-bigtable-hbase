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

package com.google.cloud.bigtable.hbase.replication.configuration;

/** Static class containing all the configuration keys and their descriptions. */
public class HBaseToCloudBigtableReplicationConfiguration {

  // Only used for defining static config keys
  private HBaseToCloudBigtableReplicationConfiguration() {}

  /**
   * Threshold to consider the deleteFamilyBefore as a DeleteFamily mutation. When DeleteFamily or
   * HBase translates a DeleteFamily or DeleteRow to DeleteFamilyBeforeTimestamp(now). This is then
   * written to WAL. For local clusters, the WALKey.writeTime() is same as "now" from the
   * DeleteFamilyBeforeTimestamp mutation. However, if the mutation was generated from a different
   * cluster, the WALKey.writeTime and timestamp in DeleteFamilyBeforeTimestamp will have diff of
   * ReplicationLag. Users can set this config to Max(ReplicationLag) to make sure that all the
   * deleteRow/DeleteColumnFamily are correctly interpreted. If you only issue DeleteFamily or
   * DeleteRow mutations, you can set this to Integer.MAX_VALUE. This will lead to any
   * DeleteFamilyBeforeTimestamp where (timestamp < walkey.writeTime()) as DeleteFamily.
   */
  public static final String DELETE_FAMILY_WRITE_THRESHOLD_KEY =
      "google.bigtable.deletefamily.threshold";

  public static final int DEFAULT_DELETE_FAMILY_WRITE_THRESHOLD_IN_MILLIS = 5 * 60 * 1000;

  private static final String INCOMPATIBLE_MUTATION_ADAPTER_CLASS_KEY =
      "google.bigtable.incompatible_mutation.adapter.class";

  // Config keys to access project id and instance id from.
  public static final String PROJECT_KEY = "google.bigtable.project.id";
  public static final String INSTANCE_KEY = "google.bigtable.instance.id";
  public static final String APP_PROFILE_ID = "google.bigtable.app_profile.id";

  public static final String ENABLE_DRY_RUN_MODE_KEY = "google.bigtable.replication.enable_dry_run";
  public static final boolean DEFAULT_DRY_RUN_MODE = false;

  public static final String NUM_REPLICATION_SINK_THREADS_KEY =
      "google.bigtable.replication.thread_count";
  // TODO maybe it should depend on the number of processors on the VM.
  public static final int DEFAULT_THREAD_COUNT = 10;

  /**
   * Flag that determines if cells that exceed value of FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES_KEY
   * should be logged and dropped.
   */
  public static final String FILTER_LARGE_CELLS_KEY =
      "google.bigtable.replication.filter_large_cells_flag";

  public static final Boolean DEFAULT_FILTER_LARGE_CELLS = false;

  /**
   * Determines the size in bytes of the cells that should be logged and dropped when replicating to
   * Bigtable. Default: Max cell size accepted by Bigtable.
   */
  public static final String FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES_KEY =
      "google.bigtable.replication.large_cells_threshold_bytes";

  public static final Integer DEFAULT_FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES = 100 * 1024 * 1024;

  /**
   * Determines the size of request to CBT. This parameter controls the number of concurrent RPCs to
   * Cloud Bigtable.
   *
   * <p>Set it to higher value to reduce concurrency, setting it to a value >
   * replication.source.size.capacity will result in a single batch of writes to Cloud Bigtable per
   * CBTEndpoint replicate call.
   *
   * <p>Set it to a smaller value to send small batches with max concurrency of
   * "google.bigtable.replication.thread_count". This will result in lower replication lag for Cloud
   * Bigtable at the expense of more RPCs.
   */
  public static final String BATCH_SIZE_KEY = "google.bigtable.replication.batch_size_bytes";
  // TODO: Tune this parameter. Usually, this should be smaller than the HBase replication source
  // batch capacity by counts and bytes. These capacity are set by `replication.source.nb.capacity`
  // and `replication.source.size.capacity` config keys.
  public static final long DEFAULT_BATCH_SIZE_IN_BYTES = 500_000;

  /**
   * Enables logic to prevent replication loops from forming when this library is used in
   * conjunction with a Bigtable-HBase replicator.
   *
   * <p>CBT and HBase qualifiers must match those used in CBT replicator configs for this mode to
   * work.
   */
  public static final String CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY =
      "google.bigtable.replication.enable_bidirectional_replication";

  public static final String CBT_REPL_HBASE_QUALIFIER_KEY =
      "google.bigtable.replication.hbase_qualifier";
  public static final String CBT_REPL_CBT_QUALIFIER_KEY =
      "google.bigtable.replication.cbt_qualifier";

  public static final boolean CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE = false;
  public static final String CBT_REPL_HBASE_QUALIFIER = "SOURCE_HBASE";
  public static final String CBT_REPL_CBT_QUALIFIER = "SOURCE_CBT";
}
