/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase;

import com.google.api.core.BetaApi;
import com.google.api.core.InternalExtensionOnly;

/**
 * Define {@link org.apache.hadoop.conf.Configuration} names for setting {@link
 * com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings}.
 */
@InternalExtensionOnly
public class BigtableOptionsFactory {
  /** Constant <code>BIGTABLE_PORT_KEY="google.bigtable.endpoint.port"</code> */
  public static final String BIGTABLE_PORT_KEY = "google.bigtable.endpoint.port";
  /** Constant <code>BIGTABLE_ADMIN_HOST_KEY="google.bigtable.admin.endpoint.host"</code> */
  public static final String BIGTABLE_ADMIN_HOST_KEY = "google.bigtable.admin.endpoint.host";
  /** Constant <code>BIGTABLE_HOST_KEY="google.bigtable.endpoint.host"</code> */
  public static final String BIGTABLE_HOST_KEY = "google.bigtable.endpoint.host";

  /**
   * Constant <code>BIGTABLE_HOST_KEY="google.bigtable.emulator.endpoint.host"</code>. Values should
   * be of format: `host:port`
   */
  public static final String BIGTABLE_EMULATOR_HOST_KEY = "google.bigtable.emulator.endpoint.host";

  /** Constant <code>PROJECT_ID_KEY="google.bigtable.project.id"</code> */
  public static final String PROJECT_ID_KEY = "google.bigtable.project.id";
  /** Constant <code>INSTANCE_ID_KEY="google.bigtable.instance.id"</code> */
  public static final String INSTANCE_ID_KEY = "google.bigtable.instance.id";
  /** Constant <code>APP_PROFILE_ID_KEY="google.bigtable.app_profile.id"</code> */
  public static final String APP_PROFILE_ID_KEY = "google.bigtable.app_profile.id";

  /**
   * Constant <code>BIGTABLE_SNAPSHOT_CLUSTER_ID_KEY="google.bigtable.snapshot.cluster.id"</code>
   */
  public static final String BIGTABLE_SNAPSHOT_CLUSTER_ID_KEY =
      "google.bigtable.snapshot.cluster.id";
  /**
   * Constant <code>
   * BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_KEY="google.bigtable.snapshot.default.ttl.secs"</code> Will
   * default to 24 hrs if not set (see BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_VALUE)
   */
  public static final String BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_KEY =
      "google.bigtable.snapshot.default.ttl.secs";

  /*
   * Constant for default ttl for backups - 24 hours
   */
  public static final int BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_VALUE = 86400;

  /** Constant <code>CUSTOM_USER_AGENT_KEY="google.bigtable.custom.user.agent"</code> */
  public static final String CUSTOM_USER_AGENT_KEY = "google.bigtable.custom.user.agent";

  /**
   * Key to set to enable service accounts to be used, either metadata server-based or P12-based.
   * Defaults to enabled.
   */
  public static final String BIGTABLE_USE_SERVICE_ACCOUNTS_KEY =
      "google.bigtable.auth.service.account.enable";
  /** Constant <code>BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT=true</code> */
  public static final boolean BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT = true;

  /** Key to allow unit tests to proceed with an invalid credential configuration. */
  public static final String BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY =
      "google.bigtable.auth.null.credential.enable";
  /** Constant <code>BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT=false</code> */
  public static final boolean BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT = false;

  /**
   * Key to set when using P12 keyfile authentication. The value should be the service account email
   * address as displayed. If this value is not set and using service accounts is enabled, a
   * metadata server account will be used.
   */
  public static final String BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY =
      "google.bigtable.auth.service.account.email";

  /**
   * Key to set custom Credentials class. This class must extend {@link com.google.auth.Credentials}
   * class and must provide a public constructor which takes {@link
   * org.apache.hadoop.conf.Configuration} argument.
   */
  public static final String BIGTABLE_CUSTOM_CREDENTIALS_CLASS_KEY =
      "google.bigtable.auth.custom.credentials.class";

  /**
   * Key to set to a location where a P12 keyfile can be found that corresponds to the provided
   * service account email address.
   */
  public static final String BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY =
      "google.bigtable.auth.service.account.keyfile";

  /** Key to set to a location where a json security credentials file can be found. */
  public static final String BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY =
      "google.bigtable.auth.json.keyfile";

  /** Key to set to a json security credentials string. */
  public static final String BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY =
      "google.bigtable.auth.json.value";

  /**
   * Key to set to a boolean flag indicating whether or not grpc retries should be enabled. The
   * default is to enable retries on failed idempotent operations.
   */
  public static final String ENABLE_GRPC_RETRIES_KEY = "google.bigtable.grpc.retry.enable";

  /**
   * By default, the hbase client will set the timestamp on the client side before sending a Put if
   * it's not set already. That's done to ensure that only a single cell is written for the put if
   * retries occur. Retries aren't common, and writing multiple cells isn't a problem in many cases.
   * Sometimes, setting the server timestamp is beneficial. If you want the server-side time stamp,
   * set this to true.
   */
  public static final String ALLOW_NO_TIMESTAMP_RETRIES_KEY =
      "google.bigtable.alllow.no.timestamp.retries";

  /**
   * Key to set to a comma separated list of grpc codes to retry. See {@link io.grpc.Status.Code}
   * for more information.
   */
  public static final String ADDITIONAL_RETRY_CODES = "google.bigtable.grpc.retry.codes";

  /**
   * Key to set to a boolean flag indicating whether or not to retry grpc call on deadline exceeded.
   * This flag is used only when grpc retries is enabled.
   */
  public static final String ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY =
      "google.bigtable.grpc.retry.deadlineexceeded.enable";

  /**
   * Key to set the initial amount of time to wait for retries, given a backoff policy on errors.
   * This flag is used only when grpc retries is enabled.
   */
  public static final String INITIAL_ELAPSED_BACKOFF_MILLIS_KEY =
      "google.bigtable.grpc.retry.initial.elapsed.backoff.ms";

  /**
   * Key to set the maximum amount of time to wait for retries, given a backoff policy on errors.
   * This flag is used only when grpc retries is enabled.
   */
  public static final String MAX_ELAPSED_BACKOFF_MILLIS_KEY =
      "google.bigtable.grpc.retry.max.elapsed.backoff.ms";

  /** Key to set the amount of time to wait when reading a partial row. */
  public static final String READ_PARTIAL_ROW_TIMEOUT_MS =
      "google.bigtable.grpc.read.partial.row.timeout.ms";

  /** Key to set the number of time to retry after a scan timeout */
  public static final String MAX_SCAN_TIMEOUT_RETRIES =
      "google.bigtable.grpc.retry.max.scan.timeout.retries";

  /** Key to set the maximum number of messages to buffer when scanning. */
  public static final String READ_BUFFER_SIZE = "google.bigtable.grpc.read.streaming.buffer.size";

  /** The number of grpc channels to open for asynchronous processing such as puts. */
  public static final String BIGTABLE_DATA_CHANNEL_COUNT_KEY = "google.bigtable.grpc.channel.count";

  /** @deprecated This option is now ignored. Please configure explicit timeouts instead */
  @Deprecated public static final String BIGTABLE_USE_BULK_API = "google.bigtable.use.bulk.api";

  /** Constant <code>BIGTABLE_USE_BATCH="google.bigtable.use.batch"</code> */
  public static final String BIGTABLE_USE_BATCH = "google.bigtable.use.batch";

  /**
   * Constant <code>
   * BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES="google.bigtable.bulk.max.request.size.b"{trunked}</code>
   */
  public static final String BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES =
      "google.bigtable.bulk.max.request.size.bytes";

  /**
   * Constant <code>BIGTABLE_BULK_MAX_ROW_KEY_COUNT="google.bigtable.bulk.max.row.key.count"</code>
   */
  public static final String BIGTABLE_BULK_MAX_ROW_KEY_COUNT =
      "google.bigtable.bulk.max.row.key.count";

  /** Constant <code>BIGTABLE_BULK_AUTOFLUSH_MS_KEY="google.bigtable.bulk.autoflush.ms"</code> */
  public static final String BIGTABLE_BULK_AUTOFLUSH_MS_KEY = "google.bigtable.bulk.autoflush.ms";

  /**
   * Constant <code>MAX_INFLIGHT_RPCS_KEY="google.bigtable.buffered.mutator.max.in"{trunked}</code>
   */
  public static final String MAX_INFLIGHT_RPCS_KEY =
      "google.bigtable.buffered.mutator.max.inflight.rpcs";

  /** The maximum amount of memory to be used for asynchronous buffered mutator RPCs. */
  public static final String BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY =
      "google.bigtable.buffered.mutator.max.memory";

  /**
   * Turn on a feature that will reduce the likelihood of BufferedMutator overloading a Cloud
   * Bigtable cluster.
   */
  public static final String BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING =
      "google.bigtable.buffered.mutator.throttling.enable";

  /**
   * The default target RPC response time for a MutateRows request. This value is meaningful if bulk
   * mutation throttling is enabled. 100 ms is a generally ok latency for MutateRows RPCs, but it
   * could go higher (for example 300 ms) for less latency sensitive applications that need more
   * throughput, or lower (10 ms) for latency sensitive applications.
   *
   * <p>For internal use only - public for technical reasons.
   */
  public static final int BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT = 100;

  /** Tweak the throttling */
  public static final String BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS =
      "google.bigtable.buffered.mutator.throttling.threshold.ms";

  /**
   * Constant <code>BIGTABLE_USE_PLAINTEXT_NEGOTIATION="google.bigtable.use.plaintext.negotiation"
   * </code>
   */
  public static final String BIGTABLE_USE_PLAINTEXT_NEGOTIATION =
      "google.bigtable.use.plaintext.negotiation";

  /**
   * Constant <code>
   * BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL="google.bigtable.use.cached.data.channel.pool"</code>
   */
  public static final String BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL =
      "google.bigtable.use.cached.data.channel.pool";

  /** The number of asynchronous workers to use for buffered mutator operations. */
  public static final String BIGTABLE_ASYNC_MUTATOR_COUNT_KEY =
      "google.bigtable.buffered.mutator.async.worker.count";

  /**
   * Should timeouts be used? Currently, this feature is experimental.
   *
   * @deprecated This is no longer used, timeouts are always enabled now
   */
  @Deprecated
  public static final String BIGTABLE_USE_TIMEOUTS_KEY = "google.bigtable.rpc.use.timeouts";

  /**
   * If timeouts are set, how many milliseconds should pass before a DEADLINE_EXCEEDED? Currently,
   * this feature is experimental.
   */
  public static final String BIGTABLE_RPC_TIMEOUT_MS_KEY = "google.bigtable.rpc.timeout.ms";

  /**
   * If timeouts are set, how many milliseconds should pass before a DEADLINE_EXCEEDED for an
   * individual RPC attempt? Note that multiple attempts may happen within an overall operation,
   * whose timeout is governed by {@link #BIGTABLE_RPC_TIMEOUT_MS_KEY}. Currently, this feature is
   * experimental.
   */
  @BetaApi("The API for setting attempt timeouts is not yet stable and may change in the future")
  public static final String BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY =
      "google.bigtable.rpc.attempt.timeout.ms";

  /**
   * If timeouts are set, how many milliseconds should pass before a DEADLINE_EXCEEDED for a batch
   * read or scan? Currently, this feature is experimental.
   *
   * @deprecated Please use {@link #BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY} for batch mutation or {@link
   *     #BIGTABLE_READ_RPC_TIMEOUT_MS_KEY} for batch read or scan.
   */
  @Deprecated
  public static final String BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY =
      "google.bigtable.long.rpc.timeout.ms";

  /**
   * If timeouts are set, how many milliseconds should pass before a DEADLINE_EXCEEDED for a batch
   * mutation. Currently, this feature is experimental.
   */
  public static final String BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY =
      "google.bigtable.mutate.rpc.timeout.ms";

  /**
   * If timeouts are set, how many milliseconds should pass before a DEADLINE_EXCEEDED for an RPC
   * attempt within a batch mutation. Note that multiple attempts may happen within an overall
   * operation, whose timeout is governed by {@link #BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY}. Currently,
   * this feature is experimental.
   */
  @BetaApi("The API for setting attempt timeouts is not yet stable and may change in the future")
  public static final String BIGTABLE_MUTATE_RPC_ATTEMPT_TIMEOUT_MS_KEY =
      "google.bigtable.mutate.rpc.attempt.timeout.ms";

  /**
   * If timeouts are set, how many milliseconds should pass before a DEADLINE_EXCEEDED for a batch
   * read or scan. Currently, this feature is experimental.
   */
  public static final String BIGTABLE_READ_RPC_TIMEOUT_MS_KEY =
      "google.bigtable.read.rpc.timeout.ms";

  /**
   * If timeouts are set, how many milliseconds should pass before a DEADLINE_EXCEEDED for an RPC
   * attempt within a batch read or scan. Note that multiple attempts may happen within an overall
   * operation, whose timeout is governed by {@link #BIGTABLE_READ_RPC_TIMEOUT_MS_KEY}. Currently,
   * this feature is experimental.
   */
  @BetaApi("The API for setting attempt timeouts is not yet stable and may change in the future")
  public static final String BIGTABLE_READ_RPC_ATTEMPT_TIMEOUT_MS_KEY =
      "google.bigtable.read.rpc.attempt.timeout.ms";

  /** Allow namespace methods to be no-ops */
  public static final String BIGTABLE_NAMESPACE_WARNING_KEY = "google.bigtable.namespace.warnings";

  /** Tracing cookie to send in the header with the requests */
  @BetaApi("The API for setting tracing cookie is not yet stable and may change in the future")
  public static final String BIGTABLE_TRACING_COOKIE = "google.bigtable.tracing.cookie.header";

  /**
   * If this option is set to true, log a warning when creating a managed connection instead of
   * throwing illegal argument exception.
   */
  @BetaApi("This API is not yet stable and may change in the future")
  public static final String MANAGED_CONNECTION_WARNING =
      "google.bigtable.managed.connection.warning";

  /**
   * Option to enable flow control. When enabled, traffic to Bigtable is automatically rate-limited
   * based on the current Bigtable usage to prevent overloading Bigtable clusters while keeping
   * enough load to trigger Bigtable Autoscaling (if enabled) to provision more nodes as needed. It
   * is different from the flow control set by {@link #BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING},
   * which limits the number of outstanding requests to Bigtable.
   */
  public static final String BIGTABLE_ENABLE_BULK_MUTATION_FLOW_CONTROL =
      "google.bigtable.enable.bulk.mutation.flow.control";
}
