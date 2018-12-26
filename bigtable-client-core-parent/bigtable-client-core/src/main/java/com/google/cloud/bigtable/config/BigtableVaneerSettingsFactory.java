/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.config;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import io.grpc.internal.GrpcUtil;
import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.annotation.Nonnull;
import org.threeten.bp.Duration;

import static com.google.api.client.util.Preconditions.checkState;
import static com.google.cloud.bigtable.config.CallOptionsConfig.SHORT_TIMEOUT_MS_DEFAULT;
import static org.threeten.bp.Duration.ofMillis;

/**
 * Static methods to convert an instance of {@link BigtableOptions} to a
 * {@link BigtableDataSettings} or {@link BigtableTableAdminSettings} instance .
 */
public class BigtableVaneerSettingsFactory {

  /** Constant <code>LOG</code> */
  private static final Logger LOG = new Logger(BigtableVaneerSettingsFactory.class);

  /**
   * To create an instance of {@link BigtableDataSettings} from {@link BigtableOptions}.
   *
   * @param options a {@link BigtableOptions} object.
   * @return a {@link BigtableDataSettings} object.
   * @throws IOException if any.
   */
  public static BigtableDataSettings createBigtableDataSettings(@Nonnull final BigtableOptions options) throws IOException {
    checkState(options.getProjectId() != null, "Project ID is required");
    checkState(options.getInstanceId() != null, "Instance ID is required");
    checkState(options.getRetryOptions().enableRetries(), "Disabling retries is not currently supported.");

    final BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder();
    final String endpoint = options.getDataHost() + ":" + options.getPort();

    builder.setProjectId(options.getProjectId());
    builder.setInstanceId(options.getInstanceId());
    builder.setAppProfileId(options.getAppProfileId());

    builder.setEndpoint(endpoint);

    builder.setCredentialsProvider(buildCredentialProvider(options.getCredentialOptions()));

    buildBulkMutationsSettings(builder, options);

    buildCheckAndMutateRowSettings(builder, options.getCallOptionsConfig().getShortRpcTimeoutMs());

    buildReadModifyWriteSettings(builder, options.getCallOptionsConfig().getShortRpcTimeoutMs());

    buildReadRowsSettings(builder, options);

    buildMutateRowSettings(builder, options);

    buildSampleRowKeysSettings(builder, options);

    String userAgent = BigtableVersionInfo.CORE_USER_AGENT + "," + options.getUserAgent();
    HeaderProvider headers = FixedHeaderProvider.create(GrpcUtil.USER_AGENT_KEY.name(), userAgent);

    builder.setTransportChannelProvider(
        InstantiatingGrpcChannelProvider.newBuilder()
            .setHeaderProvider(headers)
            .setEndpoint(endpoint)
            .setPoolSize(options.getChannelCount())
            .build());

    return builder.build();
  }

  /**
   * To create an instance of {@link BigtableTableAdminSettings} from {@link BigtableOptions}.
   *
   * @param options a {@link BigtableOptions} object.
   * @return a {@link BigtableTableAdminSettings} object.
   * @throws IOException if any.
   */
  public static BigtableTableAdminSettings createTableAdminSettings(@Nonnull final BigtableOptions options)
      throws IOException {
    checkState(options.getProjectId() != null, "Project ID is required");
    checkState(options.getInstanceId() != null, "Instance ID is required");

    final BigtableTableAdminSettings.Builder adminBuilder = BigtableTableAdminSettings.newBuilder();

    adminBuilder.setProjectId(options.getProjectId());
    adminBuilder.setInstanceId(options.getInstanceId());

    final String endpoint = options.getAdminHost() + ":" + options.getPort();
    adminBuilder.stubSettings().setEndpoint(endpoint);

    //Overriding default credential with BigtableOptions's credential.
    adminBuilder.stubSettings()
        .setCredentialsProvider(buildCredentialProvider(options.getCredentialOptions()));

    String userAgent = BigtableVersionInfo.CORE_USER_AGENT + "," + options.getUserAgent();
    HeaderProvider headers = FixedHeaderProvider.create(GrpcUtil.USER_AGENT_KEY.name(), userAgent);

    adminBuilder.stubSettings().setTransportChannelProvider(
        InstantiatingGrpcChannelProvider.newBuilder()
            .setHeaderProvider(headers)
            .setEndpoint(endpoint)
            .setPoolSize(options.getChannelCount())
            .build());

    return adminBuilder.build();
  }

  /**
   * This method is use to build {@link BatchingSettings}.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param options a {@link BigtableOptions} object.
   */
  private static void buildBulkMutationsSettings(Builder builder, BigtableOptions options) {
    BulkOptions bulkOptions = options.getBulkOptions();
    BatchingSettings.Builder batchSettingsBuilder = BatchingSettings.newBuilder();

    long autoFlushMs = bulkOptions.getAutoflushMs();
    long bulkMaxRowKeyCount = bulkOptions.getBulkMaxRowKeyCount();
    long maxInflightRpcs = bulkOptions.getMaxInflightRpcs();
    long shortRpcTimeoutMs = options.getCallOptionsConfig().getShortRpcTimeoutMs();

    if (autoFlushMs > 0) {
      batchSettingsBuilder.setDelayThreshold(ofMillis(autoFlushMs));
    }
    FlowControlSettings.Builder flowControlBuilder = FlowControlSettings.newBuilder();
    if (maxInflightRpcs > 0) {
      flowControlBuilder
          .setMaxOutstandingRequestBytes(bulkOptions.getMaxMemory())
          .setMaxOutstandingElementCount(maxInflightRpcs * bulkMaxRowKeyCount);
    }

    batchSettingsBuilder
        .setIsEnabled(bulkOptions.useBulkApi())
        .setElementCountThreshold(Long.valueOf(bulkOptions.getBulkMaxRowKeyCount()))
        .setRequestByteThreshold(bulkOptions.getBulkMaxRequestSize())
        .setFlowControlSettings(flowControlBuilder.build());

    // TODO(rahulkql): implement bulkMutationThrottling & bulkMutationRpcTargetMs, once available
    builder.bulkMutationsSettings()
        .setBatchingSettings(batchSettingsBuilder.build())
        .setSimpleTimeoutNoRetries(
            ofMillis(shortRpcTimeoutMs));
  }

  /**
   * To build BigtableDataSettings#sampleRowKeysSettings with default retries settings.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param options a {@link BigtableOptions} object.
   */
  private static void buildSampleRowKeysSettings(Builder builder, BigtableOptions options) {
    builder.sampleRowKeysSettings()
        .setRetrySettings(buildIdempotentRetrySettings(options));
  }

  /**
   * To build BigtableDataSettings#mutateRowSettings with default retries settings.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param options a {@link BigtableOptions} object.
   */
  private static void buildMutateRowSettings(Builder builder, BigtableOptions options) {
    builder.mutateRowSettings()
        .setRetrySettings(buildIdempotentRetrySettings(options));
  }

  /**
   * To build BigtableDataSettings#readRowsSettings with default retries settings.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param options a {@link BigtableOptions} object.
   */
  private static void buildReadRowsSettings(Builder builder, BigtableOptions options) {
    RetryOptions retryOptions = options.getRetryOptions();

    RetrySettings.Builder retryBuilder = RetrySettings.newBuilder()
        .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(ofMillis(retryOptions.getMaxElapsedBackoffMillis()))
        .setMaxAttempts(retryOptions.getMaxScanTimeoutRetries());

    // configurations for RPC timeouts
    Duration readPartialRowTimeout = ofMillis(retryOptions.getReadPartialRowTimeoutMillis());
    retryBuilder
        .setInitialRpcTimeout(readPartialRowTimeout)
        .setMaxRpcTimeout(readPartialRowTimeout)
        .setTotalTimeout(ofMillis(options.getCallOptionsConfig().getLongRpcTimeoutMs()));

    builder.readRowsSettings()
        .setRetrySettings(retryBuilder.build());
  }

  /**
   * This method builds BigtableDataSettings#readModifyWriteSettngs when short timeout is other
   * than 60_000 ms.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param rpcTimeoutMs a long value for RPC timeout.
   */
  private static void buildReadModifyWriteSettings(Builder builder, long rpcTimeoutMs) {
    if(rpcTimeoutMs != SHORT_TIMEOUT_MS_DEFAULT) {
      builder.readModifyWriteRowSettings()
          .setSimpleTimeoutNoRetries(ofMillis(rpcTimeoutMs));
    }
  }

  /**
   * This method builds BigtableDataSettings#checkAndMutateRowSettings when short timeout is other
   * than 60_000 ms.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param rpcTimeoutMs a long value for RPC timeout.
   */
  private static void buildCheckAndMutateRowSettings(Builder builder, long rpcTimeoutMs) {
    if(rpcTimeoutMs != SHORT_TIMEOUT_MS_DEFAULT) {
      builder.checkAndMutateRowSettings()
          .setSimpleTimeoutNoRetries(ofMillis(rpcTimeoutMs));
    }
  }

  /**
   * To create default {@link RetrySettings} for all idempotent method.
   *
   * @param options a {@link BigtableOptions} object.
   * @return an object of {@link RetrySettings} with idempotent method configuration.
   */
  private static RetrySettings buildIdempotentRetrySettings(BigtableOptions options) {
    RetryOptions retryOptions = options.getRetryOptions();

    RetrySettings.Builder retryBuilder = RetrySettings.newBuilder()
        .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(ofMillis(retryOptions.getMaxElapsedBackoffMillis()))
        .setMaxAttempts(retryOptions.getMaxScanTimeoutRetries());

    // configurations for RPC timeouts
    Duration shortRpcTimeout = ofMillis(options.getCallOptionsConfig().getShortRpcTimeoutMs());
    retryBuilder
        .setInitialRpcTimeout(shortRpcTimeout)
        .setMaxRpcTimeout(shortRpcTimeout)
        .setTotalTimeout(ofMillis(options.getCallOptionsConfig().getLongRpcTimeoutMs()));

    if (retryOptions.allowRetriesWithoutTimestamp()) {
      LOG.warn("Retries without Timestamp does not support yet.");
    }

    return retryBuilder.build();
  }

  /**
   * To create {@link CredentialsProvider} based on {@link CredentialOptions}.
   *
   * @param credentialOptions a {@link CredentialOptions} object.
   * @throws IOException if any.
   */
  private static CredentialsProvider buildCredentialProvider(
      CredentialOptions credentialOptions) throws IOException {
    try {
      final Credentials credentials = CredentialFactory.getCredentials(credentialOptions);
      if (credentials == null) {
        LOG.info("Enabling the use of null credentials. This should not be used in production.");
        return NoCredentialsProvider.create();
      }

      return FixedCredentialsProvider.create(credentials);
    } catch (GeneralSecurityException exception) {
      throw new IOException("Could not initialize credentials.", exception);
    }
  }
}
