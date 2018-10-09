/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * An immutable class providing access to configuration options for Bigtable.
 *
 * @author sduskis
 * @version $Id: $Id
 */
//TODO: Perhaps break this down into smaller options objects?
//TODO: This should be @Autovalue + Builder
public class BigtableOptions implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  // If set to a host:port address, this environment variable will configure the client to connect
  // to a Bigtable emulator running at the given address with plaintext negotiation.
  // TODO: Link to emulator documentation when available.
  /** Constant <code>BIGTABLE_EMULATOR_HOST_ENV_VAR="bigtableadmin.googleapis.com"</code> */
  public static final String BIGTABLE_EMULATOR_HOST_ENV_VAR = "BIGTABLE_EMULATOR_HOST";

  /** Constant <code>BIGTABLE_ADMIN_HOST_DEFAULT="bigtableadmin.googleapis.com"</code> */
  public static final String BIGTABLE_ADMIN_HOST_DEFAULT =
      "bigtableadmin.googleapis.com";
  /** Constant <code>BIGTABLE_DATA_HOST_DEFAULT="bigtable.googleapis.com"</code> */
  public static final String BIGTABLE_DATA_HOST_DEFAULT = "bigtable.googleapis.com";
  /** Constant <code>BIGTABLE_BATCH_DATA_HOST_DEFAULT="bigtable.googleapis.com"</code> */
  public static final String BIGTABLE_BATCH_DATA_HOST_DEFAULT = "batch-bigtable.googleapis.com";
  /** Constant <code>BIGTABLE_PORT_DEFAULT=443</code> */
  public static final int BIGTABLE_PORT_DEFAULT = 443;

  /** Constant <code>BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT=getDefaultDataChannelCount()</code> */
  public static final int BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT = getDefaultDataChannelCount();

  /** Constant <code>BIGTABLE_APP_PROFILE_DEFAULT=""</code>, defaults to the server default app profile */
  public static final String BIGTABLE_APP_PROFILE_DEFAULT = "";

  private static final Logger LOG = new Logger(BigtableOptions.class);

  private static int getDefaultDataChannelCount() {
    // 20 Channels seemed to work well on a 4 CPU machine, and this ratio seems to scale well for
    // higher CPU machines. Use no more than 250 Channels by default.
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    return Math.min(250, Math.max(1, availableProcessors * 4));
  }

  public static BigtableOptions getDefaultOptions() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }
  /**
   * A mutable builder for BigtableConnectionOptions.
   */
  public static class Builder {

    private BigtableOptions options = new BigtableOptions();

    @Deprecated
    public Builder() {
      options = new BigtableOptions();
      options.appProfileId = BIGTABLE_APP_PROFILE_DEFAULT;

      // Optional configuration for hosts - useful for the Bigtable team, more than anything else.
      options.dataHost = BIGTABLE_DATA_HOST_DEFAULT;
      options.adminHost = BIGTABLE_ADMIN_HOST_DEFAULT;
      options.port = BIGTABLE_PORT_DEFAULT;

      options.dataChannelCount = BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT;
      options.usePlaintextNegotiation = false;
      options.useCachedDataPool = false;

      options.retryOptions = new RetryOptions.Builder().build();
      options.callOptionsConfig = new CallOptionsConfig.Builder().build();
      // CredentialOptions.defaultCredentials() gets credentials from well known locations, such as
      // the Google Compute Engine metadata service or gcloud configuration in other environments. A
      // user can also override the default behavior with P12 or JSON configuration.
      options.credentialOptions = CredentialOptions.defaultCredentials();
      options.useBatch = false;
    }

    private Builder(BigtableOptions options) {
      this.options = options.clone();
    }

    public Builder setAdminHost(String adminHost) {
      options.adminHost = adminHost;
      return this;
    }

    public Builder setDataHost(String dataHost) {
      options.dataHost = dataHost;
      return this;
    }

    public Builder setPort(int port) {
      options.port = port;
      return this;
    }

    public Builder setProjectId(String projectId) {
      options.projectId = projectId;
      return this;
    }

    public Builder setInstanceId(String instanceId) {
      options.instanceId = instanceId;
      return this;
    }

    public Builder setAppProfileId(String appProfileId) {
      Preconditions.checkNotNull(appProfileId);
      options.appProfileId = appProfileId;
      return this;
    }

    public Builder setCredentialOptions(CredentialOptions credentialOptions) {
      options.credentialOptions = credentialOptions;
      return this;
    }

    public Builder setUserAgent(String userAgent) {
      options.userAgent = userAgent;
      return this;
    }

    public Builder setDataChannelCount(int dataChannelCount) {
      options.dataChannelCount = dataChannelCount;
      return this;
    }

    public int getDataChannelCount() {
      return options.dataChannelCount;
    }

    public Builder setRetryOptions(RetryOptions retryOptions) {
      options.retryOptions = retryOptions;
      return this;
    }

    public Builder setBulkOptions(BulkOptions bulkOptions) {
      options.bulkOptions = bulkOptions;
      return this;
    }

    public Builder setUsePlaintextNegotiation(boolean usePlaintextNegotiation) {
      options.usePlaintextNegotiation = usePlaintextNegotiation;
      return this;
    }

    /**
     * This enables an experimental {@link BigtableSession} feature that caches datapools for cases
     * where there are many HBase Connections / BigtableSessions opened. This happens frequently in
     * Dataflow
     * @param useCachedDataPool
     * @return this
     */
    public Builder setUseCachedDataPool(boolean useCachedDataPool) {
      options.useCachedDataPool = useCachedDataPool;
      return this;
    }

    public Builder setCallOptionsConfig(CallOptionsConfig callOptionsConfig) {
      options.callOptionsConfig = callOptionsConfig;
      return this;
    }

    public Builder setUseBatch(boolean useBatch) {
      options.useBatch = useBatch;
      return this;
    }

    /**
     * Apply emulator settings from the relevant environment variable, if set.
     */
    private void applyEmulatorEnvironment() {
      // Look for a host:port for the emulator.
      String emulatorHost = System.getenv(BIGTABLE_EMULATOR_HOST_ENV_VAR);
      if (emulatorHost == null) {
        return;
      }

      enableEmulator(emulatorHost);
    }

    public Builder enableEmulator(String emulatorHostAndPort) {
      String[] hostPort = emulatorHostAndPort.split(":");
      Preconditions.checkArgument(hostPort.length == 2,
          "Malformed " + BIGTABLE_EMULATOR_HOST_ENV_VAR + " environment variable: " +
          emulatorHostAndPort + ". Expecting host:port.");

      int port;
      try {
        port = Integer.parseInt(hostPort[1]);
      } catch (NumberFormatException e) {
        throw new RuntimeException("Invalid port in " + BIGTABLE_EMULATOR_HOST_ENV_VAR +
            " environment variable: " + emulatorHostAndPort);
      }
      enableEmulator(hostPort[0], port);
      return this;
    }

    public Builder enableEmulator(String host, int port) {
      Preconditions.checkArgument(host != null && !host.isEmpty(), "Host cannot be null or empty");
      Preconditions.checkArgument(port > 0, "Port must be positive");
      setUsePlaintextNegotiation(true);
      setCredentialOptions(CredentialOptions.nullCredential());
      setDataHost(host);
      setAdminHost(host);
      setPort(port);

      LOG.info("Connecting to the Bigtable emulator at " + host + ":" + port);
      return this;
    }

    public BigtableOptions build() {
      if (options.bulkOptions == null) {
        int maxInflightRpcs =
            BulkOptions.BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT * options.dataChannelCount;
        options.bulkOptions = BulkOptions.builder().setMaxInflightRpcs(maxInflightRpcs).build();
      } else if (options.bulkOptions.getMaxInflightRpcs() <= 0) {
        int maxInflightRpcs =
            BulkOptions.BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT * options.dataChannelCount;
        options.bulkOptions = options.bulkOptions.toBuilder().setMaxInflightRpcs(maxInflightRpcs).build();
      }
      applyEmulatorEnvironment();
      options.adminHost = Preconditions.checkNotNull(options.adminHost);
      options.dataHost = Preconditions.checkNotNull(options.dataHost);
      if (!Strings.isNullOrEmpty(options.projectId)
          && !Strings.isNullOrEmpty(options.instanceId)) {
        options.instanceName = new BigtableInstanceName(options.projectId, options.instanceId);
      } else {
        options.instanceName = null;
      }

      if(options.useBatch) {
        options.useCachedDataPool = true;
        options.dataHost = BIGTABLE_BATCH_DATA_HOST_DEFAULT;
        RetryOptions.Builder retryOptionsBuilder = options.retryOptions.toBuilder();
        retryOptionsBuilder.setInitialBackoffMillis((int) TimeUnit.SECONDS.toMillis(5));
        retryOptionsBuilder.setMaxElapsedBackoffMillis((int) TimeUnit.SECONDS.toMillis(5));
        options.retryOptions = retryOptionsBuilder.build();
      }
      LOG.debug("Connection Configuration: projectId: %s, instanceId: %s, data host %s, "
              + "admin host %s.",
          options.projectId,
          options.instanceId,
          options.dataHost,
          options.adminHost);

      return options;
    }
  }

  private String adminHost;
  private String dataHost;
  private int port;
  private String projectId;
  private String instanceId;
  private String appProfileId = BIGTABLE_APP_PROFILE_DEFAULT;
  private String userAgent;
  private int dataChannelCount;
  private boolean usePlaintextNegotiation;
  private boolean useCachedDataPool;

  private BigtableInstanceName instanceName;

  private BulkOptions bulkOptions;
  private CallOptionsConfig callOptionsConfig;
  private CredentialOptions credentialOptions;
  private RetryOptions retryOptions;
  private boolean useBatch;

  @VisibleForTesting
  BigtableOptions() {
  }

  /**
   * <p>Getter for the field <code>projectId</code>.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * <p>Getter for the field <code>dataHost</code>.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  public String getDataHost() {
    return dataHost;
  }

  /**
   * <p>Getter for the field <code>tableAdminHost</code>.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  public String getAdminHost() {
    return adminHost;
  }

  /**
   * <p>Getter for the field <code>instanceId</code>.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * <p>Getter for the field <code>appProfileId</code>.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  public String getAppProfileId() {
    return appProfileId;
  }

  /**
   * <p>Getter for the field <code>port</code>.</p>
   *
   * @return a int.
   */
  public int getPort() {
    return port;
  }

  /**
   * Get the credential this object was constructed with. May be null.
   *
   * @return Null to indicate no credentials, otherwise, the Credentials object.
   */
  public CredentialOptions getCredentialOptions() {
    return credentialOptions;
  }

  /**
   * Gets the user-agent to be appended to User-Agent header when creating new streams
   * for the channel.
   *
   * @return a {@link java.lang.String} object.
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * Options controlling retries.
   *
   * @return a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   */
  public RetryOptions getRetryOptions() {
    return retryOptions;
  }

  /**
   * The number of data channels to create.
   *
   * @return a int.
   */
  public int getChannelCount() {
    return dataChannelCount;
  }

  /**
   * <p>Getter for the field <code>instanceName</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableInstanceName} object.
   */
  public BigtableInstanceName getInstanceName() {
    return instanceName;
  }

  /**
   * <p>Getter for the field <code>bulkOptions</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.BulkOptions} object.
   */
  public BulkOptions getBulkOptions() {
    return bulkOptions;
  }

  /**
   * <p>usePlaintextNegotiation.</p>
   *
   * @return a boolean.
   */
  public boolean usePlaintextNegotiation() {
    return usePlaintextNegotiation;
  }

  /**
   * <p>Getter for the field <code>callOptionsConfig</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.CallOptionsConfig} object.
   */
  public CallOptionsConfig getCallOptionsConfig() {
    return callOptionsConfig;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != BigtableOptions.class) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    BigtableOptions other = (BigtableOptions) obj;
    return (port == other.port)
        && (dataChannelCount == other.dataChannelCount)
        && (usePlaintextNegotiation == other.usePlaintextNegotiation)
        && (useCachedDataPool == other.useCachedDataPool)
        && Objects.equals(adminHost, other.adminHost)
        && Objects.equals(dataHost, other.dataHost)
        && Objects.equals(projectId, other.projectId)
        && Objects.equals(instanceId, other.instanceId)
        && Objects.equals(appProfileId, other.appProfileId)
        && Objects.equals(userAgent, other.userAgent)
        && Objects.equals(credentialOptions, other.credentialOptions)
        && Objects.equals(retryOptions, other.retryOptions)
        && Objects.equals(bulkOptions, other.bulkOptions)
        && Objects.equals(callOptionsConfig, other.callOptionsConfig)
        && Objects.equals(useBatch, other.useBatch);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("dataHost", dataHost)
        .add("adminHost", adminHost)
        .add("port", port)
        .add("projectId", projectId)
        .add("instanceId", instanceId)
        .add("appProfileId", appProfileId)
        .add("userAgent", userAgent)
        .add("credentialType", credentialOptions.getCredentialType())
        .add("dataChannelCount", dataChannelCount)
        .add("retryOptions", retryOptions)
        .add("bulkOptions", bulkOptions)
        .add("callOptionsConfig", callOptionsConfig)
        .add("usePlaintextNegotiation", usePlaintextNegotiation)
        .add("useCachedDataPool", useCachedDataPool)
        .add("useBatch", useBatch)
        .toString();
  }

  /**
   * <p>toBuilder.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.BigtableOptions.Builder} object.
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /**
   * Experimental feature to allow situations with multiple connections to optimize their startup
   * time.
   * @return true if this feature should be turned on in {@link BigtableSession}.
   */
  public boolean useCachedChannel() {
    return useCachedDataPool;
  }

  public boolean useBatch() {
    return useBatch;
  }

  protected BigtableOptions clone() {
    try {
      return (BigtableOptions) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Could not cloe BigtableOptions");
    }
  }
}
