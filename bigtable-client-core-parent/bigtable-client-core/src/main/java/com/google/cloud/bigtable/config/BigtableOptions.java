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
public class BigtableOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  // If set to a host:port address, this environment variable will configure the client to connect
  // to a Bigtable emulator running at the given address with plaintext negotiation.
  // TODO: Link to emulator documentation when available.
  /** Constant <code>BIGTABLE_EMULATOR_HOST_ENV_VAR="bigtableadmin.googleapis.com"</code> */
  public static final String BIGTABLE_EMULATOR_HOST_ENV_VAR = "BIGTABLE_EMULATOR_HOST";

  /** Constant <code>BIGTABLE_TABLE_ADMIN_HOST_DEFAULT="bigtableadmin.googleapis.com"</code> */
  public static final String BIGTABLE_TABLE_ADMIN_HOST_DEFAULT =
      "bigtableadmin.googleapis.com";
  /** Constant <code>BIGTABLE_INSTANCE_ADMIN_HOST_DEFAULT="bigtableadmin.googleapis.com"</code> */
  public static final String BIGTABLE_INSTANCE_ADMIN_HOST_DEFAULT =
      "bigtableadmin.googleapis.com";
  /** Constant <code>BIGTABLE_DATA_HOST_DEFAULT="bigtable.googleapis.com"</code> */
  public static final String BIGTABLE_DATA_HOST_DEFAULT = "bigtable.googleapis.com";
  /** Constant <code>BIGTABLE_BATCH_DATA_HOST_DEFAULT="bigtable.googleapis.com"</code> */
  public static final String BIGTABLE_BATCH_DATA_HOST_DEFAULT = "batch-bigtable.googleapis.com";
  /** Constant <code>BIGTABLE_PORT_DEFAULT=443</code> */
  public static final int BIGTABLE_PORT_DEFAULT = 443;

  /** Constant <code>BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT=getDefaultDataChannelCount()</code> */
  public static final int BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT = getDefaultDataChannelCount();

  private static final Logger LOG = new Logger(BigtableOptions.class);

  private static int getDefaultDataChannelCount() {
    // 20 Channels seemed to work well on a 4 CPU machine, and this ratio seems to scale well for
    // higher CPU machines. Use no more than 250 Channels by default.
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    return (int) Math.min(250, Math.max(1, Math.ceil(availableProcessors * 2.5d)));
  }

  /**
   * A mutable builder for BigtableConnectionOptions.
   */
  public static class Builder {
    // Configuration that a user is required to set.
    private String projectId;
    private String userAgent;

    private String instanceId;

    // Legacy v1 options
    private String zoneId;
    private String clusterId;

    // Optional configuration for hosts - useful for the Bigtable team, more than anything else.
    private String dataHost = BIGTABLE_DATA_HOST_DEFAULT;
    private String tableAdminHost = BIGTABLE_TABLE_ADMIN_HOST_DEFAULT;
    private String instanceAdminHost = BIGTABLE_INSTANCE_ADMIN_HOST_DEFAULT;
    private int port = BIGTABLE_PORT_DEFAULT;

    private int dataChannelCount = BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT;

    private BulkOptions bulkOptions;
    private boolean usePlaintextNegotiation = false;
    private boolean useCachedDataPool = false;

    private RetryOptions retryOptions = new RetryOptions.Builder().build();
    private CallOptionsConfig callOptionsConfig = new CallOptionsConfig.Builder().build();
    // CredentialOptions.defaultCredentials() gets credentials from well known locations, such as
    // the Google Compute Engine metadata service or gcloud configuration in other environments. A
    // user can also override the default behavior with P12 or JSON configuration.
    private CredentialOptions credentialOptions = CredentialOptions.defaultCredentials();

    public Builder() {
    }

    private Builder(BigtableOptions original) {
      this.projectId = original.projectId;
      this.instanceId = original.instanceId;
      this.userAgent = original.userAgent;
      this.clusterId = original.clusterId;
      this.zoneId = original.zoneId;
      this.dataHost = original.dataHost;
      this.tableAdminHost = original.tableAdminHost;
      this.instanceAdminHost = original.instanceAdminHost;
      this.port = original.port;
      this.credentialOptions = original.credentialOptions;
      this.retryOptions = original.retryOptions;
      this.dataChannelCount = original.dataChannelCount;
      this.bulkOptions = original.bulkOptions;
      this.usePlaintextNegotiation = original.usePlaintextNegotiation;
      this.callOptionsConfig = original.callOptionsConfig;
    }

    public Builder setTableAdminHost(String tableAdminHost) {
      this.tableAdminHost = tableAdminHost;
      return this;
    }

    public Builder setInstanceAdminHost(String instanceAdminHost) {
      this.instanceAdminHost = instanceAdminHost;
      return this;
    }

    public Builder setDataHost(String dataHost) {
      this.dataHost = dataHost;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder setInstanceId(String instanceId) {
      this.instanceId = instanceId;
      return this;
    }

    public Builder setCredentialOptions(CredentialOptions credentialOptions) {
      this.credentialOptions = credentialOptions;
      return this;
    }

    public Builder setUserAgent(String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    public Builder setClusterId(String clusterId) {
      this.clusterId = clusterId;
      return this;
    }

    public Builder setZoneId(String zoneId) {
      this.zoneId = zoneId;
      return this;
    }

    public Builder setDataChannelCount(int dataChannelCount) {
      this.dataChannelCount = dataChannelCount;
      return this;
    }

    public int getDataChannelCount() {
      return dataChannelCount;
    }

    public Builder setRetryOptions(RetryOptions retryOptions) {
      this.retryOptions = retryOptions;
      return this;
    }

    public Builder setBulkOptions(BulkOptions bulkOptions) {
      this.bulkOptions = bulkOptions;
      return this;
    }

    public Builder setUsePlaintextNegotiation(boolean usePlaintextNegotiation) {
      this.usePlaintextNegotiation = usePlaintextNegotiation;
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
      this.useCachedDataPool = useCachedDataPool;
      return this;
    }

    public Builder setCallOptionsConfig(CallOptionsConfig callOptionsConfig) {
      this.callOptionsConfig = callOptionsConfig;
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

      String[] hostPort = emulatorHost.split(":");
      Preconditions.checkArgument(hostPort.length == 2,
          "Malformed " + BIGTABLE_EMULATOR_HOST_ENV_VAR + " environment variable: " +
          emulatorHost + ". Expecting host:port.");

      int port;
      try {
        port = Integer.parseInt(hostPort[1]);
      } catch (NumberFormatException e) {
        throw new RuntimeException("Invalid port in " + BIGTABLE_EMULATOR_HOST_ENV_VAR +
            " environment variable: " + emulatorHost);
      }
      setUsePlaintextNegotiation(true);
      setCredentialOptions(CredentialOptions.nullCredential());
      setDataHost(hostPort[0]);
      setTableAdminHost(hostPort[0]);
      setInstanceAdminHost(hostPort[0]);
      setPort(port);

      LOG.info("Connecting to the Bigtable emulator at " + emulatorHost);
    }

    public BigtableOptions build() {
      if (bulkOptions == null) {
        int maxInflightRpcs =
            BulkOptions.BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT * dataChannelCount;
        bulkOptions = new BulkOptions.Builder().setMaxInflightRpcs(maxInflightRpcs).build();
      } else if (bulkOptions.getMaxInflightRpcs() <= 0) {
        int maxInflightRpcs =
            BulkOptions.BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT * dataChannelCount;
        bulkOptions = bulkOptions.toBuilder().setMaxInflightRpcs(maxInflightRpcs).build();
      }
      applyEmulatorEnvironment();
      return new BigtableOptions(
          instanceAdminHost,
          tableAdminHost,
          dataHost,
          port,
          projectId,
          instanceId,
          userAgent,
          zoneId,
          clusterId,
          usePlaintextNegotiation,
          useCachedDataPool,
          dataChannelCount,
          bulkOptions,
          callOptionsConfig,
          credentialOptions,
          retryOptions);
    }
  }

  private final String instanceAdminHost;
  private final String tableAdminHost;
  private final String dataHost;
  private final int port;
  private final String projectId;
  private final String instanceId;
  private final String userAgent;
  private final String zoneId;
  private final String clusterId;
  private final int dataChannelCount;
  private final boolean usePlaintextNegotiation;
  private final boolean useCachedDataPool;

  private final BigtableInstanceName instanceName;

  private final BulkOptions bulkOptions;
  private final CallOptionsConfig callOptionsConfig;
  private final CredentialOptions credentialOptions;
  private final RetryOptions retryOptions;

  @VisibleForTesting
  BigtableOptions() {
      instanceAdminHost = null;
      tableAdminHost = null;
      dataHost = null;
      port = 0;
      projectId = null;
      instanceId = null;
      userAgent = null;
      clusterId = null;
      zoneId = null;
      dataChannelCount = 1;
      instanceName = null;
      usePlaintextNegotiation = false;
      useCachedDataPool = false;

      bulkOptions = null;
      callOptionsConfig = null;
      credentialOptions = null;
      retryOptions = null;
  }

  private BigtableOptions(
      String instanceAdminHost,
      String tableAdminHost,
      String dataHost,
      int port,
      String projectId,
      String instanceId,
      String userAgent,
      String zoneId,
      String clusterId,
      boolean usePlaintextNegotiation,
      boolean useCachedChannel,
      int channelCount,
      BulkOptions bulkOptions,
      CallOptionsConfig callOptionsConfig,
      CredentialOptions credentialOptions,
      RetryOptions retryOptions) {
    Preconditions.checkArgument(channelCount > 0, "Channel count has to be at least 1.");
    Preconditions.checkArgument((Strings.isNullOrEmpty(clusterId)) == (Strings.isNullOrEmpty(zoneId)),
        "clusterId and zoneId must be specified as a pair.");

    this.tableAdminHost = Preconditions.checkNotNull(tableAdminHost);
    this.instanceAdminHost = Preconditions.checkNotNull(instanceAdminHost);
    this.dataHost = Preconditions.checkNotNull(dataHost);
    this.port = port;
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.credentialOptions = credentialOptions;
    this.userAgent = userAgent;
    this.zoneId = zoneId;
    this.clusterId = clusterId;
    this.retryOptions = retryOptions;
    this.dataChannelCount = channelCount;
    this.bulkOptions = bulkOptions;
    this.usePlaintextNegotiation = usePlaintextNegotiation;
    this.useCachedDataPool = useCachedChannel;
    this.callOptionsConfig = callOptionsConfig;

    if (!Strings.isNullOrEmpty(projectId)
        && !Strings.isNullOrEmpty(instanceId)) {
      this.instanceName = new BigtableInstanceName(projectId, instanceId);
    } else {
      this.instanceName = null;
    }

    LOG.debug("Connection Configuration: projectId: %s, instanceId: %s, data host %s, "
        + "table admin host %s, cluster admin host %s.",
        projectId,
        instanceId,
        dataHost,
        tableAdminHost,
        instanceAdminHost);

    if (!Strings.isNullOrEmpty(zoneId) || !Strings.isNullOrEmpty(clusterId)) {
      LOG.debug("Using legacy connection configuration: zoneId: %s, clusterId: %s.",
          zoneId, clusterId);
    }
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
  public String getTableAdminHost() {
    return tableAdminHost;
  }

  /**
   * <p>Getter for the field <code>instanceAdminHost</code>.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  public String getInstanceAdminHost() {
    return instanceAdminHost;
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
   * <p>Getter for the field <code>zoneId</code>.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  public String getZoneId() {
    return zoneId;
  }

  /**
   * <p>Getter for the field <code>clusterId</code>.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  public String getClusterId() {
    return clusterId;
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
        && Objects.equals(instanceAdminHost, other.instanceAdminHost)
        && Objects.equals(tableAdminHost, other.tableAdminHost)
        && Objects.equals(dataHost, other.dataHost)
        && Objects.equals(projectId, other.projectId)
        && Objects.equals(instanceId, other.instanceId)
        && Objects.equals(userAgent, other.userAgent)
        && Objects.equals(zoneId, other.zoneId)
        && Objects.equals(clusterId, other.clusterId)
        && Objects.equals(credentialOptions, other.credentialOptions)
        && Objects.equals(retryOptions, other.retryOptions)
        && Objects.equals(bulkOptions, other.bulkOptions)
        && Objects.equals(callOptionsConfig, other.callOptionsConfig);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("dataHost", dataHost)
        .add("tableAdminHost", tableAdminHost)
        .add("instanceAdminHost", instanceAdminHost)
        .add("projectId", projectId)
        .add("instanceId", instanceId)
        .add("userAgent", userAgent)
        .add("zoneId", zoneId)
        .add("clusterId", clusterId)
        .add("credentialType", credentialOptions.getCredentialType())
        .add("port", port)
        .add("dataChannelCount", dataChannelCount)
        .add("retryOptions", retryOptions)
        .add("bulkOptions", bulkOptions)
        .add("callOptionsConfig", callOptionsConfig)
        .add("usePlaintextNegotiation", usePlaintextNegotiation)
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
}
