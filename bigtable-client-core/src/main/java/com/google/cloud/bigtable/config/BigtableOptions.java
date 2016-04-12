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

import com.google.api.client.util.Objects;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * An immutable class providing access to configuration options for Bigtable.
 */
//TODO: Perhaps break this down into smaller options objects?
public class BigtableOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String BIGTABLE_TABLE_ADMIN_HOST_DEFAULT =
      "bigtabletableadmin.googleapis.com";
  public static final String BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT =
      "bigtableclusteradmin.googleapis.com";
  public static final String BIGTABLE_DATA_HOST_DEFAULT = "bigtable.googleapis.com";
  public static final int BIGTABLE_PORT_DEFAULT = 443;

  public static final int BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT = getDefaultDataChannelCount();
  public static final int BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT = 2;

  /**
   * This describes the maximum size a bulk mutation RPC should be before sending it to the server
   * and starting the next bulk call. Defaults to 1 MB.
   */
  public static final long BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT = 1 << 20;

  /**
   * This describes the maximum number of individual mutation requests to bundle in a single bulk
   * mutation RPC before sending it to the server and starting the next bulk call.
   * The server has a maximum of 100,000.  Since RPCs can be retried, we should limit the number of
   * keys to 100 by default so we don't keep retrying larger batches.
   */
  public static final int BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT = 100;

  private static final Logger LOG = new Logger(BigtableOptions.class);

  private static int getDefaultDataChannelCount() {
    // 10 Channels seemed to work well on a 4 CPU machine, and this seems to scale well for higher
    // CPU machines. Use no more than 250 Channels by default.
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    return (int) Math.min(250, Math.max(1, Math.ceil(availableProcessors * 2.5d)));
  }

  /**
   * A mutable builder for BigtableConnectionOptions.
   */
  public static class Builder {
    // Configuration that a user is required to set.
    private String projectId;
    private String zoneId;
    private String clusterId;
    private String userAgent;

    // Optional configuration for hosts - useful for the Bigtable team, more than anything else.
    private String dataHost = BIGTABLE_DATA_HOST_DEFAULT;
    private String tableAdminHost = BIGTABLE_TABLE_ADMIN_HOST_DEFAULT;
    private String clusterAdminHost = BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT;
    private int port = BIGTABLE_PORT_DEFAULT;

    // The default credentials get credential from well known locations, such as the GCE
    // metdata service or gcloud configuration in other environments. A user can also override
    // the default behavior with P12 or JSon configuration.
    private CredentialOptions credentialOptions = CredentialOptions.defaultCredentials();

    // Performance tuning options.
    private RetryOptions retryOptions = new RetryOptions.Builder().build();
    private int dataChannelCount = BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT;
    private int asyncMutatorCount = BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT;

    private boolean useBulkApi = false;
    private int bulkMaxRowKeyCount = BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT;
    private long bulkMaxRequestSize = BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT;
    private boolean usePlaintextNegotiation = false;

    public Builder() {
    }

    private Builder(BigtableOptions original) {
      this.projectId = original.projectId;
      this.zoneId = original.zoneId;
      this.clusterId = original.clusterId;
      this.userAgent = original.userAgent;
      this.dataHost = original.dataHost;
      this.tableAdminHost = original.tableAdminHost;
      this.clusterAdminHost = original.clusterAdminHost;
      this.port = original.port;
      this.credentialOptions = original.credentialOptions;
      this.retryOptions = original.retryOptions;
      this.dataChannelCount = original.dataChannelCount;
      this.asyncMutatorCount = original.asyncMutatorCount;
      this.useBulkApi = original.useBulkApi;
      this.bulkMaxRowKeyCount = original.bulkMaxRowKeyCount;
      this.bulkMaxRequestSize = original.bulkMaxRequestSize;
      this.usePlaintextNegotiation = original.usePlaintextNegotiation;
    }

    public Builder setTableAdminHost(String tableAdminHost) {
      this.tableAdminHost = tableAdminHost;
      return this;
    }

    public Builder setClusterAdminHost(String clusterAdminHost) {
      this.clusterAdminHost = clusterAdminHost;
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

    public Builder setZoneId(String zoneId) {
      this.zoneId = zoneId;
      return this;
    }

    public Builder setClusterId(String clusterId) {
      this.clusterId = clusterId;
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

    public Builder setDataChannelCount(int dataChannelCount) {
      this.dataChannelCount = dataChannelCount;
      return this;
    }

    public Builder setRetryOptions(RetryOptions retryOptions) {
      this.retryOptions = retryOptions;
      return this;
    }

    public Builder setAsyncMutatorWorkerCount(int asyncMutatorCount) {
      Preconditions.checkArgument(
          asyncMutatorCount >= 0, "asyncMutatorCount must be greater or equal to 0.");
      this.asyncMutatorCount = asyncMutatorCount;
      return this;
    }

    public Builder setUseBulkApi(boolean useBulkApi) {
      this.useBulkApi = useBulkApi;
      return this;
    }

    public Builder setBulkMaxRowKeyCount(int bulkMaxRowKeyCount) {
      Preconditions.checkArgument(
        bulkMaxRowKeyCount >= 0, "bulkMaxRowKeyCount must be greater or equal to 0.");
      this.bulkMaxRowKeyCount = bulkMaxRowKeyCount;
      return this;
    }

    public Builder setBulkMaxRequestSize(long bulkMaxRequestSize) {
      Preconditions.checkArgument(
        bulkMaxRequestSize >= 0, "bulkMaxRequestSize must be greater or equal to 0.");
      this.bulkMaxRequestSize = bulkMaxRequestSize;
      return this;
    }

    public Builder setUsePlaintextNegotiation(boolean usePlaintextNegotiation) {
      this.usePlaintextNegotiation = usePlaintextNegotiation;
      return this;
    }

    public BigtableOptions build() {
      return new BigtableOptions(
          clusterAdminHost,
          tableAdminHost,
          dataHost,
          port,
          projectId,
          zoneId,
          clusterId,
          credentialOptions,
          userAgent,
          retryOptions,
          dataChannelCount,
          asyncMutatorCount,
          useBulkApi,
          bulkMaxRowKeyCount,
          bulkMaxRequestSize,
          usePlaintextNegotiation);
    }
  }

  private final String clusterAdminHost;
  private final String tableAdminHost;
  private final String dataHost;
  private final int port;
  private final String projectId;
  private final String zoneId;
  private final String clusterId;
  private final CredentialOptions credentialOptions;
  private final String userAgent;
  private final RetryOptions retryOptions;
  private final int dataChannelCount;
  private final BigtableClusterName clusterName;
  private final int asyncMutatorCount;
  private final boolean useBulkApi;
  private final int bulkMaxRowKeyCount;
  private final long bulkMaxRequestSize;
  private final boolean usePlaintextNegotiation;


  @VisibleForTesting
  BigtableOptions() {
      clusterAdminHost = null;
      tableAdminHost = null;
      dataHost = null;
      port = 0;
      projectId = null;
      zoneId = null;
      clusterId = null;
      credentialOptions = null;
      userAgent = null;
      retryOptions = null;
      dataChannelCount = 1;
      clusterName = null;
      asyncMutatorCount = 1;
      useBulkApi = false;
      bulkMaxRowKeyCount = -1;
      bulkMaxRequestSize = -1;
      usePlaintextNegotiation = false;
  }

  private BigtableOptions(
      String clusterAdminHost,
      String tableAdminHost,
      String dataHost,
      int port,
      String projectId,
      String zoneId,
      String clusterId,
      CredentialOptions credentialOptions,
      String userAgent,
      RetryOptions retryOptions,
      int channelCount,
      int asyncMutatorCount,
      boolean useBulkApi,
      int bulkMaxKeyCount,
      long bulkMaxRequestSize,
      boolean usePlaintextNegotiation) {
    Preconditions.checkArgument(channelCount > 0, "Channel count has to be at least 1.");

    this.tableAdminHost = Preconditions.checkNotNull(tableAdminHost);
    this.clusterAdminHost = Preconditions.checkNotNull(clusterAdminHost);
    this.dataHost = Preconditions.checkNotNull(dataHost);
    this.port = port;
    this.projectId = projectId;
    this.zoneId = zoneId;
    this.clusterId = clusterId;
    this.credentialOptions = credentialOptions;
    this.userAgent = userAgent;
    this.retryOptions = retryOptions;
    this.dataChannelCount = channelCount;
    this.asyncMutatorCount = asyncMutatorCount;
    this.useBulkApi = useBulkApi;
    this.bulkMaxRowKeyCount = bulkMaxKeyCount;
    this.bulkMaxRequestSize = bulkMaxRequestSize;
    this.usePlaintextNegotiation = usePlaintextNegotiation;

    if (!Strings.isNullOrEmpty(projectId)
        && !Strings.isNullOrEmpty(zoneId)
        && !Strings.isNullOrEmpty(clusterId)) {
      this.clusterName = new BigtableClusterName(getProjectId(), getZoneId(), getClusterId());
    } else {
      this.clusterName = null;
    }

    LOG.debug("Connection Configuration: projectId: %s, zoneId: %s, clusterId: %s, data host %s, "
        + "table admin host %s, cluster admin host %s.",
        projectId,
        zoneId,
        clusterId,
        dataHost,
        tableAdminHost,
        clusterAdminHost);
  }

  public String getProjectId() {
    return projectId;
  }

  public String getZoneId() {
    return zoneId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getDataHost() {
    return dataHost;
  }

  public String getTableAdminHost() {
    return tableAdminHost;
  }

  public String getClusterAdminHost() {
    return clusterAdminHost;
  }

  public int getPort() {
    return port;
  }

  /**
   * Get the credential this object was constructed with. May be null.
   * @return Null to indicate no credentials, otherwise, the Credentials object.
   */
  public CredentialOptions getCredentialOptions() {
    return credentialOptions;
  }

  /**
   * Gets the user-agent to be appended to User-Agent header when creating new streams
   * for the channel.
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * Options controlling retries.
   */
  public RetryOptions getRetryOptions() {
    return retryOptions;
  }

  /**
   * The number of data channels to create.
   */
  public int getChannelCount() {
    return dataChannelCount;
  }

  public BigtableClusterName getClusterName() {
    return clusterName;
  }

  public int getAsyncMutatorCount() {
    return asyncMutatorCount;
  }

  public boolean useBulkApi() {
    return useBulkApi;
  }

  public int getBulkMaxRowKeyCount() {
    return bulkMaxRowKeyCount;
  }

  public long getBulkMaxRequestSize() {
    return bulkMaxRequestSize;
  }

  public boolean usePlaintextNegotiation() {
    return usePlaintextNegotiation;
  }

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
        && (asyncMutatorCount == other.asyncMutatorCount)
        && (useBulkApi == other.useBulkApi)
        && (bulkMaxRowKeyCount == other.bulkMaxRowKeyCount)
        && (bulkMaxRequestSize == other.bulkMaxRequestSize)
        && (usePlaintextNegotiation == other.usePlaintextNegotiation)
        && Objects.equal(clusterAdminHost, other.clusterAdminHost)
        && Objects.equal(tableAdminHost, other.tableAdminHost)
        && Objects.equal(dataHost, other.dataHost)
        && Objects.equal(projectId, other.projectId)
        && Objects.equal(zoneId, other.zoneId)
        && Objects.equal(clusterId, other.clusterId)
        && Objects.equal(userAgent, other.userAgent)
        && Objects.equal(credentialOptions, other.credentialOptions)
        && Objects.equal(retryOptions, other.retryOptions);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("dataHost", dataHost)
        .add("tableAdminHost", tableAdminHost)
        .add("clusterAdminHost", clusterAdminHost)
        .add("projectId", projectId)
        .add("zoneId", zoneId)
        .add("clusterId", clusterId)
        .add("userAgent", userAgent)
        .add("credentialType", credentialOptions.getCredentialType())
        .add("port", port)
        .add("dataChannelCount", dataChannelCount)
        .add("asyncMutatorCount", asyncMutatorCount)
        .add("useBulkApi", useBulkApi)
        .add("bulkMaxKeyCount", bulkMaxRowKeyCount)
        .add("bulkMaxRequestSize", bulkMaxRequestSize)
        .add("usePlaintextNegotiation", usePlaintextNegotiation)
        .toString();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }
}
