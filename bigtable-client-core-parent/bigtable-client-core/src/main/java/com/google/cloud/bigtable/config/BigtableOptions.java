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
import com.google.auto.value.AutoValue;

/**
 * An immutable class providing access to configuration options for Bigtable.
 *
 * @author sduskis
 * @version $Id: $Id
 */
@AutoValue
public abstract class BigtableOptions implements Serializable, Cloneable {

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
  @Nullable
  public abstract String appProfileId();
  @Nullable
  public abstract String adminHost();
  @Nullable
  public abstract String dataHost();

  public abstract int port();
  @Nullable
  public abstract String projectId();
  @Nullable
  public abstract String instanceId();
  @Nullable
  public abstract String userAgent();

  public abstract int dataChannelCount();

  public abstract boolean usePlaintextNegotiation();

  public abstract boolean useCachedDataPool();
  @Nullable
  public abstract BigtableInstanceName instanceName();
  @Nullable
  public abstract BulkOptions bulkOptions();
  @Nullable
  public abstract CallOptionsConfig callOptionsConfig();
  @Nullable
  public abstract CredentialOptions credentialOptions();
  @Nullable
  public abstract RetryOptions retryOptions();
  @Nullable
  public abstract BigtableOptions bigtableOptions();

  public BigtableOptions withProjectId(String projectId) {
    return toBuilder().setProjectId(projectId).build();
  }
  public abstract Builder toBuilder();

  private static int getDefaultDataChannelCount() {
    // 20 Channels seemed to work well on a 4 CPU machine, and this ratio seems to scale well for
    // higher CPU machines. Use no more than 250 Channels by default.
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    return (int) Math.min(250, Math.max(1, availableProcessors * 4));
  }

  /**
   * A mutable builder for BigtableConnectionOptions.
   */
  @AutoValue.Builder
  public abstract static class Builder implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract Builder setAppProfileId(String appProfileId);
    public abstract Builder setAdminHost(String adminHost);
    public abstract Builder setDataHost(String dataHost);
    public abstract Builder setPort(int port);
    public abstract Builder setProjectId(String projectId);
    public abstract Builder setInstanceId(String instanceId);

    public abstract Builder setUserAgent(String userAgent);
    public abstract Builder setDataChannelCount(int dataChannelCount);
    public abstract Builder setUsePlaintextNegotiation(
        boolean usePlaintextNegotiation);
    public abstract Builder setUseCachedDataPool(boolean useCachedDataPool);

    public abstract Builder setInstanceName(BigtableInstanceName instanceName);

    public abstract Builder setBulkOptions(BulkOptions bulkOptions);
    public abstract Builder setCallOptionsConfig(
        CallOptionsConfig callOptionsConfig);
    public abstract Builder setCredentialOptions(
        CredentialOptions credentialOptions);
    public abstract Builder setRetryOptions(RetryOptions retryOptions);

    public abstract Builder setBigtableOptions(BigtableOptions options);

    abstract BigtableOptions autoBuild();
    BigtableOptions options;

    public BigtableOptions build() {
      applyEmulatorEnvironment(); 
      options = this.autoBuild();
      if (options.bulkOptions() == null) {
        int maxInflightRpcs = BulkOptions.BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT
            * options.dataChannelCount();
        setBulkOptions(new BulkOptions.Builder()
            .setMaxInflightRpcs(maxInflightRpcs).build());
      } else if (options.bulkOptions().getMaxInflightRpcs() <= 0) {
        int maxInflightRpcs = BulkOptions.BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT
            * options.dataChannelCount();
        setBulkOptions(options.bulkOptions().toBuilder()
            .setMaxInflightRpcs(maxInflightRpcs).build());
      }
      setAdminHost(Preconditions.checkNotNull(options.adminHost()));
      setDataHost(Preconditions.checkNotNull(options.dataHost()));
      if (!Strings.isNullOrEmpty(options.projectId())
          && !Strings.isNullOrEmpty(options.instanceId())) {
        setInstanceName(new BigtableInstanceName(options.projectId(),
            options.instanceId()));
      } else {
        setInstanceName(null);
      }
      
      LOG.debug("Connection Configuration: projectId: %s, instanceId: %s, data host %s, "
              + "admin host %s.",
          options.projectId(),
          options.instanceId(),
          options.dataHost(),
          options.adminHost());

      return options;
    }
    
    /**
     * Apply emulator settings from the relevant environment variable, if set.
     * @param builder 
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
      this.setUsePlaintextNegotiation(true);
      this.setCredentialOptions(CredentialOptions.nullCredential());
      this.setDataHost(host);
      this.setAdminHost(host);
      this.setPort(port);

      LOG.info("Connecting to the Bigtable emulator at " + host + ":" + port);
      return this;
    }
  }

  public static Builder Builder() {
    return new AutoValue_BigtableOptions.Builder()
        .setAppProfileId(BIGTABLE_APP_PROFILE_DEFAULT)
        .setDataHost(BIGTABLE_DATA_HOST_DEFAULT)
        .setAdminHost(BIGTABLE_ADMIN_HOST_DEFAULT)
        .setPort(BIGTABLE_PORT_DEFAULT)
        .setDataChannelCount(BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT)
        .setUsePlaintextNegotiation(false).setUseCachedDataPool(false)
        .setRetryOptions(new RetryOptions.Builder().build())
        .setCallOptionsConfig(new CallOptionsConfig.Builder().build())
        .setCredentialOptions(CredentialOptions.defaultCredentials());
  }

  

  @VisibleForTesting
  BigtableOptions() {
  }

  /**
   * The number of data channels to create.
   *
   * @return a int.
   */
  public int getChannelCount() {
    return dataChannelCount();
  }

  /**
   * Experimental feature to allow situations with multiple connections to
   * optimize their startup time.
   * 
   * @return true if this feature should be turned on in
   *         {@link BigtableSession}.
   */
  public boolean useCachedChannel() {
    return useCachedDataPool();
  }

  protected BigtableOptions.Builder clone() {
    return toBuilder();
  }
  
  /**
   *  
   * @return
   */
  public String getAppPofileId() {
    return appProfileId();
  }
  
  /**
   * @return
   */
  public String getAdminHost() {
    return adminHost();
  }
  
  /**
   * @return
   */
  public String getDataHost() {
    return dataHost();
  }
  
  /**
   * @return
   */
  public int getPort() {
    return port();
  }
  
  /**
   * @return
   */
  public String getProjectId() {
    return projectId();
  }
  
  /**
   * @return
   */
  public String getInstanceId() {
    return instanceId();
  }
  
  /**
   * @return
   */
  public String getUserAgent() {
    return userAgent();
  }
  
  /**
   * @return
   */
  public int getDataChannelCount() {
    return dataChannelCount();
  }
  
  /**
   * @return
   */
  public boolean getUsePlaintextNegotiation() {
    return usePlaintextNegotiation();
  }
  
  /**
   * @return
   */
  public boolean getUseCachedDataPool() {
    return useCachedDataPool();
  }
  
  /**
   * @return
   */
  public BigtableInstanceName getInstanceName() {
    return instanceName();
  }

  /**
   * @return
   */
  public BulkOptions getBulkOptions() {
    return bulkOptions();
  }

  /**
   * @return
   */
  public CallOptionsConfig getCallOptionsConfig() {
    return callOptionsConfig();
  }

  /**
   * @return
   */
  public CredentialOptions getCredentialOptions() {
    return credentialOptions();
  }
  
  /**
   * @return
   */
  public RetryOptions getRetryOptions() {
    return retryOptions();
  }

  /**
   * @return
   */
  public BigtableOptions getBigtableOptions() {
    return bigtableOptions();
  }
}