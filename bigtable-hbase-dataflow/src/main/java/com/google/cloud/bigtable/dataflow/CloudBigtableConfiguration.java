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
package com.google.cloud.bigtable.dataflow;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

/**
 * This class defines all of the information Cloud Bigtable client needs to connect to a user's
 * Cloud Bigtable cluster.
 */
public class CloudBigtableConfiguration implements Serializable {

  private static final long serialVersionUID = 1655181275627002133L;

  @SuppressWarnings("unchecked")
  public static class Builder<T extends Builder<?>> {
    protected String projectId;
    protected String zoneId;
    protected String clusterId;

    public T withProjectId(String projectId) {
      this.projectId = projectId;
      return (T) this;
    }

    public T withZoneId(String zoneId) {
      this.zoneId = zoneId;
      return (T) this;
    }

    public T withClusterId(String clusterId) {
      this.clusterId = clusterId;
      return (T) this;
    }

    public CloudBigtableConfiguration build() {
      return new CloudBigtableConfiguration(projectId, zoneId, clusterId);
    }
  }

  // Not final due to serialization of CloudBigtableScanConfiguration.
  protected String projectId;
  protected String zoneId;
  protected String clusterId;

  // Used for serialization of CloudBigtableScanConfiguration.
  CloudBigtableConfiguration() {
  }

  public CloudBigtableConfiguration(String projectId, String zoneId, String clusterId) {
    this.projectId = projectId;
    this.zoneId = zoneId;
    this.clusterId = clusterId;
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

  public BigtableOptions toBigtableOptions() {
    return new BigtableOptions.Builder()
      .setProjectId(getProjectId())
      .setZoneId(getZoneId())
      .setClusterId(getClusterId())
      .setUserAgent("CloudBigtableDataflow")
      .build();
  }

  public Configuration toHBaseConfig() {
    Configuration config = new Configuration();
    config.set(BigtableOptionsFactory.PROJECT_ID_KEY, getProjectId());
    config.set(BigtableOptionsFactory.ZONE_KEY, getZoneId());
    config.set(BigtableOptionsFactory.CLUSTER_KEY, getClusterId());
    return config;
  }
}
