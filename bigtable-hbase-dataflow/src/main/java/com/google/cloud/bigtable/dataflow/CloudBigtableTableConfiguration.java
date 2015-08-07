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


/**
 * This class encapsulates the metadata required to create a connection to a Bigtable cluster,
 * and a specific table therein.
 */
public class CloudBigtableTableConfiguration extends CloudBigtableConfiguration {

  private static final long serialVersionUID = 2435897354284600685L;

  public static CloudBigtableTableConfiguration fromCBTOptions(CloudBigtableOptions options){
    return new CloudBigtableTableConfiguration(
        options.getBigtableProjectId(),
        options.getBigtableZoneId(),
        options.getBigtableClusterId(),
        options.getBigtableTableId());
  }

  @SuppressWarnings("unchecked")
  public static class Builder<T extends Builder<?>> extends CloudBigtableConfiguration.Builder<T> {
    protected String tableId;

    public T withTableId(String tableId) {
      this.tableId = tableId;
      return (T) this;
    }

    public CloudBigtableTableConfiguration build() {
      return new CloudBigtableTableConfiguration(projectId, zoneId, clusterId, tableId);
    }
  }

  protected String tableId;

  // This is required for serialization of CloudBigtableScanConfiguration.
  CloudBigtableTableConfiguration() {
  }

  public CloudBigtableTableConfiguration(String projectId, String zoneId, String clusterId,
      String tableId) {
    super(projectId, zoneId, clusterId);
    this.tableId = tableId;
  }

  public String getTableId() {
    return tableId;
  }
}
