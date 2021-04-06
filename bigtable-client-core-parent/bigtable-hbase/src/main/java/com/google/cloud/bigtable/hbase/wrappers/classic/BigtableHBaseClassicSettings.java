/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableHBaseClassicSettings extends BigtableHBaseSettings {

  private final BigtableOptions bigtableOptions;

  public BigtableHBaseClassicSettings(Configuration configuration) throws IOException {
    super(configuration);
    this.bigtableOptions = BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Override
  public String getDataHost() {
    return bigtableOptions.getDataHost();
  }

  @Override
  public String getAdminHost() {
    return bigtableOptions.getAdminHost();
  }

  @Override
  public int getPort() {
    return bigtableOptions.getPort();
  }

  @Override
  public int getBulkMaxRowCount() {
    return bigtableOptions.getBulkOptions().getBulkMaxRowKeyCount();
  }

  @Override
  public long getBatchingMaxRequestSize() {
    return bigtableOptions.getBulkOptions().getMaxMemory();
  }

  @Override
  public boolean isRetriesWithoutTimestampAllowed() {
    return bigtableOptions.getRetryOptions().allowRetriesWithoutTimestamp();
  }

  public BigtableOptions getBigtableOptions() {
    return bigtableOptions;
  }
}
