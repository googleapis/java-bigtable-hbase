/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.bigtable.batch.common;

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableHBaseVeneerSettings;
import java.io.IOException;
import java.util.List;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class CloudBigtableServiceImpl implements CloudBigtableService {
  @Override
  public List<KeyOffset> getSampleRowKeys(CloudBigtableTableConfiguration config)
      throws IOException {

    // TODO: figure out how to stick to HBase api here
    BigtableHBaseVeneerSettings settings =
        (BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(config.toHBaseConfig());
    try (BigtableDataClient client = BigtableDataClient.create(settings.getDataSettings())) {
      return client.sampleRowKeys(config.getTableId());
    }
  }
}
