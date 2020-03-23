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
package com.google.cloud.bigtable.hbase.wrappers;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.wrappers.classic.BigtableClassicApi;
import com.google.cloud.bigtable.hbase.wrappers.classic.BigtableHBaseClassicSettings;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableHBaseVeneerSettings;
import java.io.IOException;

/**
 * Common API surface for settings, data and admin client across this library.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public abstract class BigtableApi implements AutoCloseable {

  private final BigtableHBaseSettings hBaseSettings;

  public static BigtableApi create(BigtableHBaseSettings settings) throws IOException {
    if (settings instanceof BigtableHBaseVeneerSettings) {
      throw new UnsupportedOperationException("Veneer client is not yet supported.");
    } else {
      return new BigtableClassicApi((BigtableHBaseClassicSettings) settings);
    }
  }

  protected BigtableApi(BigtableHBaseSettings hbaseSettings) {
    this.hBaseSettings = hbaseSettings;
  }

  public abstract AdminClientWrapper getAdminClient();

  public abstract DataClientWrapper getDataClient();

  public BigtableHBaseSettings getBigtableHBaseSettings() {
    return hBaseSettings;
  }
}
