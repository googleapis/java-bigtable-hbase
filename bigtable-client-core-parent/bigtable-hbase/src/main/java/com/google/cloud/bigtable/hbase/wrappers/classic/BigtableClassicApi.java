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
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import java.io.IOException;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableClassicApi extends BigtableApi {

  private final BigtableSession bigtableSession;
  private final DataClientWrapper dataClientWrapper;
  private final AdminClientWrapper adminClientWrapper;

  public BigtableClassicApi(BigtableHBaseClassicSettings settings) throws IOException {
    this(settings, new BigtableSession(settings.getBigtableOptions()));
  }

  public BigtableClassicApi(BigtableHBaseSettings settings, BigtableSession session)
      throws IOException {
    super(settings);
    this.bigtableSession = session;

    RequestContext requestContext =
        RequestContext.create(
            settings.getProjectId(),
            settings.getInstanceId(),
            session.getOptions().getAppProfileId());
    this.dataClientWrapper = new DataClientClassicApi(bigtableSession, requestContext);

    BigtableInstanceName instanceName =
        new BigtableInstanceName(settings.getProjectId(), settings.getInstanceId());
    this.adminClientWrapper =
        new AdminClientClassicApi(bigtableSession.getTableAdminClient(), instanceName);
  }

  @Override
  public AdminClientWrapper getAdminClient() {
    return adminClientWrapper;
  }

  @Override
  public DataClientWrapper getDataClient() {
    return dataClientWrapper;
  }

  @Override
  public void close() throws Exception {
    if (adminClientWrapper != null) {
      adminClientWrapper.close();
    }
    dataClientWrapper.close();
    bigtableSession.close();
  }
}
