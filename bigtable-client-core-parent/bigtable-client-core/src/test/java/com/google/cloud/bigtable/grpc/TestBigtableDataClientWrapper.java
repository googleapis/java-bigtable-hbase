/*
 * Copyright 2018 Google LLC.  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestBigtableDataClientWrapper {

  private static final String PROJECT_ID = "projectId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String TABLE_ID = "tableId";
  private static final String APP_PROFILE_ID = "appProfileId";
  private static final RequestContext REQUEST_CONTEXT =
      RequestContext.create(InstanceName.of(PROJECT_ID, INSTANCE_ID), APP_PROFILE_ID);
  private BigtableDataClient bigtableDataClient;
  private BigtableOptions options;
  private BigtableDataClientWrapper bigtableDataClientWrapper;

  @Before
  public void setUp() {
    bigtableDataClient = Mockito.mock(BigtableDataClient.class);
    options = BigtableOptions.builder().setProjectId(PROJECT_ID).setInstanceId(INSTANCE_ID)
        .setAppProfileId(APP_PROFILE_ID).build();
    bigtableDataClientWrapper = new BigtableDataClientWrapper(bigtableDataClient, options);
  }

  @Test
  public void testMutateRow() {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, "key");
    MutateRowRequest mutateRowRequest = rowMutation.toProto(REQUEST_CONTEXT);
    when(bigtableDataClient.mutateRow(mutateRowRequest))
        .thenReturn(MutateRowResponse.getDefaultInstance());
    bigtableDataClientWrapper.mutateRow(rowMutation);
    verify(bigtableDataClient).mutateRow(mutateRowRequest);
  }
}
