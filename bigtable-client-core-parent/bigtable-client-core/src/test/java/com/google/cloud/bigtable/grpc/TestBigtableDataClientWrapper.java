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

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestBigtableDataClientWrapper {

  private static final String PROJECT_ID = "projectId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String TABLE_ID = "tableId";
  private static final String APP_PROFILE_ID = "appProfileId";
  private static final RequestContext REQUEST_CONTEXT =
      RequestContext.create(InstanceName.of(PROJECT_ID, INSTANCE_ID), APP_PROFILE_ID);

  private BigtableOptions options;
  private BigtableDataClientWrapper clientWrapper;
  @Mock
  private BigtableDataClient client;

  @Mock
  private ListenableFuture<MutateRowResponse> mockMutateRowAsync;

  @Before
  public void setUp() {
    options = BigtableOptions.builder().setProjectId(PROJECT_ID).setInstanceId(INSTANCE_ID)
        .setAppProfileId(APP_PROFILE_ID).build();
    clientWrapper = new BigtableDataClientWrapper(client, options);
  }

  @Test
  public void testMutateRow() {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, "key");
    MutateRowRequest mutateRowRequest = rowMutation.toProto(REQUEST_CONTEXT);
    when(client.mutateRow(mutateRowRequest))
        .thenReturn(MutateRowResponse.getDefaultInstance());
    clientWrapper.mutateRow(rowMutation);
    verify(client).mutateRow(mutateRowRequest);
  }

  @Test
  public void testMutateRowAsync() {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, "key");
    MutateRowRequest request = rowMutation.toProto(REQUEST_CONTEXT);
    when(client.mutateRowAsync(request)).thenReturn(mockMutateRowAsync);
    clientWrapper.mutateRowAsync(rowMutation);
    verify(client).mutateRowAsync(request);
  }

  @Test
  public void testTrueMutationRow(){
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").then(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response = CheckAndMutateRowResponse.newBuilder()
        .setPredicateMatched(true).build();
    when(client.checkAndMutateRow(request)).thenReturn(response);
    Boolean actual = clientWrapper.checkAndMutateRow(conditonalMutation);
    verify(client).checkAndMutateRow(request);
    Assert.assertTrue(actual);
  }

  @Test
  public void testFalseMutationRow(){
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").otherwise(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response = CheckAndMutateRowResponse.newBuilder()
        .setPredicateMatched(false).build();
    when(client.checkAndMutateRow(request)).thenReturn(response);
    Boolean actual = clientWrapper.checkAndMutateRow(conditonalMutation);
    verify(client).checkAndMutateRow(request);
    Assert.assertTrue(actual);
  }

  @Test
  public void testTrueMutationRowAsync() throws Exception{
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").then(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);

    CheckAndMutateRowResponse response = CheckAndMutateRowResponse.newBuilder()
        .setPredicateMatched(true).build();
    ListenableFuture<CheckAndMutateRowResponse> future = Futures.immediateFuture(response);

    when(client.checkAndMutateRowAsync(request)).thenReturn(future);
    ListenableFuture<Boolean> actual = clientWrapper.checkAndMutateRowAsync(conditonalMutation);
    verify(client).checkAndMutateRowAsync(request);
    Assert.assertTrue(actual.get());
  }

  @Test
  public void testFalseMutationRowAsync() throws Exception{
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").otherwise(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response = CheckAndMutateRowResponse.newBuilder()
        .setPredicateMatched(false).build();
    ListenableFuture<CheckAndMutateRowResponse> future = Futures.immediateFuture(response);
    when(client.checkAndMutateRowAsync(request)).thenReturn(future);
    ListenableFuture<Boolean> actual = clientWrapper.checkAndMutateRowAsync(conditonalMutation);
    verify(client).checkAndMutateRowAsync(request);
    Assert.assertTrue(actual.get());
  }
}
