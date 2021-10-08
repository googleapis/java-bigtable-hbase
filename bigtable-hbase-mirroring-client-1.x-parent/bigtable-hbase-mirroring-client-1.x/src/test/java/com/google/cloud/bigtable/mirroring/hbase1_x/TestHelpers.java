/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestHelpers {
  public static Put createPut(String row, String family, String qualifier, String value) {
    Put put = new Put(row.getBytes());
    put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
    return put;
  }

  public static Put createPut(
      String row, String family, String qualifier, long timestamp, String value) {
    Put put = new Put(row.getBytes());
    put.addColumn(family.getBytes(), qualifier.getBytes(), timestamp, value.getBytes());
    return put;
  }

  public static Result createResult(String key, String... values) {
    byte[][] valuesBytes = new byte[values.length][];
    for (int i = 0; i < values.length; i++) {
      valuesBytes[i] = values[i].getBytes();
    }
    return createResult(key.getBytes(), valuesBytes);
  }

  public static Result createResult(byte[] key, byte[]... values) {
    ArrayList<Cell> cells = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      cells.add(CellUtil.createCell(key, values[i]));
    }
    return Result.create(cells);
  }

  public static Result createResult(
      String row, String family, String qualifier, long timestamp, String value) {
    return Result.create(
        Arrays.asList(createCell(row, family, qualifier, timestamp, Type.Put, value)));
  }

  public static Result createResult(Cell... cells) {
    return Result.create(cells);
  }

  public static Get createGet(String key) {
    return new Get(key.getBytes());
  }

  public static List<Get> createGets(String... keys) {
    List<Get> result = new ArrayList<>();
    for (String key : keys) {
      result.add(createGet(key));
    }
    return result;
  }

  public static Cell createCell(
      String row, String family, String qualifier, long timestamp, Type type, String value) {
    return CellUtil.createCell(
        row.getBytes(),
        family.getBytes(),
        qualifier.getBytes(),
        timestamp,
        type.getCode(),
        value.getBytes());
  }

  public static ResourceReservation setupFlowControllerMock(FlowController flowController) {
    ResourceReservation resourceReservationMock = mock(ResourceReservation.class);

    SettableFuture<ResourceReservation> resourceReservationFuture = SettableFuture.create();
    resourceReservationFuture.set(resourceReservationMock);

    lenient()
        .doReturn(resourceReservationFuture)
        .when(flowController)
        .asyncRequestResource(any(RequestResourcesDescription.class));

    return resourceReservationMock;
  }

  public static void setupFlowControllerToRejectRequests(FlowController flowController) {
    SettableFuture<FlowController.ResourceReservation> resourceReservationFuture =
        SettableFuture.create();
    resourceReservationFuture.setException(new IOException("expected"));

    doReturn(resourceReservationFuture)
        .when(flowController)
        .asyncRequestResource(any(RequestResourcesDescription.class));
  }

  public static <T> T blockMethodCall(
      T table, final SettableFuture<Void> secondaryOperationAllowedFuture) {
    return doAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                secondaryOperationAllowedFuture.get(10, TimeUnit.SECONDS);
                return invocationOnMock.callRealMethod();
              }
            })
        .when(table);
  }

  public static <T> SettableFuture<Void> blockMethodCall(T methodCall) {
    final SettableFuture<Void> secondaryOperationAllowedFuture = SettableFuture.create();
    when(methodCall)
        .thenAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                secondaryOperationAllowedFuture.get(10, TimeUnit.SECONDS);
                return invocationOnMock.callRealMethod();
              }
            });
    return secondaryOperationAllowedFuture;
  }
}
