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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

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
    ArrayList<Cell> cells = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      cells.add(CellUtil.createCell(key.getBytes(), values[i].getBytes()));
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
}
