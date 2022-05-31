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
package com.google.cloud.bigtable.mirroring.core;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.ResourceReservation;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoException;
import org.mockito.invocation.Invocation;
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

  public static Delete createDelete(String row) {
    return new Delete(row.getBytes());
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

  public static IOException setupFlowControllerToRejectRequests(FlowController flowController) {
    IOException thrownException = new IOException("flow control expected exception");
    SettableFuture<ResourceReservation> resourceReservationFuture = SettableFuture.create();
    resourceReservationFuture.setException(thrownException);

    lenient()
        .doReturn(resourceReservationFuture)
        .when(flowController)
        .asyncRequestResource(any(RequestResourcesDescription.class));
    return thrownException;
  }

  /**
   * A helper function that blocks method on a mock until a future is set or default timeout is
   * reached.
   *
   * <p>Once unblocked by {@link SettableFuture#set} the call is unblocked and let through.
   *
   * <p>When timeout is reached a TimeoutException is thrown and blocked method is never called.
   *
   * @param mock mock whose method is blocked
   * @param futureToWaitFor future which unblocks method calls
   * @param before action to be called before waiting for {@param futureToWaitFor}
   * @param <T> - type of {@param mock}
   * @return {@param mock}
   */
  public static <T> T blockMethodCall(
      T mock, final SettableFuture<Void> futureToWaitFor, final Runnable before) {
    return doAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                before.run();
                futureToWaitFor.get(10, TimeUnit.SECONDS);
                try {
                  return invocationOnMock.callRealMethod();
                } catch (MockitoException e) {
                  // there was no real method to call, ignore.
                  return null;
                }
              }
            })
        .when(mock);
  }

  public static <T> T blockMethodCall(
      T mock, final SettableFuture<Void> secondaryOperationAllowedFuture) {
    return blockMethodCall(
        mock,
        secondaryOperationAllowedFuture,
        new Runnable() {
          @Override
          public void run() {}
        });
  }

  public static <T> T blockMethodCall(
      T mock,
      final SettableFuture<Void> secondaryOperationAllowedFuture,
      final SettableFuture<Void> startedFuture) {
    return blockMethodCall(
        mock,
        secondaryOperationAllowedFuture,
        new Runnable() {
          @Override
          public void run() {
            startedFuture.set(null);
          }
        });
  }

  public static <T> T blockMethodCall(
      T mock,
      final SettableFuture<Void> secondaryOperationAllowedFuture,
      final Semaphore startedSemaphore) {
    return blockMethodCall(
        mock,
        secondaryOperationAllowedFuture,
        new Runnable() {
          @Override
          public void run() {
            startedSemaphore.release();
          }
        });
  }

  public static <T> SettableFuture<Void> blockMethodCall(T methodCall) {
    final SettableFuture<Void> secondaryOperationAllowedFuture = SettableFuture.create();
    when(methodCall)
        .thenAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                secondaryOperationAllowedFuture.get(10, TimeUnit.SECONDS);
                try {
                  return invocationOnMock.callRealMethod();
                } catch (MockitoException e) {
                  // there was no real method to call, ignore.
                  return null;
                }
              }
            });
    return secondaryOperationAllowedFuture;
  }

  public static <T> T delayMethodCall(T mock, final int ms) {
    return doAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(ms);
                try {
                  return invocationOnMock.callRealMethod();
                } catch (MockitoException e) {
                  // there was no real method to call, ignore.
                  return null;
                }
              }
            })
        .when(mock);
  }

  public static <T> void waitUntilCalled(
      T mockitoObject, String methodName, int calls, int timeoutSeconds) throws TimeoutException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (stopwatch.elapsed(TimeUnit.SECONDS) < timeoutSeconds) {
      Collection<Invocation> invocations = Mockito.mockingDetails(mockitoObject).getInvocations();
      long count = 0;
      for (Invocation invocation : invocations) {
        if (invocation.getMethod().getName().equals(methodName)) {
          count++;
        }
      }
      if (count >= calls) {
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new TimeoutException();
  }

  public static void assertPutsAreEqual(
      Put expectedPut, Put value, CellComparatorCompat cellComparator) {
    assertThat(expectedPut.getRow()).isEqualTo(value.getRow());
    assertThat(expectedPut.getFamilyCellMap().size()).isEqualTo(value.getFamilyCellMap().size());
    for (byte[] family : expectedPut.getFamilyCellMap().keySet()) {
      assertThat(value.getFamilyCellMap()).containsKey(family);
      List<Cell> expectedCells = expectedPut.getFamilyCellMap().get(family);
      List<Cell> valueCells = value.getFamilyCellMap().get(family);
      assertThat(expectedCells.size()).isEqualTo(valueCells.size());
      for (int i = 0; i < expectedCells.size(); i++) {
        assertThat(cellComparator.compare(expectedCells.get(i), valueCells.get(i))).isEqualTo(0);
      }
    }
  }

  public interface CellComparatorCompat {
    int compare(Cell a, Cell b);
  }

  public static Map<Object, Object> mapOf(Object... keyValuePairs) {
    Preconditions.checkArgument(keyValuePairs.length % 2 == 0);
    Map<Object, Object> mapping = new HashMap<>();
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      mapping.put(keyValuePairs[i], keyValuePairs[i + 1]);
    }
    return mapping;
  }

  public static void mockBatch(Table table1, Table table2, Object... keyValuePairs)
      throws IOException, InterruptedException {
    mockBatch(table1, keyValuePairs);
    mockBatch(table2, keyValuePairs);
  }

  public static void mockBatch(Table table, Object... keyValuePairs)
      throws IOException, InterruptedException {

    lenient()
        .doAnswer(createMockBatchAnswer(keyValuePairs))
        .when(table)
        .batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  /**
   * Function used to mock Table.batch(operations, results) call by filling the result Array.
   *
   * <p>For objects in {@param keyValuePairs} returns a provided value, otherwise constructs a
   * default one.
   *
   * <p>Throws iff any of the values returned to caller of batch is a Throwable.
   *
   * @param keyValuePairs - key:value pairs of objects, key may be either operation or operation
   *     class
   * @return {@link Answer} for use in {@link org.mockito.stubbing.BaseStubber#doAnswer(Answer)}
   */
  public static Answer<Void> createMockBatchAnswer(final Object... keyValuePairs) {
    final Map<Object, Object> mapping = mapOf(keyValuePairs);

    return new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] args = invocationOnMock.getArguments();
        List<? extends Row> operations = (List<? extends Row>) args[0];
        Object[] result = (Object[]) args[1];

        List<Throwable> exceptions = new ArrayList<>();
        List<Row> failedOps = new ArrayList<>();
        List<String> hostnameAndPorts = new ArrayList<>();

        for (int i = 0; i < operations.size(); i++) {
          Row operation = operations.get(i);
          if (mapping.containsKey(operation) || mapping.containsKey(operation.getClass())) {
            Object value;
            if (mapping.containsKey(operation)) {
              value = mapping.get(operation);
            } else {
              value = mapping.get(operation.getClass());
            }
            result[i] = value;
            if (value instanceof Throwable) {
              failedOps.add(operation);
              exceptions.add((Throwable) value);
              hostnameAndPorts.add("test:1");
            }
          } else if (operation instanceof Get) {
            Get get = (Get) operation;
            result[i] = createResult(get.getRow(), get.getRow());
          } else {
            result[i] = Result.create(new Cell[0]);
          }
        }
        if (!failedOps.isEmpty()) {
          throw new RetriesExhaustedWithDetailsException(exceptions, failedOps, hostnameAndPorts);
        }
        return null;
      }
    };
  }
}
