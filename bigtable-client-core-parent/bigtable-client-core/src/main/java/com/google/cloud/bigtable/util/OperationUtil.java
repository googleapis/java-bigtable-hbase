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
package com.google.cloud.bigtable.util;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.common.primitives.Ints;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsGrpc;
import io.grpc.protobuf.StatusProto;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class OperationUtil {

  public static Operation getOperation(GetOperationRequest request, OperationsGrpc.OperationsBlockingStub operationsStub) {
    return operationsStub.getOperation(request);
  }

  public static void waitForOperation(Operation operation, OperationsGrpc.OperationsBlockingStub operationsStub) throws IOException, TimeoutException {
    waitForOperation(operation, 10, TimeUnit.MINUTES, operationsStub);
  }

  public static void waitForOperation(Operation operation, long timeout, TimeUnit timeUnit, OperationsGrpc.OperationsBlockingStub operationsStub)
      throws TimeoutException, IOException {
    GetOperationRequest request =
        GetOperationRequest.newBuilder().setName(operation.getName()).build();

    ExponentialBackOff backOff =
        new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(100)
            .setMultiplier(1.3)
            .setMaxIntervalMillis(Ints.checkedCast(TimeUnit.SECONDS.toMillis(60)))
            .setMaxElapsedTimeMillis(Ints.checkedCast(timeUnit.toMillis(timeout)))
            .build();

    Operation currentOperationState = operation;

    while (true) {
      if (currentOperationState.getDone()) {
        switch (currentOperationState.getResultCase()) {
          case RESPONSE:
            return;
          case ERROR:
            throw StatusProto.toStatusRuntimeException(currentOperationState.getError());
          case RESULT_NOT_SET:
            throw new IllegalStateException(
                "System returned invalid response for Operation check: " + currentOperationState);
        }
      }

      final long backOffMillis;
      try {
        backOffMillis = backOff.nextBackOffMillis();
      } catch (IOException e) {
        // Should never happen.
        throw new RuntimeException(e);
      }
      if (backOffMillis == BackOff.STOP) {
        throw new TimeoutException("Operation did not complete in time");
      } else {
        try {
          Thread.sleep(backOffMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for operation to finish");
        }
      }

      currentOperationState = getOperation(request, operationsStub);
    }
  }
}
