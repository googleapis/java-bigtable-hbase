/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import io.grpc.Status;
import io.grpc.Status.OperationRuntimeException;

import java.io.IOException;


/**
 * An IOException that carries a gRPC Status object.
 */
public class IOExceptionWithStatus extends IOException {

  private final String message;
  private final OperationRuntimeException cause;

  public IOExceptionWithStatus(String message, OperationRuntimeException cause) {
    super(message, cause);
    this.message = message;
    this.cause = cause;
  }

  /**
   * Status from the provided OperationRuntimeException.
   */
  public Status getStatus() {
    return cause.getStatus();
  }
}
