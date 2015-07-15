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
import io.grpc.StatusRuntimeException;

import java.io.IOException;


/**
 * An IOException that carries a gRPC Status object.
 */
public class IOExceptionWithStatus extends IOException {

  private static final long serialVersionUID = 8642100644073789860L;
  private final StatusRuntimeException cause;

  public IOExceptionWithStatus(String message, StatusRuntimeException cause) {
    super(message, cause);
    this.cause = cause;
  }

  /**
   * Status from the provided OperationRuntimeException.
   */
  // TODO: cause.getStatus() sends JVCM warnings, even though it shouldn't.  The gRPC team
  // ought to fix that.
  public Status getStatus() {
    return cause.getStatus();
  }
}
