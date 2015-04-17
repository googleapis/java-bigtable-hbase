package com.google.cloud.hadoop.hbase;

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
