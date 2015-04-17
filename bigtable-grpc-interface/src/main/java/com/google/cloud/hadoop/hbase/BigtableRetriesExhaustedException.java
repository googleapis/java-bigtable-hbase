package com.google.cloud.hadoop.hbase;

import java.io.IOException;

/**
 * An Exception that is thrown when an operation fails, even in the face of retrues.
 */
public class BigtableRetriesExhaustedException extends IOException {
  public BigtableRetriesExhaustedException(String message, Throwable cause) {
    super(message, cause);
  }
}
