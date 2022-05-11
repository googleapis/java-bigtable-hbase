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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

/**
 * Provides additional context for exceptions thrown by mirrored operations when {@link
 * MirroringConfigurationHelper.MIRRORING_SYNCHRONOUS_WRITES} is enabled.
 *
 * <p>Instances of this class are not thrown directly. These exceptions are attached as the root
 * cause of the exception chain (as defined by {@link Exception#getCause()}) returned by the
 * MirroringClient when it is set to synchronous mode. One can easily retrieve it using {@link
 * MirroringOperationException#extractRootCause(Throwable)}.
 *
 * <p>If thrown exception is a {@link
 * org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException} then {@link
 * MirroringOperationException} is not added to it directly, but it is added to every exception in
 * it.
 */
public class MirroringOperationException extends Exception {
  public static class ExceptionDetails {
    public final Throwable exception;
    public final String hostnameAndPort;

    public ExceptionDetails(Throwable exception) {
      this(exception, "");
    }

    public ExceptionDetails(Throwable exception, String hostnameAndPort) {
      this.exception = exception;
      this.hostnameAndPort = hostnameAndPort;
    }
  }

  public enum DatabaseIdentifier {
    Primary,
    Secondary,
    Both
  }

  /** Identifies which database failed. */
  public final DatabaseIdentifier databaseIdentifier;
  /**
   * Operation that failed. Might be null if no specific operation was related to this exception.
   *
   * <p>If operation failed on primary, then this operation is one of the operations provided by the
   * user.
   *
   * <p>If operation failed on secondary, then this operation might not be one of them - this will
   * be the case with {@link Increment} and {@link Append} operations that are mirrored using {@link
   * Put} to mitigate possible inconsistencies.
   */
  public final Row operation;

  /**
   * Stores an exception that happened on secondary database along with hostnamePort if available.
   *
   * <p>If operation failed on both databases then this field can, but is not required to, have a
   * value.
   */
  public final ExceptionDetails secondaryException;

  private MirroringOperationException(DatabaseIdentifier databaseIdentifier, Row operation) {
    this(databaseIdentifier, operation, null);
  }

  private MirroringOperationException(
      DatabaseIdentifier databaseIdentifier, Row operation, ExceptionDetails secondaryException) {
    Preconditions.checkArgument(
        secondaryException == null || databaseIdentifier == DatabaseIdentifier.Both);
    this.databaseIdentifier = databaseIdentifier;
    this.operation = operation;
    this.secondaryException = secondaryException;
  }

  public static <T extends Throwable> T markedAsPrimaryException(T e, Row primaryOperation) {
    return markedWith(
        e, new MirroringOperationException(DatabaseIdentifier.Primary, primaryOperation));
  }

  public static <T extends Throwable> T markedAsBothException(
      T e, ExceptionDetails secondaryExceptionDetails, Row primaryOperation) {
    return markedWith(
        e,
        new MirroringOperationException(
            DatabaseIdentifier.Both, primaryOperation, secondaryExceptionDetails));
  }

  public static <T extends Throwable> T markedAsSecondaryException(T e, Row secondaryOperation) {
    return markedWith(
        e, new MirroringOperationException(DatabaseIdentifier.Secondary, secondaryOperation));
  }

  private static <T extends Throwable> T markedWith(T e, Throwable marker) {
    Throwables.getRootCause(e).initCause(marker);
    return e;
  }

  /**
   * Extracts {@link MirroringOperationException} instance from the bottom of chain of causes.
   *
   * @param exception Exception thrown by mirroring operation.
   * @return {@link MirroringOperationException} instance if present, {@code null} otherwise.
   */
  public static @Nullable MirroringOperationException extractRootCause(Throwable exception) {
    Throwable rootCause = Throwables.getRootCause(exception);
    if (rootCause instanceof MirroringOperationException) {
      return (MirroringOperationException) rootCause;
    }
    return null;
  }
}
