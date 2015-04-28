package com.google.cloud.bigtable.grpc;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.ExponentialBackOff.Builder;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Sleeper;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.protobuf.ByteString;

import io.grpc.Status;

import java.io.IOException;


/**
 * A ResultScanner that attempts to resume the readRows call when it
 * encounters gRPC INTERNAL errors.
 */
public class ResumingStreamingResultScanner extends AbstractBigtableResultScanner {

  private static final ByteString NEXT_ROW_SUFFIX = ByteString.copyFrom(new byte[]{0x00});
  private final BigtableResultScannerFactory scannerFactory;

  /**
   * Construct a ByteString containing the next possible row key.
   */
  static ByteString nextRowKey(ByteString previous) {
    return previous.concat(NEXT_ROW_SUFFIX);
  }

  private final Builder backOffBuilder;
  private final ReadRowsRequest originalRequest;
  private final boolean retryOnDeadlineExceeded;

  private BackOff currentBackoff;
  private ResultScanner<Row> currentDelegate;
  private ByteString lastRowKey = null;
  private Sleeper sleeper = Sleeper.DEFAULT;

  public ResumingStreamingResultScanner(
      RetryOptions retryOptions,
      ReadRowsRequest originalRequest,
      BigtableResultScannerFactory scannerFactory) {
    Preconditions.checkArgument(
        !originalRequest.getAllowRowInterleaving(),
        "Row interleaving is not supported when using resumable streams");
    retryOnDeadlineExceeded = retryOptions.retryOnDeadlineExceeded();
    this.backOffBuilder = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(retryOptions.getInitialBackoffMillis())
        .setMaxElapsedTimeMillis(retryOptions.getMaxElaspedBackoffMillis())
        .setMultiplier(retryOptions.getBackoffMultiplier());
    this.originalRequest = originalRequest;
    this.scannerFactory = scannerFactory;
    this.currentBackoff = backOffBuilder.build();
    this.currentDelegate = scannerFactory.createScanner(originalRequest);
  }

  @Override
  public Row next() throws IOException {
    while (true) {
      try {
        Row result = currentDelegate.next();
        if (result != null) {
          lastRowKey = result.getKey();
        }
        // We've had at least one successful RPC, reset the backoff
        currentBackoff.reset();

        return result;
      } catch (IOExceptionWithStatus ioe) {
        Status.Code code = ioe.getStatus().getCode();
        if (code == Status.INTERNAL.getCode()
            || code == Status.UNAVAILABLE.getCode()
            || (retryOnDeadlineExceeded && code == Status.DEADLINE_EXCEEDED.getCode())) {
          long nextBackOff = currentBackoff.nextBackOffMillis();
          if (nextBackOff == BackOff.STOP) {
            throw new BigtableRetriesExhaustedException(
                "Exhausted streaming retries.", ioe);
          }
          sleep(nextBackOff);
          reissueRequest();
        } else {
          throw ioe;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    currentDelegate.close();
  }

  private void reissueRequest() {
    ReadRowsRequest.Builder newRequest = originalRequest.toBuilder();
    if (lastRowKey != null) {
      newRequest.getRowRangeBuilder().setStartKey(nextRowKey(lastRowKey));
    }
    currentDelegate = scannerFactory.createScanner(newRequest.build());
  }

  private void sleep(long millis) throws IOException {
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while sleeping for resume", e);
    }
  }
}
