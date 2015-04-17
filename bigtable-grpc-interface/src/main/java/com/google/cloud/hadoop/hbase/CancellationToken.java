package com.google.cloud.hadoop.hbase;

import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.Executor;

/**
 * A class for communicating that an operation has been or should be cancelled.
 */
public class CancellationToken {

  private final SettableFuture<Void> cancelledFuture = SettableFuture.create();

  /**
   * Add a listener that will be fired if and when this token is cancelled.
   */
  public void addListener(Runnable runnable, Executor executor) {
    cancelledFuture.addListener(runnable, executor);
  }

  /**
   * Inform listeners that the action has been / should be cancelled.
   */
  public void cancel() {
    cancelledFuture.set(null);
  }
}
