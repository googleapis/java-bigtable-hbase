package com.google.cloud.bigtable.grpc.async;

import com.codahale.metrics.Timer;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class limits access by RPCs to system resources
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ResourceLimiter {
  private static final Logger LOG = new Logger(ResourceLimiter.class);

  private static final long REGISTER_WAIT_MILLIS = 5;

  private final long maxHeapSize;
  private final int absoluteMaxInFlightRpcs;
  private int currentInFlightMaxRpcs;
  private final AtomicLong operationSequenceGenerator = new AtomicLong();
  private final Map<Long, Long> pendingOperationsWithSize = new HashMap<>();
  private final LinkedBlockingDeque<Long> completedOperationIds = new LinkedBlockingDeque<>();
  private long currentWriteBufferSize;
  private boolean isThrottling = false;

  /**
   * <p>Constructor for ResourceLimiter.</p>
   *
   * @param maxHeapSize a long.
   * @param maxInFlightRpcs a int.
   */
  public ResourceLimiter(long maxHeapSize, int maxInFlightRpcs) {
    this.maxHeapSize = maxHeapSize;
    this.absoluteMaxInFlightRpcs = maxInFlightRpcs;
    this.currentInFlightMaxRpcs = maxInFlightRpcs;
  }

  /**
   * Register an operation with the given size before sending.
   * This call WILL BLOCK until resources are available. This method must
   * be paired with a call to {@code markCanBeCompleted} in order to make sure
   * resources are properly released.
   *
   * @param heapSize The serialized size of the RPC to be sent
   * @return A unique operation id
   * @throws java.lang.InterruptedException if any.
   */
  public synchronized long registerOperationWithHeapSize(long heapSize)
      throws InterruptedException {
    long operationId = operationSequenceGenerator.incrementAndGet();
    while (unsynchronizedIsFull()) {
      waitForCompletions(REGISTER_WAIT_MILLIS);
    }

    pendingOperationsWithSize.put(operationId, heapSize);
    currentWriteBufferSize += heapSize;
    return operationId;
  }

  /**
   * Mark an operation id, as returned by {@code registerOperationWithHeapSize}, as complete
   *
   * @param id a long.
   */
  public void markCanBeCompleted(long id) {
    completedOperationIds.offerLast(id);
  }

  /**
   * <p>Getter for the field <code>maxHeapSize</code>.</p>
   *
   * @return The maximum allowed number of bytes across all across all outstanding RPCs
   */
  public long getMaxHeapSize() {
    return maxHeapSize;
  }

  /**
   * <p>Getter for the field <code>maxInFlightRpcs</code>.</p>
   *
   * @return The maximum allowed number of in-flight RPCs
   */
  public int getAbsoluteMaxInFlightRpcs() {
    return absoluteMaxInFlightRpcs;
  }

  /**
   * <p>Getter for the field <code>currentInFlightMaxRpcs</code>.</p>
   *
   * @return The current maximum number of allowed in-flight RPCs
   */
  public int getCurrentInFlightMaxRpcs() {
    return currentInFlightMaxRpcs;
  }

  /**
   * <p>Setter for the field <code>currentInFlightMaxRpcs</code>.</p>
   */
  public void setCurrentInFlightMaxRpcs(int currentInFlightMaxRpcs) {
    this.currentInFlightMaxRpcs = currentInFlightMaxRpcs;
  }

  /**
   * <p>getHeapSize.</p>
   *
   * @return The total size of all currently outstanding RPCs
   */
  public long getHeapSize() {
    return currentWriteBufferSize;
  }

  /**
   * <p>isFull.</p>
   *
   * @return true if no more RPCs can be started, false otherwise
   */
  public synchronized boolean isFull() {
    return unsynchronizedIsFull();
  }

  private boolean isFullInternal() {
    return currentWriteBufferSize >= maxHeapSize
        || pendingOperationsWithSize.size() >= currentInFlightMaxRpcs;
  }

  private boolean unsynchronizedIsFull() {
    if (!isFullInternal()) {
      return false;
    }
    // If we're not full, don't worry about cleaning up just yet.
    cleanupFinishedOperations();
    return isFullInternal();
  }

  /**
   * <p>hasInflightRequests.</p>
   *
   * @return true if there are currently in-flight RPCs
   */
  public synchronized boolean hasInflightRequests() {
    cleanupFinishedOperations();
    return !pendingOperationsWithSize.isEmpty();
  }

  private void cleanupFinishedOperations() {
    List<Long> toClean = new ArrayList<>();
    completedOperationIds.drainTo(toClean);
    if (!toClean.isEmpty()) {
      markOperationsCompleted(toClean);
    }
  }

  private synchronized void markOperationsCompleted(List<Long> operationSequenceIds) {
    for (Long operationSequenceId : operationSequenceIds) {
      markOperationComplete(operationSequenceId);
    }
  }

  /**
   * Waits for a completion and then marks it as complete.
   * @throws InterruptedException
   */
  private void waitForCompletions(long timeoutMs) throws InterruptedException {
    Long completedOperation =
        this.completedOperationIds.pollFirst(timeoutMs, TimeUnit.MILLISECONDS);
    if (completedOperation != null) {
      markOperationComplete(completedOperation);
    }
  }

  private void markOperationComplete(Long operationSequenceId) {
    Long heapSize = pendingOperationsWithSize.remove(operationSequenceId);
    if (heapSize != null) {
      currentWriteBufferSize -= heapSize;
    } else {
      LOG.warn("An operation completed successfully but provided multiple completion notifications."
          + " Please notify Google that this occurred.");
    }
  }

  /**
   * Enable an experimental feature that will throttle requests made from {@link BulkMutation}. The
   * logic is as follows:
   * <p>
   * <ul>
   * <li>To start: <ul>
   *   <li>reduce parallelism to 25% -- The parallelism is high to begin with. This reduction should
   *       reduce the impacts of a bursty job, such as those found in Dataflow.
   *   </ul>
   * <li>every 20 seconds:
   *   <pre>
   *   if (rpc_latency &gt; threshold) {
   *      decrease parallelism by 10% of original maximum.
   *   } else if (rpc_latency &lt; threshold && rpcsWereThrottled()) {
   *      increase parallelism by 5% of original maximum.
   *   }
   * </pre>
   * NOTE: increases are capped by the initial maximum.  Decreases are floored at 1% of the
   * original maximum so that there is some level of throughput.
   * </ul>
   * @param bulkMutationRpcTargetMs the target for latency of MutateRows requests in milliseconds.
   */
  public synchronized void throttle(final int bulkMutationRpcTargetMs) {
    if (isThrottling) {
      // Throttling was already turned on.  No need to do it again.
      return;
    }
    LOG.info(
      "Initializing BulkMutation throttling.  "
          + "Once latency is higher than %d ms, parallelism will be reduced.",
      bulkMutationRpcTargetMs);

    // target roughly 20% within the the target so that there isn't too much churn
    final long highTargetMs = (long) (bulkMutationRpcTargetMs * 1.2);
    final long lowTargetMs = (long) (bulkMutationRpcTargetMs * 0.8);

    // Increase at 5% of the maximum RPC count increments, and reduce at 10%. The basic assumption
    // is that the throttling should be aggressive, and caution should be taken to not go over the
    // latency cap. The assumption is that maximizing throughput is less important than system
    // stability.
    // The maximum in flight RPCs is pretty high. Start with a significantly reduced number, and
    // then work up or down.
    setCurrentInFlightMaxRpcs(getCurrentInFlightMaxRpcs() / 4);

    Runnable r = new Runnable() {
      @Override
      public void run() {
        ResourceLimiterStats stats = ResourceLimiterStats.getInstance();
        long meanLatencyMs = getMeanMs(stats.getMutationTimer());
        if (meanLatencyMs >= bulkMutationRpcTargetMs * 3) {
          // decrease at 30% of the maximum RPCs, with a minimum of 2.5%
          reduceParallelism(meanLatencyMs, absoluteMaxInFlightRpcs * 3 / 10);

        } else if (meanLatencyMs >= highTargetMs) {
          // decrease at 10% of the maximum RPCs, with a minimum of 2.5%
          reduceParallelism(meanLatencyMs, absoluteMaxInFlightRpcs / 10);

        } else if (getMeanMs(stats.getThrottlingTimer()) > 1) {
          if (meanLatencyMs <= lowTargetMs) {
            // if latency is low, and there was throttling of at least one millisecond, then
            // increase the parallelism so that new calls will not be throttled.

            // Increase parallelism at a slower than we decrease. The lower rate should help the
            // system maintain stability.
            increaseParallelism(meanLatencyMs, absoluteMaxInFlightRpcs / 20);

          } else if (currentInFlightMaxRpcs < absoluteMaxInFlightRpcs / 20
              && meanLatencyMs <= (bulkMutationRpcTargetMs * 2)) {
            // For some reason, when parallelism is reduced latency tends to be artificially higher.
            // Increase slowly to ensure that the system restabilizes.
            increaseParallelism(meanLatencyMs, absoluteMaxInFlightRpcs / 50);
          }
        }
      }

      private long getMeanMs(Timer timer) {
        return TimeUnit.NANOSECONDS.toMillis((long) timer.getSnapshot().getMean());
      }

      private void reduceParallelism(long meanLatencyNanos, int step) {
        int minimumRpcCount = Math.max(absoluteMaxInFlightRpcs / 100, 1);
        int newValue = Math.max(currentInFlightMaxRpcs - step, minimumRpcCount);
        setParallelism(meanLatencyNanos, "Reducing", newValue);
      }

      private void increaseParallelism(long meanLatencyNanos, int incrementStep) {
        int newValue = Math.min(currentInFlightMaxRpcs + incrementStep, absoluteMaxInFlightRpcs);
        setParallelism(meanLatencyNanos, "Increasing", newValue);
      }

      private void setParallelism(long meanLatencyNanos, String type, int newValue) {
        int currentValue = getCurrentInFlightMaxRpcs();
        if (newValue != currentValue) {
          setCurrentInFlightMaxRpcs(newValue);
          LOG.debug("Latency is at %d ms. %s paralellelism from %d to %d.",
            TimeUnit.NANOSECONDS.toMillis(meanLatencyNanos), type, currentValue,
            newValue);
        }
      }
    };

    // 20 seconds is an assumption that seems to work. In bad situations, the throttling will start
    // at 50%, and then 5 cycles, or 100 seconds later.
    BigtableSessionSharedThreadPools.getInstance().getRetryExecutor().scheduleAtFixedRate(r, 20, 20,
      TimeUnit.SECONDS);
    isThrottling = true;
  }
}
