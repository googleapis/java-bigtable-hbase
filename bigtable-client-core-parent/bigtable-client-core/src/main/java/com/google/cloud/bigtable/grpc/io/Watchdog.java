package com.google.cloud.bigtable.grpc.io;

import com.google.api.client.util.Clock;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.config.Logger;
import com.google.common.base.Preconditions;
import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

@InternalApi
public class Watchdog implements Runnable {
  private static final Logger LOG = new Logger(Watchdog.class);

  // By default kill the stream after 10 minutes of inactivity
  private static final long DEFAULT_IDLE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10);
  private static final long MIN_CHECK_PERIOD_MS = TimeUnit.SECONDS.toMillis(10);

  // Dummy value to convert the ConcurrentHashMap into a Set
  private static Object PRESENT = new Object();
  private final ConcurrentHashMap<WatchedCall<?,?>, Object> openStreams = new ConcurrentHashMap<>();

  private final Clock clock;
  private final long waitTimeoutMs;
  private final long idleTimeoutMs;

  private ScheduledFuture<?> scheduledFuture;

  public Watchdog(Clock clock, long waitTimeoutMs) {
    this(clock, waitTimeoutMs, DEFAULT_IDLE_TIMEOUT_MS);
  }

  public Watchdog(Clock clock, long waitTimeoutMs, long idleTimeoutMs) {
    this.clock = Preconditions.checkNotNull(clock, "clock can't be null");
    this.waitTimeoutMs = waitTimeoutMs;
    this.idleTimeoutMs = idleTimeoutMs;
  }

  public <ReqT, RespT> ClientCall<ReqT, RespT> watch(ClientCall<ReqT, RespT> innerCall) {
    return new WatchedCall<>(innerCall);
  }

  public void run() {
    Iterator<Entry<WatchedCall<?,?>, Object>> it = openStreams.entrySet().iterator();

    int count = 0;

    while (it.hasNext()) {
      WatchedCall<?,?> stream = it.next().getKey();
      if (stream.cancelIfStale()) {
        count++;
        it.remove();
      }
    }

    if (count > 0) {
      LOG.warn("Found %d stale streams and cancelled them", count);
    }
  }

  public void start(ScheduledExecutorService executor) {
    Preconditions.checkState(scheduledFuture == null, "Already started");
    long minTimeoutMs = Math.min(waitTimeoutMs, idleTimeoutMs);
    long checkPeriodMs = Math.max(minTimeoutMs / 2, MIN_CHECK_PERIOD_MS);

    scheduledFuture = executor.scheduleAtFixedRate(this,
        checkPeriodMs, checkPeriodMs, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    scheduledFuture.cancel(true);
    scheduledFuture = null;
  }

  enum State {
    /** Stream has been started, but doesn't have any outstanding requests. */
    IDLE,
    /** Stream is awaiting a response from upstream. */
    WAITING,
    /**
     * Stream received a response from upstream, and is awaiting outerResponseObserver processing.
     */
    DELIVERING
  }

  class WatchedCall<ReqT, RespT> extends SimpleForwardingClientCall<ReqT, RespT> {
    private final Object lock = new Object();

    @GuardedBy("lock")
    private State state;

    @GuardedBy("lock")
    private int pendingCount = 0;

    @GuardedBy("lock")
    private long lastActivityAt = clock.currentTimeMillis();

    WatchedCall(ClientCall<ReqT, RespT> delegate) {
      super(delegate);
    }

    @Override
    public void start(Listener<RespT> listener, Metadata metadata) {
      ClientCall<ReqT, RespT> call = delegate();

      synchronized (lock) {
        Preconditions.checkState(state == null, "Already started");
        state = (pendingCount == 0) ? State.IDLE : State.WAITING;
        lastActivityAt = clock.currentTimeMillis();
      }

      openStreams.put(this, PRESENT);

      call.start(new SimpleForwardingClientCallListener<RespT>(listener) {
        @Override
        public void onMessage(RespT message) {
          synchronized (lock) {
            state = State.DELIVERING;
          }

          try {
            super.onMessage(message);
          } finally {
            synchronized (lock) {
              pendingCount--;
              lastActivityAt = clock.currentTimeMillis();

              state = (pendingCount > 0) ? State.WAITING : State.IDLE;
            }
          }
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
          openStreams.remove(WatchedCall.this);

          // Take care to convert an error caused by StreamWaitTimeoutException into an ABORTED
          // status. This will allow it to be retried.
          if (status.getCause() instanceof StreamWaitTimeoutException) {
            status = Status.ABORTED
                .withDescription(status.getDescription())
                .withCause(status.getCause());
          }

          super.onClose(status, trailers);
        }
      }, metadata);
    }

    @Override
    public void request(int count) {
      synchronized (lock) {
        if (state == State.IDLE) {
          state = State.WAITING;
          lastActivityAt = clock.currentTimeMillis();
        }

        // Increment the request count without overflow
        int maxIncrement = Integer.MAX_VALUE - pendingCount;
        count = Math.min(maxIncrement, count);
        pendingCount += count;
      }

      super.request(count);
    }

    boolean cancelIfStale() {
      synchronized (lock) {
        long waitTime = clock.currentTimeMillis() - lastActivityAt;

        switch (this.state) {
          case IDLE:
            if (waitTime >= idleTimeoutMs) {
              delegate().cancel("Canceled due to idle connection", new CancellationException());
              return true;
            }
            break;
          case WAITING:
            if (waitTime >= waitTimeoutMs) {
              delegate().cancel("Canceled due to timeout waiting for next response", new StreamWaitTimeoutException());
              return true;
            }
            break;
          case DELIVERING:
            // Don't cancel the stream while it's results are being processed by user code.
            break;
          default:
            throw new IllegalStateException("Unknown state: " + this.state);
        }
      }

      return false;
    }
  }

  /**
   * Marker exception to replace cancelled status with aborted to allow retries.
   */
  private static class StreamWaitTimeoutException extends RuntimeException {}
}
