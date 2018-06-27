package com.google.cloud.bigtable.grpc.io;

import com.google.api.core.ApiClock;
import com.google.api.core.InternalApi;
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
import org.joda.time.Duration;

@InternalApi
public class Watchdog implements Runnable {
  // Dummy value to convert the ConcurrentHashMap into a Set
  private static Object PRESENT = new Object();
  private final ConcurrentHashMap<WatchedCall<?,?>, Object> openStreams = new ConcurrentHashMap<>();

  private final ApiClock clock;
  private final Duration waitTimeout;
  private final Duration idleTimeout;

  private ScheduledFuture<?> scheduledFuture;


  public Watchdog(ApiClock clock, Duration waitTimeout, Duration idleTimeout) {
    this.clock = Preconditions.checkNotNull(clock, "clock can't be null");
    this.waitTimeout = Preconditions.checkNotNull(waitTimeout, "waitTimeout can't be null");
    this.idleTimeout = Preconditions.checkNotNull(idleTimeout, "idleTimeout can't be null");
  }

  public <ReqT, RespT> ClientCall<ReqT, RespT> watch(ClientCall<ReqT, RespT> innerCall) {
    return new WatchedCall<>(innerCall);
  }

  public void run() {
    Iterator<Entry<WatchedCall<?,?>, Object>> it = openStreams.entrySet().iterator();

    while (it.hasNext()) {
      WatchedCall<?,?> stream = it.next().getKey();
      if (stream.cancelIfStale()) {
        it.remove();
      }
    }
  }

  public void start(ScheduledExecutorService executor) {
    Preconditions.checkState(scheduledFuture == null, "Already started");
    Duration min = waitTimeout;
    if (idleTimeout.isShorterThan(min)) {
      min = idleTimeout;
    }
    Duration checkPeriod = min.dividedBy(2);
    if (checkPeriod.isShorterThan(Duration.standardSeconds(10))) {
      checkPeriod = Duration.standardSeconds(10);
    }

    scheduledFuture = executor.scheduleAtFixedRate(this, checkPeriod.getMillis(), checkPeriod.getMillis(), TimeUnit.MILLISECONDS);
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
    private long lastActivityAt = clock.millisTime();

    WatchedCall(ClientCall<ReqT, RespT> delegate) {
      super(delegate);
    }

    @Override
    public void start(Listener<RespT> listener, Metadata metadata) {
      ClientCall<ReqT, RespT> call = delegate();

      synchronized (lock) {
        Preconditions.checkState(state == null, "Already started");
        state = (pendingCount == 0) ? State.IDLE : State.WAITING;
        lastActivityAt = clock.millisTime();
      }

      openStreams.put(this, PRESENT);

      call.start(new SimpleForwardingClientCallListener<RespT>(listener) {
        @Override
        public void onMessage(RespT message) {
          synchronized (lock) {
            state = State.DELIVERING;
          }

          super.onMessage(message);

          synchronized (lock) {
            pendingCount--;
            lastActivityAt = clock.millisTime();

            state = (pendingCount > 0) ? State.WAITING : State.IDLE;
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
          lastActivityAt = clock.millisTime();
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
        long waitTime = clock.millisTime() - lastActivityAt;

        switch (this.state) {
          case IDLE:
            if (idleTimeout.getMillis() > 0 && waitTime >= idleTimeout.getMillis()) {
              delegate().cancel("Canceled due to idle connection", new CancellationException());
              return true;
            }
            break;
          case WAITING:
            if (waitTimeout.getMillis() > 0 && waitTime >= waitTimeout.getMillis()) {
              delegate().cancel("Canceled due to timeout waiting for next response", new StreamWaitTimeoutException());
              return true;
            }
            break;
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
