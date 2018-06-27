package com.google.cloud.bigtable.grpc.io;

import com.google.api.core.ApiClock;
import com.google.common.base.Preconditions;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.joda.time.Duration;

/**
 * Internal implementation detail to prevent RPC from hanging
 */
public class WatchdogInterceptor implements ClientInterceptor {
  // Dummy value to convert the ConcurrentHashMap into a Set
  private static Object PRESENT = new Object();
  private final ConcurrentHashMap<WatchedCall<?,?>, Object> openStreams = new ConcurrentHashMap<>();

  private final Set<MethodDescriptor<?,?>> watchedMethods;
  private final ScheduledExecutorService executor;
  private final ApiClock clock;
  private final Duration waitTimeout;
  private final Duration idleTimeout;

  private ScheduledFuture<?> future;


  public WatchdogInterceptor(Set<MethodDescriptor<?, ?>> watchedMethods,
      ScheduledExecutorService executor,
      ApiClock clock, Duration waitTimeout, Duration idleTimeout) {
    this.watchedMethods = watchedMethods;
    this.executor = executor;
    this.clock = Preconditions.checkNotNull(clock, "clock can't be null");
    this.waitTimeout = Preconditions.checkNotNull(waitTimeout, "waitTimeout can't be null");
    this.idleTimeout = Preconditions.checkNotNull(idleTimeout, "idleTimeout can't be null");
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

    ClientCall<ReqT, RespT> call = channel.newCall(methodDescriptor, callOptions);
    if (watchedMethods.contains(methodDescriptor)) {
      call = new WatchedCall<>(call);
    }

    return call;
  }

  public void start() {
    future = executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        abortStaleStreams();
      }
    }, 0, waitTimeout.dividedBy(2).getMillis(), TimeUnit.MILLISECONDS);
  }

  public void stop() {
    future.cancel(true);
  }


  public void abortStaleStreams() {
    Iterator<Entry<WatchedCall<?,?>, Object>> it = openStreams.entrySet().iterator();

    while (it.hasNext()) {
      WatchedCall<?,?> stream = it.next().getKey();
      if (stream.cancelIfStale()) {
        it.remove();
      }
    }
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
