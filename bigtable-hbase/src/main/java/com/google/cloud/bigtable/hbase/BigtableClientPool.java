package com.google.cloud.bigtable.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.CheckAndMutateRowResponse;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.hadoop.hbase.BigtableClient;
import com.google.cloud.hadoop.hbase.ResultScanner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.ServiceException;

public class BigtableClientPool implements BigtableClient {

  protected static final Logger LOG = new Logger(BigtableClientPool.class);

  public interface BigtableClientFactory {
    BigtableClient create();
  }

  private static class TimedBigtableClient {
    private BigtableClient client;
    private long end;

    public TimedBigtableClient(BigtableClient client, long end) {
      super();
      this.client = client;
      this.end = end;
    }

    public BigtableClient getClient() {
      return client;
    }

    public boolean isFinished() {
      if (end < 0) {
        return false;
      }
      return System.currentTimeMillis() > this.end;
    }
  }

  private static final long MAX_CREATE_RETRIES = 10;
  private static final long SLEEP_TIME = 1000;

  private final long timeoutMs;

  private final List<TimedBigtableClient> delegates = new ArrayList<>();
  private final BigtableClientFactory factory;
  private final AtomicInteger requestCount = new AtomicInteger();
  private final Executor executor;
  private boolean closed = false;
  private Object closeSignal = new String("");

  public BigtableClientPool(final int channelPoolSize, long timeoutMs, Executor executor,
      BigtableClientFactory factory) {
    this.factory = factory;
    this.timeoutMs = timeoutMs;
    delegates.add(createChannel());
    this.executor = executor;
    initPool(channelPoolSize, executor);
  }

  public void initPool(final int channelPoolSize, Executor executor) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        while (!closed) {
          boolean created = false;
          if (delegates.size() < channelPoolSize) {
            boolean adding = false;
            synchronized (delegates) {
              adding = !closed && delegates.size() < channelPoolSize;
            }
            if (adding) {
              try {
                TimedBigtableClient channel = createChannel();
                if (channel != null) {
                  created = true;
                  synchronized (delegates) {
                    delegates.add(channel);
                    delegates.notify();
                  }
                }
              } catch (Exception e) {
                LOG.warn("Could not create a channel.", e);
              }
            }
          }
          if (!created && !closed) {
            try {
              synchronized (closeSignal) {
                if (!closed) {
                  closeSignal.wait(SLEEP_TIME);
                }
              }
            } catch (InterruptedException e) {
              LOG.info("Sleep interrupted in Channel creator thread.", e);
            }
          }
        }
      }
    });
  }

  /**
   * Creates a ClosableChannel with a timeout.
   */
  private TimedBigtableClient createChannel() {
    BigtableClient client = this.factory.create();
    if (client == null) {
      return null;
    }
    // Set the timeout. Use a random variability to reduce jetteriness.
    double randomizedPercentage = 1 - (.05 * Math.random());
    long timeout = this.timeoutMs < 0 ? -1L : (long) (this.timeoutMs * randomizedPercentage);
    return new TimedBigtableClient(client, timeout + System.currentTimeMillis());
  }

  @Override
  public void close() throws Exception {
    this.closed = true;
    synchronized (closeSignal) {
      closeSignal.notify();
    }
    List<TimedBigtableClient> toClose = null;
    synchronized (delegates) {
      toClose = new ArrayList<>(delegates);
      delegates.clear();
      delegates.notifyAll();
    }
    for (TimedBigtableClient wrapper : toClose) {
      close(wrapper);
    }
  }

  @Override
  public CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request)
      throws ServiceException {
    return getDelegate().checkAndMutateRow(request);
  }

  @Override
  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    return getDelegate().checkAndMutateRowAsync(request);
  }

  @Override
  public Empty mutateRow(MutateRowRequest request) throws ServiceException {
    return getDelegate().mutateRow(request);
  }

  @Override
  public ListenableFuture<Empty> mutateRowAsync(MutateRowRequest request) {
    return getDelegate().mutateRowAsync(request);
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRowRequest request) {
    return getDelegate().readModifyWriteRow(request);
  }

  @Override
  public ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRowRequest request) {
    return getDelegate().readModifyWriteRowAsync(request);
  }

  @Override
  public ResultScanner<Row> readRows(ReadRowsRequest request) {
    return getDelegate().readRows(request);
  }

  @Override
  public ListenableFuture<List<Row>> readRowsAsync(ReadRowsRequest request) {
    return getDelegate().readRowsAsync(request);
  }

  @Override
  public ImmutableList<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request) {
    return getDelegate().sampleRowKeys(request);
  }

  @Override
  public ListenableFuture<ImmutableList<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request) {
    return getDelegate().sampleRowKeysAsync(request);
  }

  private BigtableClient getDelegate() {
    int requestNum = requestCount.getAndIncrement();
    int waited = 0;
    while (waited < MAX_CREATE_RETRIES && !closed) {
      if (!delegates.isEmpty()) {
        TimedBigtableClient wrapper = null;
        try {
          wrapper = delegates.get(requestNum % delegates.size());
          if (wrapper != null) {
            if (wrapper.isFinished()) {
              synchronized (delegates) {
                delegates.remove(wrapper);
              }
              close(wrapper);
            } else {
              return wrapper.getClient();
            }
          }
        } catch (IndexOutOfBoundsException | ArithmeticException e) {
          // The client must have been reduced in size between the isEmpty() and the call the
          // delegates.get(). This would only happen if this ChannelSet were used across threads.
          // Using this class across threads is not recommended.
        }
      } else {
        synchronized (delegates) {
          if (delegates.isEmpty()) {
            try {
              waited++;
              delegates.wait(SLEEP_TIME);
            } catch (InterruptedException e) {
              LOG.info("Could not sleep in getChannel.", e);
            }
          }
        }
      }
    }
    throw new IllegalStateException("Could not get a Channel.");
  }

  private void close(final TimedBigtableClient wrapper) {
    this.executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          wrapper.getClient().close();
        } catch (Exception e) {
          LOG.warn("Could not close channel in getChannel.", e);
        }
      }
    });
  }
}
