/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.hbase;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a pool of {@link BigtableClient}s and uses them in a round robin fashion to make RPC
 * calls. This is a temporary performance fix as well as stane connection fix. Longer term fixes are
 * in the works to the gRPC and other technologies on which BigtableClient relies.
 */
public class BigtableClientPool implements BigtableClient {

  protected static final Logger LOG = new Logger(BigtableClientPool.class);

  /** Creates a functional BigtableClient so it can be added to the pool. */
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

    public void close() throws Exception {
      this.client.close();
      this.client = null;
    }

    public boolean needsRefresh() {
      return this.end > 0 && System.currentTimeMillis() > this.end;
    }

    public BigtableClient refresh(BigtableClient client, long end) {
      BigtableClient oldClient = client;
      this.end = end;
      this.client = client;
      return oldClient;
    }
  }

  private static final long MAX_CREATE_RETRIES = 10;
  private static final long SLEEP_TIME = 1000;

  private final long timeoutMs;

  private final List<TimedBigtableClient> delegates = new ArrayList<>();
  private final BigtableClientFactory factory;
  private final AtomicInteger requestCount = new AtomicInteger();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public BigtableClientPool(final int channelPoolSize, long timeoutMs, Executor executor,
      final BigtableClientFactory factory) {
    this.factory = factory;
    this.timeoutMs = timeoutMs;
    delegates.add(createChannel());
    Runnable createClientRunnable = new Runnable() {
      @Override
      public void run() {
        TimedBigtableClient wrapper = null;
        for (int i = 0; i < MAX_CREATE_RETRIES; i++) {
          try {
            wrapper = new TimedBigtableClient(factory.create(), caculateTimeout());
            synchronized (delegates) {
              delegates.add(wrapper);
              delegates.notifyAll();
            }
            break;
          } catch (Exception e) {
            LOG.warn("Could not create a connection.  Attempt #" + i , e);
          }
          // Is this a temporal issue?
          try {
            Thread.sleep(SLEEP_TIME);
          } catch (InterruptedException e) {
          }
        }

        if (wrapper == null) {
          LOG.error("Could not create a connection.");
          return;
        }

        int errorCount = 0;
        while(!closed.get() || errorCount > MAX_CREATE_RETRIES) {
          if (wrapper.needsRefresh()) {
            try {
              LOG.info("Refreshing client");
              BigtableClient oldClient = wrapper.refresh(factory.create(), caculateTimeout());
              oldClient.close();
              errorCount = 0;
            } catch (Exception e) {
              errorCount++;
              LOG.warn("Could not refresh the connection. Will try again shortly. "
                  + "This is attempt #%d of %d", e, errorCount, MAX_CREATE_RETRIES);
            }
          }
          synchronized (closed) {
            if (!closed.get()) {
              try {
                closed.wait(SLEEP_TIME);
              } catch (InterruptedException e) {
                // ignore;
              }
            }
          }
        }
      }
    };
    for (int i=0;i<channelPoolSize;i++){
      executor.execute(createClientRunnable);
    }
  }

  private long caculateTimeout(){
    // Set the timeout. Use a random variability to reduce jetteriness.
    double randomizedPercentage = 1 - (.05 * Math.random());
    long randomizedEnd = this.timeoutMs < 0 ? -1L : (long) (this.timeoutMs * randomizedPercentage);
    return randomizedEnd <=0 ? randomizedEnd : randomizedEnd + System.currentTimeMillis();
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
    this.closed.set(true);
    synchronized (closed) {
      closed.notify();
    }
    synchronized (delegates) {
      for (TimedBigtableClient wrapper : delegates) {
        wrapper.close();
      }
      delegates.clear();
      delegates.notifyAll();
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
    while (waited < MAX_CREATE_RETRIES && !closed.get()) {
      if (!delegates.isEmpty()) {
        TimedBigtableClient wrapper = null;
        wrapper = delegates.get(requestNum % delegates.size());
        return wrapper.getClient();
      } else {
        synchronized (delegates) {
          try {
            waited++;
            delegates.wait(SLEEP_TIME);
          } catch (InterruptedException e) {
            LOG.info("Could not sleep in getChannel.", e);
          }
        }
      }
    }
    throw new IllegalStateException("Could not get a Channel.");
  }
}
