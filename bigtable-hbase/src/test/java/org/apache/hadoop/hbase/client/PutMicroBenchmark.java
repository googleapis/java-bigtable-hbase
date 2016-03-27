/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package org.apache.hadoop.hbase.client;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableDataGrpcClient;
import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.ChannelPool.ChannelFactory;
import com.google.cloud.bigtable.grpc.io.HeaderInterceptor;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Empty;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PutMicroBenchmark {
  static final int NUM_CELLS = 100;
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");

  public static void main(String[] args) throws IOException, ServiceException, InterruptedException {
    HBaseRequestAdapter hbaseAdapter =
        new HBaseRequestAdapter(
            new BigtableClusterName("proj", "zoneId", "clusterId"),
            TableName.valueOf("table"),
            new Configuration(false));
    DataGenerationHelper dataHelper = new DataGenerationHelper();
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = dataHelper.randomData("testQualifier-", NUM_CELLS);
    byte[][] values = dataHelper.randomData("testValue-", NUM_CELLS);

    Put put = new Put(rowKey);
    List<QualifierValue> keyValues = new ArrayList<QualifierValue>(100);
    for (int i = 0; i < NUM_CELLS; ++i) {
      put.addColumn(COLUMN_FAMILY, quals[i], values[i]);
      keyValues.add(new QualifierValue(quals[i], values[i]));
    }

    final ManagedChannel channel = createFakeChannel();
    ChannelFactory factory =
        new ChannelFactory() {
          @Override
          public ManagedChannel create() throws IOException {
            return channel;
          }
        };

    ChannelPool channelPool = new ChannelPool(ImmutableList.<HeaderInterceptor>of(), factory);
    final BigtableDataClient client =
        new BigtableDataGrpcClient(
            channelPool,
            BigtableSessionSharedThreadPools.getInstance().getRetryExecutor(),
            new BigtableOptions.Builder().build());

    final MutateRowRequest request = hbaseAdapter.adapt(put);
    client.mutateRow(request);
    client.mutateRow(request);
    final int count = 10_000_000;
    Runnable r = new Runnable(){
      @Override
      public void run() {
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
          try {
            client.mutateRow(request);
          } catch (ServiceException e) {
            // TODO(sduskis): Auto-generated catch block
            e.printStackTrace();
          }
        }
        long totalTime = System.nanoTime() - start;
        System.out.println(
            String.format(
                "Put %d in %d ms.  %d nanos/put.  %f put/sec",
                count,
                totalTime / 1000000,
                totalTime / count,
                count * 1000000000.0 / totalTime));
      }
    };
    
    ExecutorService e = Executors.newCachedThreadPool();
    for (int i =0;i<10;i++) {
      e.execute(r);
    }
    e.shutdown();
    e.awaitTermination(10, TimeUnit.HOURS);
  }

  private static ManagedChannel createFakeChannel() {
    return new ManagedChannel() {
      @Override
      public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
          MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return createNewCall();
      }

      @Override
      public String authority() {
        return null;
      }

      @Override
      public ManagedChannel shutdownNow() {
        return null;
      }

      @Override
      public ManagedChannel shutdown() {
        return null;
      }

      @Override
      public boolean isTerminated() {
        return false;
      }

      @Override
      public boolean isShutdown() {
        return false;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
      }
    };
  }

  /**
   * @return
   */
  protected static <RequestT, ResponseT> ClientCall<RequestT, ResponseT> createNewCall() {
    return new ClientCall<RequestT, ResponseT>() {

      private ClientCall.Listener<ResponseT> responseListener;

      @Override
      public void start(ClientCall.Listener<ResponseT> responseListener, Metadata headers) {
        this.responseListener = responseListener;
      }

      @Override
      public void request(int numMessages) {}

      @Override
      public void cancel() {}

      @Override
      public void halfClose() {}

      @SuppressWarnings("unchecked")
      @Override
      public void sendMessage(RequestT message) {
        responseListener.onMessage((ResponseT) Empty.getDefaultInstance());
      }
    };
  }

  protected static class QualifierValue implements Comparable<QualifierValue> {
    protected final byte[] qualifier;
    protected final byte[] value;

    public QualifierValue(byte[] qualifier, byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }

    @Override
    public int compareTo(QualifierValue qualifierValue) {
      return Bytes.compareTo(this.qualifier, qualifierValue.qualifier);
    }
  }
}
