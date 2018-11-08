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

package com.google.cloud.bigtable.grpc;

import com.google.api.client.util.Clock;
import com.google.api.client.util.Strings;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BigtableVersionInfo;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.grpc.async.ResourceLimiter;
import com.google.cloud.bigtable.grpc.async.ResourceLimiterStats;
import com.google.cloud.bigtable.grpc.async.ThrottlingClientInterceptor;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.CredentialInterceptorCache;
import com.google.cloud.bigtable.grpc.io.GoogleCloudResourcePrefixInterceptor;
import com.google.cloud.bigtable.grpc.io.Watchdog;
import com.google.cloud.bigtable.grpc.io.WatchdogInterceptor;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.util.ThreadUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

/**
 * <p>Encapsulates the creation of Bigtable Grpc services.</p>
 *
 * <p>The following functionality is handled by this class:
 * <ol>
 *   <li> Creates Executors
 *   <li> Creates Channels - netty ChannelImpls, ReconnectingChannel and ChannelPools
 *   <li> Creates ChannelInterceptors - auth headers, performance interceptors.
 *   <li> Close anything above that needs to be closed (ExecutorService, CahnnelImpls)
 * </ol>
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableSession implements Closeable {

  private static final Logger LOG = new Logger(BigtableSession.class);
  // TODO: Consider caching channel pools per instance.
  private static ManagedChannel cachedDataChannelPool;
  private static final Map<String, ResourceLimiter> resourceLimiterMap = new HashMap<>();

  // 256 MB, server has 256 MB limit.
  private final static int MAX_MESSAGE_SIZE = 1 << 28;

  @VisibleForTesting
  static final String PROJECT_ID_EMPTY_OR_NULL = "ProjectId must not be empty or null.";
  @VisibleForTesting
  static final String INSTANCE_ID_EMPTY_OR_NULL = "InstanceId must not be empty or null.";
  @VisibleForTesting
  static final String USER_AGENT_EMPTY_OR_NULL = "UserAgent must not be empty or null";

  static {
    performWarmup();
  }

  private static void performWarmup() {
    // Initialize some core dependencies in parallel.  This can speed up startup by 150+ ms.
    ExecutorService connectionStartupExecutor = Executors
        .newCachedThreadPool(ThreadUtil.getThreadFactory("BigtableSession-startup-%d", true));

    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of BigtableSessionSharedThreadPools.getInstance() is expensive.
        // Reference it so that it gets constructed asynchronously.
        BigtableSessionSharedThreadPools.getInstance();
      }
    });
    for (final String host : Arrays.asList(BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT,
      BigtableOptions.BIGTABLE_ADMIN_HOST_DEFAULT)) {
      connectionStartupExecutor.execute(new Runnable() {
        @Override
        public void run() {
          // The first invocation of InetAddress retrieval is expensive.
          // Reference it so that it gets constructed asynchronously.
          try {
            InetAddress.getByName(host);
          } catch (UnknownHostException e) {
            // ignore.  This doesn't happen frequently, but even if it does, it's inconsequential.
          }
        }
      });
    }
    connectionStartupExecutor.shutdown();
  }

  private synchronized static ResourceLimiter initializeResourceLimiter(BigtableOptions options) {
    BigtableInstanceName instanceName = options.getInstanceName();
    String key = instanceName.toString();
    ResourceLimiter resourceLimiter = resourceLimiterMap.get(key);
    if (resourceLimiter == null) {
      int maxInflightRpcs = options.getBulkOptions().getMaxInflightRpcs();
      long maxMemory = options.getBulkOptions().getMaxMemory();
      ResourceLimiterStats stats = ResourceLimiterStats.getInstance(instanceName);
      resourceLimiter = new ResourceLimiter(stats, maxMemory, maxInflightRpcs);
      BulkOptions bulkOptions = options.getBulkOptions();
      if (bulkOptions.isEnableBulkMutationThrottling()) {
        resourceLimiter.throttle(bulkOptions.getBulkMutationRpcTargetMs());
      }
      resourceLimiterMap.put(key, resourceLimiter);
    }
    return resourceLimiter;
  }

  private Watchdog watchdog;
  private final BigtableDataClient dataClient;

  private final IBigtableDataClient bigtableDataClient;

  // This BigtableDataClient has an additional throttling interceptor, which is not recommended for
  // synchronous operations.
  private final BigtableDataClient throttlingDataClient;

  private BigtableTableAdminClient tableAdminClient;
  private BigtableInstanceGrpcClient instanceAdminClient;

  private final BigtableOptions options;
  private final List<ManagedChannel> managedChannels = Collections
      .synchronizedList(new ArrayList<ManagedChannel>());
  private final ClientInterceptor[] clientInterceptors;

  /**
   * This cluster name is either configured via BigtableOptions' clusterId, or via a lookup of the
   * clusterID based on BigtableOptions projectId and instanceId.  See {@link BigtableClusterUtilities}
   */
  private BigtableClusterName clusterName;

  /**
   * <p>Constructor for BigtableSession.</p>
   *
   * @param opts a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   * @throws java.io.IOException if any.
   */
  public BigtableSession(BigtableOptions opts) throws IOException {
    this.options = opts;
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getProjectId()), PROJECT_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getInstanceId()), INSTANCE_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getUserAgent()), USER_AGENT_EMPTY_OR_NULL);
    LOG.info(
        "Opening connection for projectId %s, instanceId %s, "
        + "on data host %s, admin host %s.",
        options.getProjectId(), options.getInstanceId(), options.getDataHost(),
        options.getAdminHost());
    LOG.info("Bigtable options: %s.", options);

    List<ClientInterceptor> clientInterceptorsList = new ArrayList<>();
    clientInterceptorsList
        .add(new GoogleCloudResourcePrefixInterceptor(options.getInstanceName().toString()));

    CredentialInterceptorCache credentialsCache = CredentialInterceptorCache.getInstance();
    RetryOptions retryOptions = options.getRetryOptions();
    CredentialOptions credentialOptions = options.getCredentialOptions();

    try {
      ClientInterceptor credentialsInterceptor =
          credentialsCache.getCredentialsInterceptor(credentialOptions, retryOptions);
      if (credentialsInterceptor != null) {
        clientInterceptorsList.add(credentialsInterceptor);
      }
    } catch (GeneralSecurityException e) {
      throw new IOException("Could not initialize credentials.", e);
    }

    clientInterceptorsList.add(setupWatchdog());

    clientInterceptors =
        clientInterceptorsList.toArray(new ClientInterceptor[clientInterceptorsList.size()]);

    Channel dataChannel = getDataChannelPool();

    BigtableSessionSharedThreadPools sharedPools = BigtableSessionSharedThreadPools.getInstance();

    // More often than not, users want the dataClient. Create a new one in the constructor.
    CallOptionsFactory.ConfiguredCallOptionsFactory callOptionsFactory =
        new CallOptionsFactory.ConfiguredCallOptionsFactory(options.getCallOptionsConfig());
    dataClient =
        new BigtableDataGrpcClient(dataChannel, sharedPools.getRetryExecutor(), options);
    dataClient.setCallOptionsFactory(callOptionsFactory);

    this.bigtableDataClient = ((IBigtableDataClient) dataClient);

    // Async operations can run amok, so they need to have some throttling. The throttling is
    // achieved through a ThrottlingClientInterceptor.  gRPC wraps ClientInterceptors in Channels,
    // and since a new Channel is needed, a new BigtableDataGrpcClient instance is needed as well.
    //
    // Throttling should not be used in blocking operations, or streaming reads. We have not tested
    // the impact of throttling on blocking operations.
    ResourceLimiter resourceLimiter = initializeResourceLimiter(options);
    Channel asyncDataChannel =
        ClientInterceptors.intercept(dataChannel, new ThrottlingClientInterceptor(resourceLimiter));
    throttlingDataClient =
        new BigtableDataGrpcClient(asyncDataChannel, sharedPools.getRetryExecutor(), options);

    BigtableClientMetrics.counter(MetricLevel.Info, "sessions.active").inc();

    // Defer the creation of both the tableAdminClient until we need them.
    }

  private ManagedChannel getDataChannelPool() throws IOException {
    String host = options.getDataHost();
    int channelCount = options.getChannelCount();
    if (options.useCachedChannel()) {
      synchronized (BigtableSession.class) {
        // TODO: Ensure that the host and channelCount are the same.
        if (cachedDataChannelPool == null) {
          cachedDataChannelPool = createChannelPool(host, channelCount);
        }
        return cachedDataChannelPool;
      }
    }
    return createManagedPool(host, channelCount);
  }

  private WatchdogInterceptor setupWatchdog() {
    Preconditions.checkState(watchdog == null, "Watchdog already setup");

    watchdog = new Watchdog(Clock.SYSTEM,
        options.getRetryOptions().getReadPartialRowTimeoutMillis());
    watchdog.start(BigtableSessionSharedThreadPools.getInstance().getRetryExecutor());

    return new WatchdogInterceptor(
        ImmutableSet.<MethodDescriptor<?, ?>>of(BigtableGrpc.getReadRowsMethod()),
        watchdog);
  }

  /**
   * Snapshot operations need various aspects of a {@link BigtableClusterName}. This method gets a
   * clusterId from either a lookup (projectId and instanceId translate to a single clusterId when
   * an instance has only one cluster).
   */
  public synchronized BigtableClusterName getClusterName() throws IOException {
    if (this.clusterName == null) {
      try (BigtableClusterUtilities util = new BigtableClusterUtilities(options)) {
        ListClustersResponse clusters = util.getClusters();
        Preconditions.checkState(clusters.getClustersCount() == 1,
          String.format(
            "Project '%s' / Instance '%s' has %d clusters. There must be exactly 1 for this operation to work.",
            options.getProjectId(), options.getInstanceId(), clusters.getClustersCount()));
        clusterName = new BigtableClusterName(clusters.getClusters(0).getName());
      } catch (GeneralSecurityException e) {
        throw new IOException("Could not get cluster Id.", e);
      }
    }
    return clusterName;
  }

  /**
   * <p>Getter for the field <code>dataClient</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableDataClient} object.
   */
  public BigtableDataClient getDataClient() {
    return dataClient;
  }

  /**
   * <p>Getter for the field <code>IBigtableDataClient</code>.</p>
   *
   * @return a {@link IBigtableDataClient} object.
   */
  public IBigtableDataClient getBigtableDataClient() {
    return bigtableDataClient;
  }

  /**
   * <p>createAsyncExecutor.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.async.AsyncExecutor} object.
   */
  public AsyncExecutor createAsyncExecutor() {
    return new AsyncExecutor(throttlingDataClient);
  }

  /**
   * <p>createBulkMutation.</p>
   *
   * @param tableName a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   * @param asyncExecutor a {@link com.google.cloud.bigtable.grpc.async.AsyncExecutor} object.
   * @return a {@link com.google.cloud.bigtable.grpc.async.BulkMutation} object.
   * @deprecated use {@link #createBulkMutation(BigtableTableName)} instead.
   */
  @Deprecated
  public BulkMutation createBulkMutation(BigtableTableName tableName, AsyncExecutor asyncExecutor) {
    return createBulkMutation(tableName);
  }

  /**
   * <p>createBulkMutation.</p>
   *
   * @param tableName a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   * @return a {@link com.google.cloud.bigtable.grpc.async.BulkMutation} object.
   */
  public BulkMutation createBulkMutation(BigtableTableName tableName) {
    return new BulkMutation(
        tableName,
        throttlingDataClient,
        BigtableSessionSharedThreadPools.getInstance().getRetryExecutor(),
        options.getBulkOptions());
  }

  /**
   * <p>createBulkRead.</p>
   *
   * @param tableName a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   * @return a {@link com.google.cloud.bigtable.grpc.async.BulkRead} object.
   */
  public BulkRead createBulkRead(BigtableTableName tableName) {
    return new BulkRead(dataClient, tableName, options.getBulkOptions().getBulkMaxRowKeyCount(),
        BigtableSessionSharedThreadPools.getInstance().getBatchThreadPool()
    );
  }

  /**
   * <p>Getter for the field <code>tableAdminClient</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableTableAdminClient} object.
   * @throws java.io.IOException if any.
   */
  public synchronized BigtableTableAdminClient getTableAdminClient() throws IOException {
    if (tableAdminClient == null) {
      ManagedChannel channel = createManagedPool(options.getAdminHost(), 1);
      tableAdminClient = new BigtableTableAdminGrpcClient(channel,
          BigtableSessionSharedThreadPools.getInstance().getRetryExecutor(), options);
    }
    return tableAdminClient;
  }

  /**
   * <p>Getter for the field <code>instanceAdminClient</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableInstanceClient} object.
   * @throws java.io.IOException if any.
   */
  public synchronized BigtableInstanceClient getInstanceAdminClient() throws IOException {
    if (instanceAdminClient == null) {
      ManagedChannel channel = createManagedPool(options.getAdminHost(), 1);
      instanceAdminClient = new BigtableInstanceGrpcClient(channel);
    }
    return instanceAdminClient;
  }

  /**
   * Create a new {@link com.google.cloud.bigtable.grpc.io.ChannelPool}, with auth headers.
   *
   * @param hostString a {@link java.lang.String} object.
   * @return a {@link com.google.cloud.bigtable.grpc.io.ChannelPool} object.
   * @throws java.io.IOException if any.
   */
  protected ManagedChannel createChannelPool(final String hostString, int count) throws IOException {
    ChannelPool.ChannelFactory channelFactory = new ChannelPool.ChannelFactory() {
      @Override
      public ManagedChannel create() throws IOException {
        return createNettyChannel(hostString, options, clientInterceptors);
      }
    };
    return createChannelPool(channelFactory, count);
  }

  /**
   * Create a new {@link com.google.cloud.bigtable.grpc.io.ChannelPool}, with auth headers.  This
   * method allows users to override the default implementation with their own.
   *
   * @param channelFactory a {@link ChannelPool.ChannelFactory} object.
   * @param count The number of channels in the pool.
   * @return a {@link com.google.cloud.bigtable.grpc.io.ChannelPool} object.
   * @throws java.io.IOException if any.
   */
  protected ManagedChannel createChannelPool(final ChannelPool.ChannelFactory channelFactory, int count)
      throws IOException {
    return new ChannelPool(channelFactory, count);
  }

  /**
   * Create a new {@link com.google.cloud.bigtable.grpc.io.ChannelPool}, with auth headers, that
   * will be cleaned up when the connection closes.
   *
   * @param host a {@link java.lang.String} object.
   * @return a {@link com.google.cloud.bigtable.grpc.io.ChannelPool} object.
   * @throws java.io.IOException if any.
   */
  protected ManagedChannel createManagedPool(String host, int channelCount) throws IOException {
    ManagedChannel channelPool = createChannelPool(host, channelCount);
    managedChannels.add(channelPool);
    return channelPool;
  }

  /**
   * Create a {@link BigtableInstanceClient}.  {@link BigtableSession} objects assume that
   * {@link BigtableOptions} have a project and instance.  A {@link BigtableInstanceClient} does not
   * require project id or instance id, so {@link BigtableOptions#getDefaultOptions()} may be used
   * if there are no service account credentials settings.
   *
   * @return a fully formed {@link BigtableInstanceClient}
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static BigtableInstanceClient createInstanceClient(BigtableOptions options)
      throws IOException, GeneralSecurityException {
    return new BigtableInstanceGrpcClient(createChannelPool(options.getAdminHost(), options));
  }

  /**
   * Create a new {@link com.google.cloud.bigtable.grpc.io.ChannelPool}, with auth headers.
   *
   * @param host a {@link String} object.
   * @param options a {@link BigtableOptions} object.
   * @return a {@link ChannelPool} object.
   * @throws IOException if any.
   * @throws GeneralSecurityException
   */
  public static ManagedChannel createChannelPool(final String host, final BigtableOptions options)
      throws IOException, GeneralSecurityException {
    return createChannelPool(host, options, 1);
  }

  /**
   * Create a new {@link com.google.cloud.bigtable.grpc.io.ChannelPool}, with auth headers.
   *
   * @param host a {@link String} object specifying the host to connect to.
   * @param options a {@link BigtableOptions} object with the credentials, retry and other connection options.
   * @param count an int defining the number of channels to create
   * @return a {@link ChannelPool} object.
   * @throws IOException if any.
   * @throws GeneralSecurityException
   */
  public static ManagedChannel createChannelPool(final String host, final BigtableOptions options,
      int count) throws IOException, GeneralSecurityException {
    final List<ClientInterceptor> interceptorList = new ArrayList<>();

    ClientInterceptor credentialsInterceptor = CredentialInterceptorCache.getInstance()
            .getCredentialsInterceptor(options.getCredentialOptions(), options.getRetryOptions());
    if (credentialsInterceptor != null) {
      interceptorList.add(credentialsInterceptor);
    }

    if (options.getInstanceName() != null) {
      interceptorList
          .add(new GoogleCloudResourcePrefixInterceptor(options.getInstanceName().toString()));
    }
    final ClientInterceptor[] interceptors =
        interceptorList.toArray(new ClientInterceptor[interceptorList.size()]);

    ChannelPool.ChannelFactory factory = new ChannelPool.ChannelFactory() {
      @Override
      public ManagedChannel create() throws IOException {
        return createNettyChannel(host, options, interceptors);
      }
    };
    return new ChannelPool(factory, count);
  }

  /**
   * <p>createNettyChannel.</p>
   *
   * @param host a {@link String} object.
   * @param options a {@link BigtableOptions} object.
   * @return a {@link ManagedChannel} object.
   * @throws SSLException if any.
   */
  public static ManagedChannel createNettyChannel(String host,
      BigtableOptions options, ClientInterceptor ... interceptors) throws SSLException {

    // Ideally, this should be ManagedChannelBuilder.forAddress(...) rather than an explicit
    // call to NettyChannelBuilder.  Unfortunately, that doesn't work for shaded artifacts.
    ManagedChannelBuilder<?> builder = ManagedChannelBuilder
        .forAddress(host, options.getPort());

    if (options.usePlaintextNegotiation()) {
      // NOTE: usePlaintext(true) is deprecated in newer versions of grpc (1.11.0).
      //       usePlantxext() is the preferred approach, but won't work with older versions.
      //       This means that plaintext negotiation can't be used with Beam.
      builder.usePlaintext();
    }

    return builder
        .idleTimeout(Long.MAX_VALUE, TimeUnit.SECONDS)
        .maxInboundMessageSize(MAX_MESSAGE_SIZE)
        .userAgent(BigtableVersionInfo.CORE_UESR_AGENT + "," + options.getUserAgent())
        .intercept(interceptors)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    watchdog.stop();

    if (managedChannels.isEmpty()) {
      return;
    }
    long timeoutNanos = TimeUnit.SECONDS.toNanos(10);
    long endTimeNanos = System.nanoTime() + timeoutNanos;
    for (ManagedChannel channel : managedChannels) {
      channel.shutdown();
    }
    for (ManagedChannel channel : managedChannels) {
      long awaitTimeNanos = endTimeNanos - System.nanoTime();
      if (awaitTimeNanos <= 0) {
        break;
      }
      try {
        channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while closing the channelPools");
      }
    }
    for (ManagedChannel channel : managedChannels) {
      if (!channel.isTerminated()) {
        // Sometimes, gRPC channels don't close properly. We cannot explain why that happens,
        // nor can we reproduce the problem reliably. However, that doesn't actually cause
        // problems. Synchronous RPCs will throw exceptions right away. Buffered Mutator based
        // async operations are already logged. Direct async operations may have some trouble,
        // but users should not currently be using them directly.
        //
        // NOTE: We haven't seen this problem since removing the RefreshingChannel
        LOG.info("Could not close the channel after 10 seconds.");
        break;
      }
    }
    managedChannels.clear();
    BigtableClientMetrics.counter(MetricLevel.Info, "sessions.active").dec();
  }

  /**
   * <p>Getter for the field <code>options</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   */
  public BigtableOptions getOptions() {
    return this.options;
  }
}
