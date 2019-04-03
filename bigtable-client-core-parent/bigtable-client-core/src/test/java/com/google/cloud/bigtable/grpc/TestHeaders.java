/*
 * Copyright 2019 Google LLC.
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

import com.google.api.core.ApiFunction;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ApiClientHeaderProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.bigtable.v2.BigtableGrpc.BigtableImplBase;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BigtableVeneerSettingsFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.common.io.Resources;
import io.grpc.ForwardingServerCall;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.net.ServerSocket;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.net.ssl.SSLException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static io.grpc.internal.GrpcUtil.USER_AGENT_KEY;

/**
 * This class tests value present in User-Agent's on netty server.
 */
@RunWith(JUnit4.class)
public class TestHeaders {

  private static final Logger logger = new Logger(TestHeaders.class);

  private static final String TEST_PROJECT_ID = "ProjectId";
  private static final String TEST_INSTANCE_ID = "InstanceId";
  private static final String TEST_USER_AGENT = "test-user-agent";
  private static final Pattern EXPECTED_HEADER_PATTERN =
      Pattern.compile(".*" + TEST_USER_AGENT + ".*");
  private static final String TABLE_ID = "my-table-id";
  private static final String ROWKEY = "row-key";

  private BigtableDataSettings dataSettings;
  private BigtableDataClient dataClient;
  private Server server;
  private AtomicBoolean serverPasses = new AtomicBoolean(false);
  private Pattern xGoogApiPattern;

  @After
  public void tearDown() throws Exception {
    if(dataClient != null){
      dataClient.close();
      dataClient = null;
    }
    if (server != null) {
      server.shutdown();
      server.awaitTermination();
      server = null;
    }
    serverPasses.set(false);
    xGoogApiPattern = null;
  }

  /**
   * To Test Headers & PlainText Negotiation type
   * when cloud-bigtable-client {@link com.google.cloud.bigtable.grpc.BigtableDataClient}.
   */
  @Test
  public void testCBC_UserAgentUsingPlainTextNegotiation() throws Exception{
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();

    //Creates non-ssl server.
    createServer(availablePort);

    BigtableOptions bigtableOptions =
        BigtableOptions.builder()
            .setDataHost("localhost")
            .setAdminHost("localhost")
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setUserAgent(TEST_USER_AGENT)
            .setUsePlaintextNegotiation(true)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setPort(availablePort)
            .build();

    xGoogApiPattern = Pattern.compile(".* cbt/.*");
    try (BigtableSession session = new BigtableSession(bigtableOptions)) {
      session.getClientWrapper()
          .readFlatRows(Query.create("fake-table")).next();
      Assert.assertTrue(serverPasses.get());
    }
  }

  /**
   * To Test UserAgent & PlainText Negotiation type
   * when {@link BigtableDataSettings} is created using {@link BigtableOptions}.
   */
  @Test
  public void testGCJ_UserAgentUsingPlainTextNegotiation() throws Exception {
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();

    //Creates non-ssl server.
    createServer(availablePort);

    BigtableOptions bigtableOptions =
        BigtableOptions.builder()
            .setDataHost("localhost")
            .setAdminHost("localhost")
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setUserAgent(TEST_USER_AGENT)
            .setUsePlaintextNegotiation(true)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setPort(availablePort)
            .build();

    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(bigtableOptions);

    xGoogApiPattern = Pattern.compile(".* gapic/.*");
    try(BigtableDataClient dataClient = BigtableDataClient.create(dataSettings)){
      dataClient.readRow(TABLE_ID, ROWKEY);
      Assert.assertTrue(serverPasses.get());
    }
  }

  /**
   * Verify userAgent on TLS Negotiation with {@link InstantiatingGrpcChannelProvider} using SSL
   * enabled server.
   */
  @Test
  public void testGCJ_UserAgentUsingTLSNegotiation() throws Exception {
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();

    //Creates SSL enabled server.
    createSecuredServer(availablePort);

    BigtableDataSettings.Builder builder =
        BigtableDataSettings.newBuilder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setCredentialsProvider(NoCredentialsProvider.create());

    //Loads secured Certificate
    final SslContext sslContext  = buildSslContext();

    final String endpoint = "localhost" + ":" + availablePort;
    HeaderProvider headers = FixedHeaderProvider.create(USER_AGENT_KEY.name(), TEST_USER_AGENT);

    builder.setTransportChannelProvider(InstantiatingGrpcChannelProvider.newBuilder()
        .setHeaderProvider(headers)
        .setEndpoint(endpoint)
        .setChannelConfigurator(new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
          @Override
          public ManagedChannelBuilder apply(ManagedChannelBuilder input) {
            return ((NettyChannelBuilder)input).sslContext(sslContext);
          }
        })
        .build());

    // Setting this to null, because as of 3/4/2019 the header doesn't get passed through.
    xGoogApiPattern = null;
    try (BigtableDataClient dataClient = BigtableDataClient.create(builder.build())) {
      dataClient.readRow(TABLE_ID, ROWKEY);
      Assert.assertTrue(serverPasses.get());
    }
  }

  /** Creates simple server to intercept plainText Negotiation RPCs. */
  private void createServer(int port) throws Exception{
    server = ServerBuilder.forPort(port).addService(
        ServerInterceptors.intercept(new BigtableExtendedImpl(), new HeaderServerInterceptor()))
        .build();
    server.start();
  }

  /** Creates secured server to intercept TLS Negotiation RPCs. */
  private void createSecuredServer(int port) throws Exception {
    ServerBuilder builder = ServerBuilder.forPort(port).addService(
        ServerInterceptors.intercept(new BigtableExtendedImpl(), new HeaderServerInterceptor()));

    try {
      URL serverCertChain = Resources.getResource("sslCertificates/server_trust.crt");
      URL privateKey = Resources.getResource("sslCertificates/server_key.pem");

      builder.useTransportSecurity(new File(serverCertChain.getFile()),
          new File(privateKey.getFile()));
    } catch (Exception ex) {
      throw new AssertionError("No server certificates found");
    }
    server = builder.build();
    server.start();
  }

  /** Creates SSLContext from certificate & private key provided in classpath. */
  private static SslContext buildSslContext() throws SSLException {
    SslContextBuilder builder = GrpcSslContexts.forClient();

    try {
      URL url = Resources.getResource("sslCertificates/client_trust.crt");
      builder.trustManager(new File(url.getFile()));
    } catch (Exception ex) {
      throw new AssertionError("No client trust certificate found");
    }

    return builder.build();
  }

  /**
   * Overrides {@link BigtableImplBase#readRows(ReadRowsRequest, StreamObserver)} and returns
   * dummy response.
   * */
  private static class BigtableExtendedImpl extends BigtableImplBase {
    @Override
    public void readRows(ReadRowsRequest request,
        StreamObserver<ReadRowsResponse> responseObserver) {
      responseObserver.onNext(ReadRowsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  /**
   * Asserts value of UserAgent header with EXPECTED_HEADER_PATTERN passed to the
   * {@link InstantiatingGrpcChannelProvider}.
   *
   * Throws {@link AssertionError} when UserAgent's pattern does not match.
   */
  private class HeaderServerInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
        final Metadata requestHeaders, ServerCallHandler<ReqT, RespT> next) {

      //Logging all available headers.
      logger.info("headers received from BigtableDataClient:" + requestHeaders);

      testHeader(requestHeaders, "user-agent", EXPECTED_HEADER_PATTERN);
      if (xGoogApiPattern != null) {
        testHeader(requestHeaders, ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            xGoogApiPattern);
      }

      // Add a test for the prefix header.  As of 3/4/2019, cloud-bigtable-client uses a different
      // header than google-cloud-java

      serverPasses.set(true);

      return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
      }, requestHeaders);
    }

    protected void testHeader(Metadata requestHeaders, String keyName, Pattern pattern) {
      Metadata.Key<String> key =
          Metadata.Key.of(keyName, Metadata.ASCII_STRING_MARSHALLER);
      String headerValue = requestHeaders.get(key);

      //In case of user-agent not matching, throwing AssertionError.
      if (headerValue == null || !pattern.matcher(headerValue).matches()) {
        throw new AssertionError(keyName + "'s format did not match.  header: " + headerValue);
      }
    }
  }
}
