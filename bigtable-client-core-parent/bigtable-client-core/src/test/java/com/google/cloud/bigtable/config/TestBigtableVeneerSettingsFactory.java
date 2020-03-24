/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.bigtable.config;

import static com.google.cloud.bigtable.config.CallOptionsConfig.SHORT_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_BACKOFF_MULTIPLIER;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_INITIAL_BACKOFF_MILLIS;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class TestBigtableVeneerSettingsFactory {

  private static final Logger LOG = new Logger(TestBigtableVeneerSettingsFactory.class);

  private static final String ACTUAL_PROJECT_ID = System.getProperty("test.client.project.id");
  private static final String ACTUAL_INSTANCE_ID = System.getProperty("test.client.instance.id");

  private static final String TEST_PROJECT_ID = "fakeProjectID";
  private static final String TEST_INSTANCE_ID = "fakeInstanceID";
  private static final String TEST_USER_AGENT = "sampleUserAgent";

  /** RetryCodes for idempotent Rpcs. */
  private static final Set<Code> DEFAULT_RETRY_CODES =
      ImmutableSet.of(Code.DEADLINE_EXCEEDED, Code.UNAVAILABLE, Code.ABORTED, Code.UNAUTHENTICATED);

  private static final boolean endToEndArgMissing =
      Strings.isNullOrEmpty(ACTUAL_PROJECT_ID) && Strings.isNullOrEmpty(ACTUAL_INSTANCE_ID);

  @Rule public ExpectedException expectException = ExpectedException.none();

  private BigtableOptions bigtableOptions;

  private BigtableDataSettings dataSettings;
  private BigtableTableAdminSettings adminSettings;
  private BigtableDataClient dataClient;
  private BigtableTableAdminClient adminClient;

  @Before
  public void setUp() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();

    bigtableOptions =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setUserAgent(TEST_USER_AGENT)
            .setAdminHost("localhost")
            .setDataHost("localhost")
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setPort(availablePort)
            .build();
  }

  @After
  public void tearDown() throws Exception {
    if (dataClient != null) {
      dataClient.close();
    }

    if (adminClient != null) {
      adminClient.close();
    }
  }

  private void initializeClients() throws IOException {
    String josnPath = CredentialOptions.getEnvJsonFile();
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(ACTUAL_PROJECT_ID)
            .setInstanceId(ACTUAL_INSTANCE_ID)
            .setUserAgent("native-bigtable-test")
            .setCredentialOptions(CredentialOptions.jsonCredentials(new FileInputStream(josnPath)))
            .build();

    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(options);
    dataClient = BigtableDataClient.create(dataSettings);

    adminSettings = BigtableVeneerSettingsFactory.createTableAdminSettings(options);
    adminClient = BigtableTableAdminClient.create(adminSettings);
  }

  /**
   * This test runs only if it finds "test.client.project.id" & "test.client.project.id" VM
   * arguments. Then it calls to an actual Bigtable Table & performs the checks below:
   *
   * <pre>
   *   <ul>
   *     <li>Checks if table with TABLE_ID exists.</li>
   *     <li>Creates a new table with TABLE_ID.</li>
   *     <li>Mutates a single row with {@link RowMutation}.</li>
   *     <li>Retrieves output in {@link ServerStream < Row >}.</li>
   *     <li>Deletes table created with TABLE_ID.</li>
   *   </ul>
   * </pre>
   */
  @Test
  public void testWithActualTables() throws Exception {
    // Checking if both arguments are available or not.
    Assume.assumeFalse(endToEndArgMissing);

    if (adminClient == null || dataClient == null) {
      initializeClients();
    }

    final String TABLE_ID = "Test-clients-" + UUID.randomUUID().toString();
    final String COLUMN_FAMILY_ID = "CF1";
    final ByteString TEST_QUALIFER = ByteString.copyFromUtf8("qualifier1");
    final ByteString TEST_KEY = ByteString.copyFromUtf8("bigtableDataSettingTest");
    final ByteString TEST_VALUE =
        ByteString.copyFromUtf8("Test using BigtableDataclient & " + "BigtableTableAdminClient");

    // Checking if table already existed in the provided instance.
    if (adminClient.exists(TABLE_ID)) {
      adminClient.deleteTable(TABLE_ID);
    }
    try {
      CreateTableRequest createTableRequest =
          CreateTableRequest.of(TABLE_ID).addFamily(COLUMN_FAMILY_ID);
      adminClient.createTable(createTableRequest);

      // Created table with vaneer TableAdminClient.
      boolean tableExist = adminClient.exists(TABLE_ID);
      LOG.info("Table successfully created : " + tableExist);
      assertTrue(tableExist);

      Mutation mutation = Mutation.create();
      mutation.setCell(COLUMN_FAMILY_ID, TEST_QUALIFER, TEST_VALUE);
      RowMutation rowMutation = RowMutation.create(TABLE_ID, TEST_KEY, mutation);

      // Write content to Bigtable using vaneer DataClient.
      dataClient.mutateRow(rowMutation);
      LOG.info("Successfully Mutated");

      Query query = Query.create(TABLE_ID);
      ServerStream<Row> rowStream = dataClient.readRows(query);
      for (Row outputRow : rowStream) {

        // Checking if the received output's KEY is same as above.
        ByteString key = outputRow.getKey();
        LOG.info("found key: " + key.toStringUtf8());
        assertEquals(TEST_KEY, outputRow.getKey());

        for (RowCell cell : outputRow.getCells()) {
          // Checking if the received output is KEY sent above.
          ByteString value = cell.getValue();
          LOG.info("Value found: " + value.toStringUtf8());
          assertEquals(TEST_VALUE, value);
        }
      }

      // Removing the table.
      adminClient.deleteTable(TABLE_ID);
    } finally {
      // Removing Table in case of exceptions.
      boolean tableExist = adminClient.exists(TABLE_ID);
      if (tableExist) {
        adminClient.deleteTable(TABLE_ID);
      }
      assertFalse(adminClient.exists(TABLE_ID));
    }
  }

  @Test
  public void testWhenRetriesAreDisabled() throws IOException {
    RetryOptions retryOptions = RetryOptions.builder().setEnableRetries(false).build();
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setRetryOptions(retryOptions)
            .build();

    expectException.expect(IllegalStateException.class);
    expectException.expectMessage("Disabling retries is not currently supported.");
    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(options);
  }

  @Test
  public void testWithNullCredentials() throws IOException {
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setUserAgent(TEST_USER_AGENT)
            .build();
    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(options);
    assertTrue(
        dataSettings.getStubSettings().getCredentialsProvider() instanceof NoCredentialsProvider);
  }

  @Test
  public void testConfigValues() throws IOException {
    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(bigtableOptions);

    // Streaming operation's RetrySettings & RetryCodes of retryable methods.
    // sampleRowKeys
    verifyRetry(dataSettings.getStubSettings().sampleRowKeysSettings().getRetrySettings());
    assertEquals(
        DEFAULT_RETRY_CODES,
        dataSettings.getStubSettings().sampleRowKeysSettings().getRetryableCodes());

    // mutateRowSettings
    verifyRetry(dataSettings.getStubSettings().mutateRowSettings().getRetrySettings());
    assertEquals(
        DEFAULT_RETRY_CODES,
        dataSettings.getStubSettings().mutateRowSettings().getRetryableCodes());

    // bulkMutationsSettings
    verifyRetry(dataSettings.getStubSettings().bulkMutateRowsSettings().getRetrySettings());
    assertEquals(
        DEFAULT_RETRY_CODES,
        dataSettings.getStubSettings().bulkMutateRowsSettings().getRetryableCodes());

    // bulkReadRowsSettings
    verifyRetry(dataSettings.getStubSettings().bulkReadRowsSettings().getRetrySettings());
    assertEquals(
        DEFAULT_RETRY_CODES,
        dataSettings.getStubSettings().bulkReadRowsSettings().getRetryableCodes());

    // Non-streaming operation's verifying RetrySettings & RetryCodes of non-retryable methods.
    // readModifyWriteRowSettings
    verifyDisabledRetry(
        dataSettings.getStubSettings().readModifyWriteRowSettings().getRetrySettings());
    assertTrue(
        dataSettings.getStubSettings().readModifyWriteRowSettings().getRetryableCodes().isEmpty());

    // checkAndMutateRowSettings
    verifyDisabledRetry(
        dataSettings.getStubSettings().checkAndMutateRowSettings().getRetrySettings());
    assertTrue(
        dataSettings.getStubSettings().checkAndMutateRowSettings().getRetryableCodes().isEmpty());
  }

  private void verifyRetry(RetrySettings retrySettings) {
    assertEquals(DEFAULT_INITIAL_BACKOFF_MILLIS, retrySettings.getInitialRetryDelay().toMillis());
    assertEquals(DEFAULT_BACKOFF_MULTIPLIER, retrySettings.getRetryDelayMultiplier(), 0);
    assertEquals(0, retrySettings.getMaxAttempts());
    assertEquals(360_000, retrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(360_000, retrySettings.getMaxRpcTimeout().toMillis());
    assertEquals(DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS, retrySettings.getTotalTimeout().toMillis());
  }

  private void verifyDisabledRetry(RetrySettings retrySettings) {
    assertEquals(Duration.ZERO, retrySettings.getInitialRetryDelay());
    assertEquals(1, retrySettings.getRetryDelayMultiplier(), 0);
    assertEquals(Duration.ZERO, retrySettings.getMaxRetryDelay());
    assertEquals(1, retrySettings.getMaxAttempts());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, retrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, retrySettings.getMaxRpcTimeout().toMillis());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, retrySettings.getTotalTimeout().toMillis());
    assertEquals(1, retrySettings.getMaxAttempts());
  }

  @Test
  public void testWhenBulkOptionIsDisabled() throws IOException {
    BulkOptions bulkOptions = BulkOptions.builder().setUseBulkApi(false).build();
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setBulkOptions(bulkOptions)
            .build();
    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(options);
    assertFalse(
        dataSettings
            .getStubSettings()
            .bulkMutateRowsSettings()
            .getBatchingSettings()
            .getIsEnabled());
  }

  @Test
  public void testBulkMutation() throws IOException {
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setUserAgent("Test-user-agent")
            .build();
    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(options);

    BulkOptions bulkOptions = options.getBulkOptions();
    BatchingSettings batchingSettings =
        dataSettings.getStubSettings().bulkMutateRowsSettings().getBatchingSettings();
    long outstandingElementCount =
        bulkOptions.getMaxInflightRpcs() * bulkOptions.getBulkMaxRowKeyCount();

    assertTrue(batchingSettings.getIsEnabled());
    assertEquals(
        bulkOptions.getBulkMaxRequestSize(),
        batchingSettings.getRequestByteThreshold().longValue());
    assertEquals(
        bulkOptions.getBulkMaxRowKeyCount(),
        batchingSettings.getElementCountThreshold().longValue());
    assertEquals(
        bulkOptions.getMaxMemory(),
        batchingSettings.getFlowControlSettings().getMaxOutstandingRequestBytes().longValue());
    assertEquals(
        outstandingElementCount,
        batchingSettings.getFlowControlSettings().getMaxOutstandingElementCount().longValue());
  }

  @Test
  public void testTableAdminWithNullCredentials() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setUserAgent(TEST_USER_AGENT)
            .setAdminHost("localhost")
            .setPort(availablePort)
            .build();
    adminSettings = BigtableVeneerSettingsFactory.createTableAdminSettings(options);
    assertTrue(
        adminSettings.getStubSettings().getCredentialsProvider() instanceof NoCredentialsProvider);
  }

  @Test
  public void testBulkReadConfig() throws IOException {
    int maxRowKeyCount = 1_000_000;
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setUserAgent("Test-user-agent")
            .setBulkOptions(BulkOptions.builder().setBulkMaxRowKeyCount(maxRowKeyCount).build())
            .build();
    dataSettings = BigtableVeneerSettingsFactory.createBigtableDataSettings(options);

    assertEquals(
        maxRowKeyCount,
        dataSettings
            .getStubSettings()
            .bulkReadRowsSettings()
            .getBatchingSettings()
            .getElementCountThreshold()
            .intValue());
  }
}
