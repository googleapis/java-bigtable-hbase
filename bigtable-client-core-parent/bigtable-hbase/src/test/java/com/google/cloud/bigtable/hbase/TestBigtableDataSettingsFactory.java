/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.config.CallOptionsConfig.LONG_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.CallOptionsConfig.SHORT_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_BACKOFF_MULTIPLIER;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_INITIAL_BACKOFF_MILLIS;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.common.collect.ImmutableSet;

@RunWith(JUnit4.class)
public class TestBigtableDataSettingsFactory {

  private static final String TEST_USER_AGENT = "sampleUserAgent";
  private static final Set<Code> DEFAULT_RETRY_CODES =
      ImmutableSet.of(Code.DEADLINE_EXCEEDED, Code.UNAVAILABLE, Code.ABORTED);

  @Rule
  public ExpectedException expectException = ExpectedException.none();

  private BigtableOptions bigtableOptions;

  @Before
  public void setup() {
    bigtableOptions = BigtableOptions.builder()
        .setProjectId(TEST_PROJECT_ID).setInstanceId(TEST_INSTANCE_ID)
        .build();
  }

  @Test
  public void testProjectIdIsRequired() throws IOException, GeneralSecurityException {
    BigtableOptions options = BigtableOptions.builder().build();

    expectException.expect(IllegalStateException.class);
    expectException.expectMessage("Project ID is required");
    BigtableDataSettingsFactory.fromBigtableOptions(options);
  }

  @Test
  public void testInstanceIdIsRequired() throws IOException, GeneralSecurityException {
    BigtableOptions options = BigtableOptions.builder().setProjectId(TEST_PROJECT_ID).build();

    expectException.expect(IllegalStateException.class);
    expectException.expectMessage("Instance ID is required");
    BigtableDataSettingsFactory.fromBigtableOptions(options);
  }

  @Test
  public void testWhenRetriesAreDisabled() throws IOException, GeneralSecurityException {
    RetryOptions retryOptions = RetryOptions.builder().setEnableRetries(false).build();
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID).setInstanceId(TEST_INSTANCE_ID)
            .setRetryOptions(retryOptions).build();

    expectException.expect(IllegalStateException.class);
    expectException.expectMessage("Disabling retries is not currently supported.");
    BigtableDataSettingsFactory.fromBigtableOptions(options);
  }

  @Test
  public void testWithNullCredentials() throws IOException, GeneralSecurityException {
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID).setInstanceId(TEST_INSTANCE_ID)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setUserAgent(TEST_USER_AGENT).build();
    BigtableDataSettings settings = BigtableDataSettingsFactory.fromBigtableOptions(options);
    Assert.assertTrue(settings.getCredentialsProvider() instanceof NoCredentialsProvider);
  }

  @Test
  public void testRetrySettings() throws IOException, GeneralSecurityException {
    BigtableDataSettings settings = BigtableDataSettingsFactory.fromBigtableOptions(bigtableOptions);

    //Verifying RetrySettings for sampleRowKey, mutateRow & readRowSettings
    verifyRetry(settings.sampleRowKeysSettings().getRetrySettings());
    verifyRetry(settings.readRowsSettings().getRetrySettings());
    verifyRetry(settings.mutateRowSettings().getRetrySettings());

    //Verifying RetrySettings & RetryCodes of non-retryable methods.
    verifyDisabledRetry(settings.bulkMutationsSettings().getRetrySettings());
    verifyDisabledRetry(settings.readModifyWriteRowSettings().getRetrySettings());
    verifyDisabledRetry(settings.checkAndMutateRowSettings().getRetrySettings());
  }

  private void verifyRetry(RetrySettings retrySettings) {
    assertEquals(DEFAULT_INITIAL_BACKOFF_MILLIS, retrySettings.getInitialRetryDelay().toMillis());
    assertEquals(DEFAULT_BACKOFF_MULTIPLIER,retrySettings.getRetryDelayMultiplier(), 0);
    assertEquals(DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS, retrySettings.getMaxRetryDelay().toMillis());
    assertEquals(DEFAULT_MAX_SCAN_TIMEOUT_RETRIES, retrySettings.getMaxAttempts());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, retrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, retrySettings.getMaxRpcTimeout().toMillis());
    assertEquals(LONG_TIMEOUT_MS_DEFAULT, retrySettings.getTotalTimeout().toMillis());
  }

  private void verifyDisabledRetry(RetrySettings ret) {
    assertEquals(Duration.ZERO , ret.getInitialRetryDelay());
    assertEquals(1.0 , ret.getRetryDelayMultiplier(), 0);
    assertEquals(Duration.ZERO, ret.getMaxRetryDelay());
    assertEquals(1, ret.getMaxAttempts());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, ret.getInitialRpcTimeout().toMillis());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, ret.getMaxRpcTimeout().toMillis());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, ret.getTotalTimeout().toMillis());
  }

  @Test
  public void testRetryCodes() throws IOException, GeneralSecurityException {
    BigtableDataSettings settings = BigtableDataSettingsFactory.fromBigtableOptions(bigtableOptions);

    assertEquals(DEFAULT_RETRY_CODES, settings.sampleRowKeysSettings().getRetryableCodes());
    assertEquals(DEFAULT_RETRY_CODES, settings.readRowsSettings().getRetryableCodes());

    assertEquals(ImmutableSet.of(Code.DEADLINE_EXCEEDED, Code.UNAVAILABLE),
        settings.mutateRowSettings().getRetryableCodes());

    assertTrue(settings.bulkMutationsSettings().getRetryableCodes().isEmpty());
    assertTrue(settings.readModifyWriteRowSettings().getRetryableCodes().isEmpty());
    assertTrue(settings.checkAndMutateRowSettings().getRetryableCodes().isEmpty());
  }

  @Test
  public void testWhenBulkOptionIsDisabled() throws IOException, GeneralSecurityException {
    BulkOptions bulkOptions = BulkOptions.builder().setUseBulkApi(false).build();
    BigtableOptions options = BigtableOptions.builder().setProjectId(TEST_PROJECT_ID)
        .setInstanceId(TEST_INSTANCE_ID).setBulkOptions(bulkOptions)
        .build();
    BigtableDataSettings dataSettings = BigtableDataSettingsFactory.fromBigtableOptions(options);
    assertFalse(dataSettings.bulkMutationsSettings().getBatchingSettings().getIsEnabled());
  }

  @Test
  public void testBulkOption() throws IOException, GeneralSecurityException {
    BigtableOptions options = BigtableOptions.builder().setProjectId(TEST_PROJECT_ID)
        .setInstanceId(TEST_INSTANCE_ID).build();
    BigtableDataSettings dataSettings = BigtableDataSettingsFactory.fromBigtableOptions(options);

    BulkOptions bulkOptions = options.getBulkOptions();
    BatchingSettings batchingSettings = dataSettings.bulkMutationsSettings().getBatchingSettings();
    long outstandingElementCount = bulkOptions.getMaxInflightRpcs() * bulkOptions.getBulkMaxRowKeyCount();
    assertTrue(batchingSettings.getIsEnabled());
    assertEquals(bulkOptions.getBulkMaxRequestSize(),
      batchingSettings.getRequestByteThreshold().longValue());
    assertEquals(bulkOptions.getBulkMaxRowKeyCount(),
      batchingSettings.getElementCountThreshold().longValue());
    assertEquals(bulkOptions.getMaxMemory(),
      batchingSettings.getFlowControlSettings().getMaxOutstandingRequestBytes().longValue());
    assertEquals(outstandingElementCount,
      batchingSettings.getFlowControlSettings().getMaxOutstandingElementCount().longValue());
  }
}
