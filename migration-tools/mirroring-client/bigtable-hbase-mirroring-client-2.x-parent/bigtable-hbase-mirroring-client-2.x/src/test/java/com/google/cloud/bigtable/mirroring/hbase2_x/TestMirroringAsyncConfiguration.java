/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestMirroringAsyncConfiguration {
  private Exception assertInvalidConfiguration(final Configuration test) {
    return assertThrows(
        IllegalArgumentException.class,
        () -> {
          new MirroringAsyncConfiguration(test);
        });
  }

  private void assertValidConfiguration(final Configuration test) {
    try {
      new MirroringAsyncConfiguration(test);
    } catch (Exception e) {
      fail("Shouldn't have thrown");
    }
  }

  @Test
  public void testRequiresConfiguringImplClasses() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "ConnectionClass1");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "ConnectionClass2");

    // MirroringAsyncConfiguration requires that keys for both synchronous and asynchronous classes
    // of both primary and secondary connections are filled.

    // None of asynchronous connection class keys is set.
    Exception exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.primary-client.async.connection.impl");

    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
        "AsyncConnectionClass1");
    // Secondary asynchronous connection class key is not set.
    exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.secondary-client.async.connection.impl");

    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY,
        "AsyncConnectionClass2");
    // All required keys are set.
    assertValidConfiguration(testConfiguration);
  }

  @Test
  public void testFillsAllClassNames() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "ConnectionClass1");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "ConnectionClass2");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
        "AsyncConnectionClass1");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY,
        "AsyncConnectionClass2");

    MirroringAsyncConfiguration asyncConfiguration =
        new MirroringAsyncConfiguration(testConfiguration);
    assertThat(asyncConfiguration.primaryConfiguration.get("hbase.client.connection.impl"))
        .isEqualTo("ConnectionClass1");
    assertThat(asyncConfiguration.secondaryConfiguration.get("hbase.client.connection.impl"))
        .isEqualTo("ConnectionClass2");
    assertThat(asyncConfiguration.primaryConfiguration.get("hbase.client.async.connection.impl"))
        .isEqualTo("AsyncConnectionClass1");
    assertThat(asyncConfiguration.secondaryConfiguration.get("hbase.client.async.connection.impl"))
        .isEqualTo("AsyncConnectionClass2");
  }

  @Test
  public void testSameConnectionClassesRequireOneOfPrefixes() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "ConnectionClass1");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "ConnectionClass2");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
        "SameAsyncConnectionClass");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY,
        "SameAsyncConnectionClass");

    Exception exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains(
            "Specify either google.bigtable.mirroring.primary-client.prefix or google.bigtable.mirroring.secondary-client.prefix");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "prefix");

    assertValidConfiguration(testConfiguration);
  }
}
