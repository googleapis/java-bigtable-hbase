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

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper;
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

  @Test
  public void testRequiresConfiguringImplClasses() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");

    Exception exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.primary-client.async.connection.impl");

    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "3");
    exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.secondary-client.async.connection.impl");

    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "4");
    MirroringAsyncConfiguration configuration = new MirroringAsyncConfiguration(testConfiguration);
  }

  @Test
  public void testFillsAllClassNames() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "3");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "4");

    MirroringAsyncConfiguration configuration = new MirroringAsyncConfiguration(testConfiguration);
    assertThat(configuration.primaryConfiguration.get("hbase.client.connection.impl"))
        .isEqualTo("1");
    assertThat(configuration.secondaryConfiguration.get("hbase.client.connection.impl"))
        .isEqualTo("2");
    assertThat(configuration.primaryConfiguration.get("hbase.client.async.connection.impl"))
        .isEqualTo("3");
    assertThat(configuration.secondaryConfiguration.get("hbase.client.async.connection.impl"))
        .isEqualTo("4");
  }

  @Test
  public void testSameConnectionClassesRequireOneOfPrefixes() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "3");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "3");

    Exception exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains(
            "Specify either google.bigtable.mirroring.primary-client.prefix or google.bigtable.mirroring.secondary-client.prefix");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "prefix");

    MirroringAsyncConfiguration config = new MirroringAsyncConfiguration(testConfiguration);
  }

  @Test
  public void testCopyConstructorSetsImplClasses() {
    Configuration empty = new Configuration(false);
    MirroringAsyncConfiguration emptyMirroringConfiguration =
        new MirroringAsyncConfiguration(empty, empty, empty);
    MirroringAsyncConfiguration configuration =
        new MirroringAsyncConfiguration(emptyMirroringConfiguration);
    assertThat(configuration.get("hbase.client.connection.impl"))
        .isEqualTo(MirroringConnection.class.getCanonicalName());
    assertThat(configuration.get("hbase.client.async.connection.impl"))
        .isEqualTo(MirroringAsyncConnection.class.getCanonicalName());
  }

  @Test
  public void testManualConstructionIsntBackwardsCompatible() {
    Configuration empty = new Configuration(false);
    MirroringAsyncConfiguration emptyMirroringConfiguration =
        new MirroringAsyncConfiguration(empty, empty, empty);
    MirroringAsyncConfiguration configuration =
        new MirroringAsyncConfiguration(emptyMirroringConfiguration);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new MirroringConfiguration(configuration);
        });
  }

  @Test
  public void testConfigurationConstructorIsBackwardsCompatible() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "1");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "2");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY, "3");
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY, "4");
    MirroringAsyncConfiguration mirroringAsyncConfiguration =
        new MirroringAsyncConfiguration(testConfiguration);

    new MirroringConfiguration(mirroringAsyncConfiguration);
  }
}
