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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMirroringConfiguration {

  private Exception assertInvalidConfiguration(final Configuration test) {
    return assertThrows(
        IllegalArgumentException.class,
        new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            new MirroringConfiguration(test);
          }
        });
  }

  @Test
  public void testRequiresConfiguringImplClasses() {
    Configuration testConfiguration = new Configuration(false);
    Exception exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.primary-client.connection.impl");

    testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.secondary-client.connection.impl");

    testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc)
        .hasMessageThat()
        .contains("Specify google.bigtable.mirroring.primary-client.connection.impl");
  }

  @Test
  public void testSameConnectionClassesRequireOneOfPrefixes() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());

    Exception exc = assertInvalidConfiguration(testConfiguration);
    assertThat(exc).hasMessageThat().contains("prefix");

    testConfiguration.set(MirroringConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "test");
    new MirroringConfiguration(testConfiguration);
  }

  @Test
  public void testDifferentConnectionClassesDoNotRequirePrefix() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(MirroringConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "test");

    new MirroringConfiguration(testConfiguration);
  }

  @Test
  public void testSamePrefixForPrimaryAndSecondaryIsNotAllowedIfConnectionClassesAreTheSame() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(MirroringConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "test");
    testConfiguration.set(MirroringConfiguration.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "test");

    assertInvalidConfiguration(testConfiguration);
  }

  @Test
  public void testConfigurationPrefixesAreStrippedAndPassedAsConfigurations() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "connection-1");
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "connection-2");
    testConfiguration.set("connection-2.test1", "21");
    testConfiguration.set("connection-2.test2", "22");

    testConfiguration.set("connection-1.test1", "11");
    testConfiguration.set("connection-1.test2", "12");

    MirroringConfiguration configuration = new MirroringConfiguration(testConfiguration);

    assertThat(configuration.primaryConfiguration.get("test1")).isEqualTo("11");
    assertThat(configuration.primaryConfiguration.get("test2")).isEqualTo("12");
    assertThat(configuration.secondaryConfiguration.get("test1")).isEqualTo("21");
    assertThat(configuration.secondaryConfiguration.get("test2")).isEqualTo("22");
  }

  @Test
  public void testConfigWithoutPrefixReceivesAllProperties() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "connection-1");
    testConfiguration.set("connection-1.test1", "11");
    testConfiguration.set("connection-1.test2", "12");
    testConfiguration.set("not-a-connection-1.test3", "13");

    testConfiguration.set("test1", "1");
    testConfiguration.set("test2", "2");

    MirroringConfiguration configuration = new MirroringConfiguration(testConfiguration);

    assertThat(configuration.primaryConfiguration.get("test1")).isEqualTo("11");
    assertThat(configuration.primaryConfiguration.get("test2")).isEqualTo("12");
    // expected test1, test2, hbase.client.connection.impl
    assertThat(configuration.primaryConfiguration.getValByRegex(".*")).hasSize(3);

    assertThat(configuration.secondaryConfiguration.get("test1")).isEqualTo("1");
    assertThat(configuration.secondaryConfiguration.get("test2")).isEqualTo("2");
    assertThat(configuration.secondaryConfiguration.get("connection-1.test1")).isEqualTo("11");
    assertThat(configuration.secondaryConfiguration.get("connection-1.test2")).isEqualTo("12");
  }

  @Test
  public void testMirroringOptionsAreRead() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(MirroringConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "test");
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_FLOW_CONTROLLER_STRATEGY_CLASS, "test-1");

    MirroringConfiguration configuration = new MirroringConfiguration(testConfiguration);

    assertThat(configuration.mirroringOptions.flowControllerStrategyClass).isEqualTo("test-1");
  }

  @Test
  public void testConnectionClassesArePassedAsHbaseConfig() {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(MirroringConfiguration.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, "test1");
    testConfiguration.set(MirroringConfiguration.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "test2");
    testConfiguration.set(
        MirroringConfiguration.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "connection-1");
    MirroringConfiguration configuration = new MirroringConfiguration(testConfiguration);

    assertThat(configuration.primaryConfiguration.get("hbase.client.connection.impl"))
        .isEqualTo("test1");
    assertThat(configuration.secondaryConfiguration.get("hbase.client.connection.impl"))
        .isEqualTo("test2");
  }
}
