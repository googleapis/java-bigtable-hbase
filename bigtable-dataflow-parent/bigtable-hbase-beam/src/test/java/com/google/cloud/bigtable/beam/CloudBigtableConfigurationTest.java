/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.bigtable.beam;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;

import com.google.cloud.bigtable.beam.CloudBigtableConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableConfiguration.Builder;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CloudBigtableConfiguration}.
 */
@RunWith(JUnit4.class)
public class CloudBigtableConfigurationTest {

  private static final String PROJECT = "my_project";

  private static final String INSTANCE = "instance_id";

  private Builder createBaseBuilder() {
    return createBaseBuilder(PROJECT, INSTANCE);
  }

  private Builder createBaseBuilder(String project, String instance) {
    return new CloudBigtableConfiguration.Builder().withProjectId(project).withInstanceId(instance);
  }

  @Test
  public void testToBuilder() {
    CloudBigtableConfiguration underTest =
        createBaseBuilder().withConfiguration("somekey", "somevalue").build();
    CloudBigtableConfiguration copy = underTest.toBuilder().build();
    Assert.assertNotSame(underTest, copy);
    Assert.assertEquals(underTest, copy);
  }

  @Test
  public void testHBaseConfig() {
    CloudBigtableConfiguration underTest = createBaseBuilder().build();

    Configuration config = underTest.toHBaseConfig();

    assertEquals(PROJECT, config.get(BigtableOptionsFactory.PROJECT_ID_KEY));
    assertEquals(INSTANCE, config.get(BigtableOptionsFactory.INSTANCE_ID_KEY));
  }

  @Test
  public void testEquals() {
    CloudBigtableConfiguration underTest1 = createBaseBuilder().build();
    CloudBigtableConfiguration underTest2 = createBaseBuilder().build();
    CloudBigtableConfiguration underTest3 =
        createBaseBuilder().withConfiguration("somekey", "somevalue").build();
    CloudBigtableConfiguration underTest4 = createBaseBuilder("other_project", INSTANCE).build();
    CloudBigtableConfiguration underTest5 =
        createBaseBuilder().withConfiguration("somekey", "somevalue").build();

    // Test CloudBigtableConfiguration that should be equal.
    Assert.assertEquals(underTest1, underTest2);

    // Test CloudBigtableConfiguration with different additionalConfigurations are not equal.
    Assert.assertNotEquals(underTest1, underTest3);

    // Test CloudBigtableConfiguration with different ProjectId are not equal.
    Assert.assertNotEquals(underTest1, underTest4);

    // Test CloudBigtableConfiguration with the same extended parameters are equal.
    Assert.assertEquals(underTest3, underTest5);
  }

  /**
   * This ensures that the config built from regular parameters are the same as the config built
   * from runtime parameters, so that we don't have to use runtime parameters to repeat the same
   * tests.
   */
  @Test
  public void testRegularAndRuntimeParametersAreEqual() {
    CloudBigtableConfiguration withRegularParameters =
        createBaseBuilder().withConfiguration("somekey", "somevalue").build();
    CloudBigtableConfiguration withRuntimeParameters =
        new CloudBigtableConfiguration.Builder()
            .withProjectId(StaticValueProvider.of(PROJECT))
            .withInstanceId(StaticValueProvider.of(INSTANCE))
            .withConfiguration("somekey", StaticValueProvider.of("somevalue"))
            .build();
    Assert.assertNotSame(withRegularParameters, withRuntimeParameters);
    Assert.assertEquals(withRegularParameters, withRuntimeParameters);
  }
}
