/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.bigtable.dataflow;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;

import com.google.bigtable.repackaged.com.google.cloud.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.dataflow.CloudBigtableConfiguration;
import com.google.cloud.bigtable.dataflow.CloudBigtableConfiguration.Builder;
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
  private static final String INSTANCE = "instance";

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
}

