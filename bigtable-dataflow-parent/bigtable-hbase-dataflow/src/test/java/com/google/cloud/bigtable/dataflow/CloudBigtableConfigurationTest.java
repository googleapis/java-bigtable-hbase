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

import java.util.Collections;

import org.junit.Assert;

import com.google.bigtable.repackaged.com.google.cloud.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.dataflow.CloudBigtableConfiguration;

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
    CloudBigtableConfiguration underTest =
        new CloudBigtableConfiguration(PROJECT, INSTANCE,
            Collections.<String, String> emptyMap());

    Configuration config = underTest.toHBaseConfig();

    assertEquals(PROJECT, config.get(BigtableOptionsFactory.PROJECT_ID_KEY));
    assertEquals(INSTANCE, config.get(BigtableOptionsFactory.INSTANCE_ID_KEY));
  }

  @Test
  public void testEquals() {
    CloudBigtableConfiguration underTest1 =
        new CloudBigtableConfiguration(PROJECT, INSTANCE, Collections.<String, String> emptyMap());
    CloudBigtableConfiguration underTest2 =
        new CloudBigtableConfiguration(PROJECT, INSTANCE, Collections.<String, String> emptyMap());
    CloudBigtableConfiguration underTest3 = new CloudBigtableConfiguration(PROJECT, INSTANCE,
        Collections.singletonMap("somekey", "somevalue"));
    CloudBigtableConfiguration underTest4 = new CloudBigtableConfiguration("other_project",
        INSTANCE, Collections.<String, String> emptyMap());
    CloudBigtableConfiguration underTest5 = new CloudBigtableConfiguration(PROJECT, INSTANCE,
        Collections.singletonMap("somekey", "somevalue"));

    // Test CloudBigtableConfiguration that should be equal.
    Assert.assertEquals(underTest1, underTest2);

    // Test CloudBigtableConfiguration with different additionalConfigurations are not equal.
    Assert.assertNotEquals(underTest1, underTest3);

    // Test CloudBigtableConfiguration with different ProjectId are not equal.
    Assert.assertNotEquals(underTest1, underTest4);

    // Test CloudBigtableConfiguration with the same extended parameters are equal.
    Assert.assertEquals(underTest3, underTest5);
}

  @Test
  public void testToBuilder() {
    CloudBigtableConfiguration underTest =
        new CloudBigtableConfiguration(PROJECT, INSTANCE, Collections.singletonMap("somekey",
          "somevalue"));
    CloudBigtableConfiguration copy = underTest.toBuilder().build();
    Assert.assertNotSame(underTest, copy);
    Assert.assertEquals(underTest, copy);
  }
}

