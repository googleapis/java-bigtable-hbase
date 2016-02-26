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

import java.io.IOException;
import java.util.Collections;

import org.junit.Assert;

import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CloudBigtableTableConfiguration}.
 */
@RunWith(JUnit4.class)
public class CloudBigtableTableConfigurationTest {

  private static final String PROJECT = "my_project";
  private static final String CLUSTER = "cluster";
  private static final String ZONE = "some-zone-1a";
  private static final String TABLE = "some-zone-1a";

  @Test
  public void testHBaseConfig() throws IOException {
    CloudBigtableTableConfiguration underTest =
        new CloudBigtableTableConfiguration(PROJECT, ZONE, CLUSTER, TABLE,
            Collections.<String, String> emptyMap());

    Configuration config = underTest.toHBaseConfig();

    assertEquals(PROJECT, config.get(BigtableOptionsFactory.PROJECT_ID_KEY));
    assertEquals(ZONE, config.get(BigtableOptionsFactory.ZONE_KEY));
    assertEquals(CLUSTER, config.get(BigtableOptionsFactory.CLUSTER_KEY));
    assertEquals(TABLE, underTest.getTableId());
  }

  @Test
  public void testEquals() {
    CloudBigtableTableConfiguration underTest1 =
        new CloudBigtableTableConfiguration(PROJECT, ZONE, CLUSTER, TABLE,
            Collections.<String, String> emptyMap());
    CloudBigtableTableConfiguration underTest2 =
        new CloudBigtableTableConfiguration(PROJECT, ZONE, CLUSTER, TABLE,
            Collections.<String, String> emptyMap());
    CloudBigtableTableConfiguration underTest3 =
        new CloudBigtableTableConfiguration(PROJECT, ZONE, CLUSTER, TABLE,
            Collections.singletonMap("somekey", "somevalue"));
    CloudBigtableTableConfiguration underTest4 =
        new CloudBigtableTableConfiguration("other_project", ZONE, CLUSTER, TABLE,
            Collections.<String, String> emptyMap());
    CloudBigtableConfiguration underTest5 =
        new CloudBigtableConfiguration(PROJECT, ZONE, CLUSTER,
            Collections.<String, String> emptyMap());

    // Test CloudBigtableTableConfiguration that should be equal.
    Assert.assertEquals(underTest1, underTest2);

    // Test CloudBigtableTableConfiguration with different additionalConfigurations are not equal.
    Assert.assertNotEquals(underTest1, underTest3);

    // Test CloudBigtableTableConfiguration with different ProjectId are not equal.
    Assert.assertNotEquals(underTest1, underTest4);

    // Test CloudBigtableTableConfiguration is not equal to a similar CloudBigtableConfiguration.
    Assert.assertNotEquals(underTest1, underTest5);

    // Test CloudBigtableConfiguration is not equal to a similar CloudBigtableTableConfiguration.
    // (reversed order from the previous test)
    Assert.assertNotEquals(underTest5, underTest1);
  }

  @Test
  public void testToBuilder(){
    CloudBigtableTableConfiguration underTest =
        new CloudBigtableTableConfiguration(PROJECT, ZONE, CLUSTER, TABLE,
            Collections.singletonMap("somekey", "somevalue"));
    CloudBigtableTableConfiguration copy = underTest.toBuilder().build();
    Assert.assertNotSame(underTest, copy);
    Assert.assertEquals(underTest, copy);
  }
}

