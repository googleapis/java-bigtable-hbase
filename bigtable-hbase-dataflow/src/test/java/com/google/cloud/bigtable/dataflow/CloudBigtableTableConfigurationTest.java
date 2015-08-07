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

import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Tests for {@link CloudBigtableTableConfiguration}.
 */
public class CloudBigtableTableConfigurationTest {

  private static final String PROJECT = "my_project";
  private static final String CLUSTER = "cluster";
  private static final String ZONE = "some-zone-1a";
  private static final String TABLE = "some-zone-1a";

  @Test
  public void testHBaseConfig(){
    CloudBigtableTableConfiguration underTest =
        new CloudBigtableTableConfiguration(PROJECT, ZONE, CLUSTER, TABLE);

    Configuration config = underTest.toHBaseConfig();

    assertEquals(PROJECT, config.get(BigtableOptionsFactory.PROJECT_ID_KEY));
    assertEquals(ZONE, config.get(BigtableOptionsFactory.ZONE_KEY));
    assertEquals(CLUSTER, config.get(BigtableOptionsFactory.CLUSTER_KEY));
    assertEquals(TABLE, underTest.getTableId());
  }
}

