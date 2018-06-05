/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.hbase1_x.BigtableConnection;

import java.io.IOException;

/**
 * This is a test to ensure that BigtableConnection can find {@link BigtableConnection}
 *
 */
@RunWith(JUnit4.class)
public class TestBigtableConnection {

  @Test
  public void testBigtableConnectionExists() {
    Assert.assertEquals(BigtableConnection.class, BigtableConfiguration.getConnectionClass());
  }

  @Test
  public void testTable() throws IOException {
    Configuration conf = BigtableConfiguration.configure("projectId", "instanceId");
    conf.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    conf.set(BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "false");
    BigtableConnection connection = new BigtableConnection(conf);
    Admin admin = connection.getAdmin() ;
    Table table = connection.getTable(TableName.valueOf("someTable"));
    BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("someTable"));
  }

  @Test
  public void testOpenSSL() throws Throwable{
    if(!OpenSsl.isAvailable()){
      throw OpenSsl.unavailabilityCause();
    }
  }
}
