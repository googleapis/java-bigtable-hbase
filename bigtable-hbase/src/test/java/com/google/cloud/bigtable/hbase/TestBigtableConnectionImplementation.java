/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.security.User;

import com.google.cloud.bigtable.grpc.BigtableSession;

public class TestBigtableConnectionImplementation extends AbstractBigtableConnection {

  public TestBigtableConnectionImplementation(Configuration conf) throws IOException {
    super(conf);
  }
  
  protected TestBigtableConnectionImplementation(Configuration conf, boolean managed, ExecutorService pool,
      User user) throws IOException {
    super(conf, managed, pool, user);
  }
  
  public TestBigtableConnectionImplementation(Configuration conf, boolean managed, ExecutorService pool,
      User user, BigtableSession session) throws IOException {
    super(conf, managed, pool, user, session);
  }

  @Override
  public Admin getAdmin() throws IOException {
    return new AbstractBigtableAdmin(getOptions(), getConfiguration(), this,
        getBigtableTableAdminClient(), getDisabledTables()) {
    };
  }

}
