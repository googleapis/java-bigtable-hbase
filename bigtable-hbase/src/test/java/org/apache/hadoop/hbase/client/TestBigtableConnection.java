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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutator;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.TestBigtableConnectionImplementation;

@RunWith(JUnit4.class)
public class TestBigtableConnection {
  private static final String PROJECT_ID = "unittestpid";
  private static final String ZONE_ID = "us-central1-c";
  private static final String CLUSTER_ID = "testcluster";
  private static final String TABLE = "testtable";
  
  private Configuration conf;
  private TableConfiguration tableConf;
  
  @Mock
  private ExecutorService pool;
  @Mock
  private BigtableSession session;
  @Mock
  private BigtableDataClient dataClient;
  
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    conf = new Configuration();
    conf.set(BigtableOptionsFactory.PROJECT_ID_KEY, PROJECT_ID);
    conf.set(BigtableOptionsFactory.ZONE_KEY, ZONE_ID);
    conf.set(BigtableOptionsFactory.CLUSTER_KEY, CLUSTER_ID);
    tableConf = new TableConfiguration(conf);
  }
  
  @Test
  public void getBufferedMutatorWithTable() throws Exception {
    final Connection connection = 
        new TestBigtableConnectionImplementation(conf, false, pool, null, session);
    assertEquals(0, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    
    final BigtableBufferedMutator mutator = (BigtableBufferedMutator)
        connection.getBufferedMutator(TableName.valueOf(TABLE));
    
    assertSame(conf, mutator.getConfiguration());
    assertEquals(TableName.valueOf(TABLE), mutator.getName());
    assertFalse(mutator.hasInflightRequests());
    assertEquals(AsyncExecutor.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT, 
        mutator.getWriteBufferSize());
    assertEquals(1, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    mutator.close();
    assertEquals(0, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getBufferedMutatorWithTableNull() throws Exception {
    final Connection connection = 
        new TestBigtableConnectionImplementation(conf, false, pool, null, session);
    connection.getBufferedMutator((TableName)null);
  }
  
  @Test
  public void getBufferedMutatorWithParamsDefault() throws Exception {
    final BufferedMutatorParams params = new BufferedMutatorParams(
        TableName.valueOf(TABLE));
    
    final Connection connection = 
        new TestBigtableConnectionImplementation(conf, false, pool, null, session);
    assertEquals(0, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    
    final BigtableBufferedMutator mutator = (BigtableBufferedMutator)
        connection.getBufferedMutator(params);
    
    assertSame(conf, mutator.getConfiguration());
    assertEquals(TableName.valueOf(TABLE), mutator.getName());
    assertFalse(mutator.hasInflightRequests());
    assertEquals(tableConf.getWriteBufferSize(), mutator.getWriteBufferSize());
    assertEquals(1, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    mutator.close();
    assertEquals(0, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getBufferedMutatorWithParamsNullTable() throws Exception {
    final BufferedMutatorParams params = new BufferedMutatorParams(null);
    final Connection connection = 
        new TestBigtableConnectionImplementation(conf, false, pool, null, session);
    connection.getBufferedMutator(params);
  }
  
  @Test
  public void getBufferedMutatorWithParamsWriteBufferOverride() throws Exception {
    final BufferedMutatorParams params = new BufferedMutatorParams(
        TableName.valueOf(TABLE)).writeBufferSize(42);
    
    final Connection connection = 
        new TestBigtableConnectionImplementation(conf, false, pool, null, session);
    assertEquals(0, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    
    final BigtableBufferedMutator mutator = (BigtableBufferedMutator)
        connection.getBufferedMutator(params);
    
    assertSame(conf, mutator.getConfiguration());
    assertEquals(TableName.valueOf(TABLE), mutator.getName());
    assertFalse(mutator.hasInflightRequests());
    assertEquals(42, mutator.getWriteBufferSize());
    assertEquals(1, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    mutator.close();
    assertEquals(0, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
  }

  @Test
  public void getBufferedMutatorWithParamsClientShutdown() throws Exception {
    final BufferedMutatorParams params = new BufferedMutatorParams(
        TableName.valueOf(TABLE));
    
    final Connection connection = 
        new TestBigtableConnectionImplementation(conf, false, pool, null, session);
    assertEquals(0, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    
    final BigtableBufferedMutator mutator = (BigtableBufferedMutator)
        connection.getBufferedMutator(params);
    
    assertSame(conf, mutator.getConfiguration());
    assertEquals(TableName.valueOf(TABLE), mutator.getName());
    assertFalse(mutator.hasInflightRequests());
    assertEquals(tableConf.getWriteBufferSize(), mutator.getWriteBufferSize());
    assertEquals(1, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    connection.close();
    // close is never called in this case. Shouldn't the mutator map be a member?
    assertEquals(1, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
    mutator.close(); // close to satisfy the other UTs
    assertEquals(0, AbstractBigtableConnection.ACTIVE_BUFFERED_MUTATORS.size());
  }
  
  // TODO - getBufferedMutator - max inflights, pool, listener
}
