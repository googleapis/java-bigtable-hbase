/*
 * Copyright 2018 Google LLC
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
package com.google.cloud.bigtable.hbase.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests to make sure that AsyncConnection methods do not throw exceptions.
 *
 * @author sduskis
 */
@RunWith(JUnit4.class)
public class TestAsyncConnection extends AbstractAsyncTest {

  private ExecutorService executorService;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    executorService = Executors.newCachedThreadPool();
  }

  @After
  public void tearDown() throws Exception {
    executorService.shutdown();
  }

  @Test
  public void testAdmin() throws InterruptedException, ExecutionException {
    Assert.assertNotNull(getAsyncConnection().getAdmin());
    Assert.assertNotNull(getAsyncConnection().getAdmin(executorService));
  }

  @Test
  public void testRegionLocator() throws InterruptedException, ExecutionException {
    Assert.assertNotNull(
        getAsyncConnection().getRegionLocator(sharedTestEnv.getDefaultTableName()));
  }

  @Test
  public void testTable() throws InterruptedException, ExecutionException {
    Assert.assertNotNull(
        getAsyncConnection().getTable(sharedTestEnv.getDefaultTableName(), executorService));
    Assert.assertNotNull(
        getAsyncConnection()
            .getTableBuilder(sharedTestEnv.getDefaultTableName(), executorService)
            .build());
  }

  @Test
  public void testBufferedMutator() throws InterruptedException, ExecutionException {
    Assert.assertNotNull(
        getAsyncConnection().getBufferedMutator(sharedTestEnv.getDefaultTableName()));
    Assert.assertNotNull(
        getAsyncConnection()
            .getBufferedMutatorBuilder(sharedTestEnv.getDefaultTableName())
            .build());
    Assert.assertNotNull(
        getAsyncConnection()
            .getBufferedMutatorBuilder(sharedTestEnv.getDefaultTableName(), executorService)
            .build());
  }

  @Test
  public void testConnectionIsClosed() throws Exception {
    Assert.assertFalse("Connection should open at this point", getAsyncConnection().isClosed());
  }
}
