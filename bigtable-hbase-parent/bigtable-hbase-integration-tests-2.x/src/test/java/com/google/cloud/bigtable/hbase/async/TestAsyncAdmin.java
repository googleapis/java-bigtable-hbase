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
package com.google.cloud.bigtable.hbase.async;

import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.junit.Assert;
import org.junit.Test;
import com.google.cloud.bigtable.hbase.AbstractTest;
import com.google.cloud.bigtable.hbase.Logger;

/**
 * Integration tests for BigtableAsyncConnection 
 * 
 * @author spollapally
 */
public class TestAsyncAdmin extends AbstractTest {
  private final Logger LOG = new Logger(getClass());

  @Test
  public void testAsyncConnection() throws Exception {
    AsyncConnection asyncCon = getAsyncConnection();
    Assert.assertNotNull("async connection should not be null", asyncCon);

    AsyncAdmin asycAdmin = asyncCon.getAdmin();
    Assert.assertNotNull("asycAdmin should not be null", asycAdmin);
  }
}
