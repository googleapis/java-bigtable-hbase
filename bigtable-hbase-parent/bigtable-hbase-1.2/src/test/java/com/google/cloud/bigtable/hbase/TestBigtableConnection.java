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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.repackaged.io.netty.handler.ssl.OpenSsl;
import com.google.cloud.bigtable.hbase1_2.BigtableConnection;


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
  public void testOpelSSL() throws Throwable{
    if(!OpenSsl.isAvailable()){
      throw OpenSsl.unavailabilityCause();
    }
  }

}
