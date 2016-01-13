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
package com.google.cloud.bigtable.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Assert;
import org.junit.Test;

public class TestBigtableOptions {

  @Test
  public void testEquals() {
    BigtableOptions options1 = new BigtableOptions.Builder()
        .setProjectId("project")
        .setZoneId("zone")
        .setClusterId("cluster")
        .setUserAgent("foo")
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();
    BigtableOptions options2 = new BigtableOptions.Builder()
        .setProjectId("project")
        .setZoneId("zone")
        .setClusterId("cluster")
        .setUserAgent("foo")
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();
    BigtableOptions options3 = new BigtableOptions.Builder()
        .setProjectId("project")
        .setZoneId("zone")
        .setClusterId("cluster")
        .setUserAgent("foo1")
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();
    BigtableOptions options4 = new BigtableOptions.Builder()
        .setProjectId("project")
        .setZoneId("zone")
        .setClusterId("cluster")
        .setUserAgent("foo1")
        .setCredentialOptions(CredentialOptions.defaultCredentials())
        .build();

    Assert.assertEquals(options1, options2);
    Assert.assertNotEquals(options1, options3);
    Assert.assertNotEquals(options1, options4);
  }

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    BigtableOptions options = new BigtableOptions.Builder()
        .setProjectId("project")
        .setZoneId("zone")
        .setClusterId("cluster")
        .setUserAgent("foo")
        .build();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(options);
    oos.close();
    byte[] byteArray = bos.toByteArray();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
    ObjectInputStream iis = new ObjectInputStream(bais);
    
    BigtableOptions deserialized = (BigtableOptions) iis.readObject();
    Assert.assertEquals(options, deserialized);
  }

  @Test
  public void testNullStringsDontThrowExceptions() {
     new BigtableOptions.Builder().build();
  }
}
