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
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TestBigtableOptions {

  @Test
  public void testEquals() {
    BigtableOptions options1 = new BigtableOptions.Builder()
        .setProjectId("project")
        .setInstanceId("instance")
        .setUserAgent("foo")
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();
    BigtableOptions options2 = new BigtableOptions.Builder()
        .setProjectId("project")
        .setInstanceId("instance")
        .setUserAgent("foo")
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();
    BigtableOptions options3 = new BigtableOptions.Builder()
        .setProjectId("project")
        .setInstanceId("instance")
        .setUserAgent("foo1")
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();
    BigtableOptions options4 = new BigtableOptions.Builder()
        .setProjectId("project")
        .setInstanceId("instance")
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
        .setInstanceId("instance")
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
    BigtableOptions.getDefaultOptions();
  }

  @Test
  public void testEmulator() {
    Map<String, String> oldEnv = System.getenv();`
    Map<String, String> testEnv = new HashMap<>();
    testEnv.put(BigtableOptions.BIGTABLE_EMULATOR_HOST_ENV_VAR, "localhost:1234");
    setTestEnv(testEnv);
    BigtableOptions options = new BigtableOptions.Builder()
        .setPort(443)
        .setDataHost("xxx")
        .build();
    Assert.assertEquals(1234, options.getPort());
    Assert.assertEquals("localhost", options.getDataHost());
    Assert.assertEquals("localhost", options.getAdminHost());
    Assert.assertTrue(options.usePlaintextNegotiation());
    Assert.assertEquals(CredentialOptions.nullCredential(), options.getCredentialOptions());

    setTestEnv(oldEnv);
    options = new BigtableOptions.Builder()
        .setDataHost("override")
        .build();
    Assert.assertEquals(BigtableOptions.BIGTABLE_PORT_DEFAULT, options.getPort());
    Assert.assertEquals("override", options.getDataHost());
    Assert.assertEquals(BigtableOptions.BIGTABLE_ADMIN_HOST_DEFAULT, options.getAdminHost());
    Assert.assertFalse(options.usePlaintextNegotiation());
    Assert.assertEquals(CredentialOptions.defaultCredentials(), options.getCredentialOptions());
  }

  /**
   * This is a dirty way to override the environment that is accessible to a test.
   * It only modifies the JVM's view of the environment, not the environment itself.
   * From: http://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java/496849
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setTestEnv(Map<String, String> newEnv)
  {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newEnv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>)     theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newEnv);
    } catch (NoSuchFieldException e) {
      try {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newEnv);
          }
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }
}
