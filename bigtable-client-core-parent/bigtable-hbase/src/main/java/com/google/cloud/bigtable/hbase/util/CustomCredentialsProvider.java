/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.bigtable.hbase.util;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.hbase.BigtableOAuthCredentials;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableCredentialsWrapper;
import java.lang.reflect.Constructor;
import org.apache.hadoop.conf.Configuration;

public class CustomCredentialsProvider {

  // Static utility functions only.
  private CustomCredentialsProvider() {}

  public static Credentials getCustomCredentials(String customAuthClassName, Configuration conf) {
    Class<?> authClass = null;
    try {
      authClass = Class.forName(customAuthClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Can not find custom credentials class: " + customAuthClassName, e);
    }

    if (!BigtableOAuthCredentials.class.isAssignableFrom(authClass)) {
      throw new IllegalArgumentException(
          "Custom credentials class ["
              + customAuthClassName
              + "] must be a child of "
              + BigtableOAuthCredentials.class.getName()
              + ".");
    }

    Constructor<?> constructor = null;
    try {
      constructor = authClass.getConstructor(Configuration.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Custom credentials class ["
              + customAuthClassName
              + "] must implement a constructor with single argument of type "
              + Configuration.class.getName()
              + ".",
          e);
    }

    try {
      return new BigtableCredentialsWrapper(
          (BigtableOAuthCredentials) constructor.newInstance(conf));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to create object of custom Credentials class [" + customAuthClassName + "].", e);
    }
  }
}
