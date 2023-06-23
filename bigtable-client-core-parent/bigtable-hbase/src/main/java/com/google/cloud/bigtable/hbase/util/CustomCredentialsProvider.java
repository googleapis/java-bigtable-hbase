package com.google.cloud.bigtable.hbase.util;

import com.google.auth.Credentials;
import java.lang.reflect.Constructor;
import org.apache.hadoop.conf.Configuration;

public class CustomCredentialsProvider {

  // Static utility functions only.
  private CustomCredentialsProvider() {
  }


  public static Credentials getCustomCredentials(String customAuthClassName, Configuration conf) {
    Class<?> authClass = null;
    try {
      authClass = Class.forName(customAuthClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Can not find custom credentials class: " + customAuthClassName, e);
    }

    if (!isParent(authClass, Credentials.class.getName())) {
      throw new IllegalArgumentException(
          "Custom credentials class [" + customAuthClassName + "] must be a child of "
              + Credentials.class.getName() + ".");
    }

    Constructor<?> constructor = null;
    try {
      constructor = authClass.getConstructor(Configuration.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Custom credentials class [" + customAuthClassName
          + "] must implement a constructor with single argument of type "
          + Configuration.class.getName() + ".", e);
    }

    try {
      return (Credentials) constructor.newInstance(conf);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to create object of custom Credentials class [" + customAuthClassName + "].", e);
    }
  }


  public static boolean isParent(Class<?> classToBeChecked, String parentClassName) {
    Class<?> parent = classToBeChecked.getSuperclass();
    while (parent != null) {
      if (classToBeChecked.getSuperclass().getName().equals(parentClassName)) {
        return true;
      }
      parent = parent.getSuperclass();
    }

    return false;
  }
}
