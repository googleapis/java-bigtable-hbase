package com.google.cloud.bigtable.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

/**
 * Utility for creating bigtable thread pools
 */
public class ThreadPoolUtil {

  public  static ThreadFactory createThreadFactory(String namePrefix) {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(namePrefix + "-%d")
        .build();
  }

}

