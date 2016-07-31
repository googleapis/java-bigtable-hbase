package com.google.cloud.bigtable.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

/**
 * Utility for creating bigtable thread pools
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ThreadPoolUtil {

  /**
   * <p>createThreadFactory.</p>
   *
   * @param namePrefix a {@link java.lang.String} object.
   * @return a {@link java.util.concurrent.ThreadFactory} object.
   */
  public  static ThreadFactory createThreadFactory(String namePrefix) {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(namePrefix + "-%d")
        .build();
  }

}

