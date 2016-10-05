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

