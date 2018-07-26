/*
 * Copyright 2018 Google LLC. All Rights Reserved.
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

import com.google.cloud.PlatformInformation;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

/**
 * Creates a {@link ThreadFactory} that's safe to use in AppEngine.
 * <p>
 * This class copies code that originates in {@link io.grpc.internal.GrpcUtil#getThreadFactory(String, boolean)}.
 */
public class ThreadUtil {

  /**
   * Get a {@link ThreadFactory} suitable for use in the current environment.
   * @param nameFormat to apply to threads created by the factory.
   * @param daemon {@code true} if the threads the factory creates are daemon threads, {@code false}
   *     otherwise.
   * @return a {@link ThreadFactory}.
   */
  public static ThreadFactory getThreadFactory(String nameFormat, boolean daemon) {
    if (PlatformInformation.isOnGAEStandard7()) {
      return MoreExecutors.platformThreadFactory();
    } else {
      return new ThreadFactoryBuilder()
          .setDaemon(daemon)
          .setNameFormat(nameFormat)
          .build();
    }
  }
}
