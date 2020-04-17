/*
 * Copyright 2020 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.PlatformInformation;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.internal.GrpcUtil;
import java.util.concurrent.ThreadFactory;

/**
 * Utility class to create a {@link ThreadFactory} that's safe to use in App Engine.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ThreadUtil {

  /**
   * Creates a {@link ThreadFactory} suitable for use in the current environment.
   *
   * @param nameFormat to apply to threads created by the factory.
   * @param daemon if {@code true} then this factory creates daemon threads.
   * @return a {@link ThreadFactory}.
   */
  public static ThreadFactory getThreadFactory(String nameFormat, boolean daemon) {
    if (PlatformInformation.isOnGAEStandard7() || PlatformInformation.isOnGAEStandard8()) {
      return MoreExecutors.platformThreadFactory();
    } else {
      return GrpcUtil.getThreadFactory(nameFormat, daemon);
    }
  }
}
