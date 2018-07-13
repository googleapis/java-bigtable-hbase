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
package io.grpc;

import java.util.concurrent.TimeUnit;

/**
 * Hack to allow the creation of a {@link Deadline} with a constant time.
 */
public class DeadlineUtil {

  public static Deadline deadlineWithFixedTime(int duration, TimeUnit unit, final long timeNs) {
    return Deadline.after(duration, unit, new Deadline.Ticker() {
      @Override
      public long read() {
        return timeNs;
      }
    });
  }
}
