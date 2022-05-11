/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog;

import org.apache.hadoop.hbase.client.Mutation;

/**
 * Objects of this class should transform a failed mutation into data to be passed to {@link
 * Appender}.
 */
public interface Serializer {
  /**
   * Create a failed mutation log entry for a given mutation and a failure cause.
   *
   * <p>This method is thread-safe.
   *
   * @param mutation the failed mutation
   * @param failureCause the cause of failure
   * @return data representing the relevant log entry
   */
  byte[] serialize(Mutation mutation, Throwable failureCause);
}
