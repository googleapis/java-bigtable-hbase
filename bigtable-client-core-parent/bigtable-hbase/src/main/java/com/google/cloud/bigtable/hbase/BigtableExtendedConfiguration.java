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
package com.google.cloud.bigtable.hbase;

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import org.apache.hadoop.conf.Configuration;

/**
 * Allows users to set an explicit {@link Credentials} object.
 *
 * <p>For internal use only - public for technical reasons.
 *
 * @see BigtableConfiguration#withCredentials(Configuration, Credentials).
 */
@InternalApi("For internal usage only")
public class BigtableExtendedConfiguration extends Configuration {
  private Credentials credentials;

  BigtableExtendedConfiguration(Configuration conf, Credentials credentials) {
    super(conf);
    this.credentials = credentials;
  }

  public Credentials getCredentials() {
    return credentials;
  }
}
