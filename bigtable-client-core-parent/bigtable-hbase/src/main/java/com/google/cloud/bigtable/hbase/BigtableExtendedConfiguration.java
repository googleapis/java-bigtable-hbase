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
 * @deprecated bigtable-hbase is primarily a compatibility layer for HBase, adding Bigtable
 * extensions is out of scope
 */
@Deprecated
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
