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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOptions;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper;
import org.apache.hadoop.conf.Configuration;

public class MirroringAsyncConfiguration extends Configuration {
  Configuration primaryConfiguration;
  Configuration secondaryConfiguration;
  MirroringOptions mirroringOptions;

  public MirroringAsyncConfiguration(
      Configuration primaryConfiguration,
      Configuration secondaryConfiguration,
      Configuration mirroringConfiguration) {
    super.set("hbase.client.connection.impl", MirroringConnection.class.getCanonicalName());
    super.set(
        "hbase.client.async.connection.impl", MirroringAsyncConnection.class.getCanonicalName());

    this.primaryConfiguration = primaryConfiguration;
    this.secondaryConfiguration = secondaryConfiguration;
    this.mirroringOptions = new MirroringOptions(mirroringConfiguration);
  }

  public MirroringAsyncConfiguration(Configuration conf) {
    super(conf); // Copy-constructor
    // In case the user constructed MirroringAsyncConfiguration by hand.
    if (conf instanceof MirroringAsyncConfiguration) {
      MirroringAsyncConfiguration mirroringConfiguration = (MirroringAsyncConfiguration) conf;
      this.primaryConfiguration = new Configuration(mirroringConfiguration.primaryConfiguration);
      this.secondaryConfiguration =
          new Configuration(mirroringConfiguration.secondaryConfiguration);
      this.mirroringOptions = mirroringConfiguration.mirroringOptions;
    } else {
      MirroringConfigurationHelper.checkParameters(
          conf,
          MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
          MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY);
      MirroringConfigurationHelper.checkParameters(
          conf,
          MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
          MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY);

      final Configuration primaryConfiguration =
          MirroringConfigurationHelper.extractPrefixedConfig(
              MirroringConfigurationHelper.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, conf);
      MirroringConfigurationHelper.fillConnectionConfigWithClassImplementation(
          primaryConfiguration,
          conf,
          MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
          "hbase.client.connection.impl");
      MirroringConfigurationHelper.fillConnectionConfigWithClassImplementation(
          primaryConfiguration,
          conf,
          MirroringConfigurationHelper.MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY,
          "hbase.client.async.connection.impl");
      this.primaryConfiguration = primaryConfiguration;

      final Configuration secondaryConfiguration =
          MirroringConfigurationHelper.extractPrefixedConfig(
              MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, conf);
      MirroringConfigurationHelper.fillConnectionConfigWithClassImplementation(
          secondaryConfiguration,
          conf,
          MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY,
          "hbase.client.connection.impl");
      MirroringConfigurationHelper.fillConnectionConfigWithClassImplementation(
          secondaryConfiguration,
          conf,
          MirroringConfigurationHelper.MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY,
          "hbase.client.async.connection.impl");
      this.secondaryConfiguration = secondaryConfiguration;

      this.mirroringOptions = new MirroringOptions(conf);
    }
  }
}
