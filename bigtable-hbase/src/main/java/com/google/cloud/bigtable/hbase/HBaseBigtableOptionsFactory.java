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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BigtableOptions.Builder;
import com.google.cloud.bigtable.config.BigtableOptionsFactory;
import com.google.cloud.bigtable.config.BigtableOptionsFactory.PropertyRetriever;

/**
 * Static methods to convert an instance of {@link Configuration}
 * to a {@link BigtableOptions} instance.
 */
public class HBaseBigtableOptionsFactory {

  private static class ConfigurationPropertiesAdapter implements BigtableOptionsFactory.PropertyRetriever {

    private final Configuration configuration;

    public ConfigurationPropertiesAdapter(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public String get(String key) {
      return configuration.get(key);
    }

    @Override
    public String get(String key, String defaultValue) {
      return configuration.get(key, defaultValue);
    }

    @Override
    public int getInt(String key, int defaultValue) {
      return configuration.getInt(key, defaultValue);
    }

    @Override
    public long getLong(String key, long defaultValue) {
      return configuration.getLong(key, defaultValue);
    }

    @Override
    public boolean getBoolean(String key, boolean defaultValue) {
      return configuration.getBoolean(key, defaultValue);
    }
  }

  public static BigtableOptions fromConfiguration(final Configuration configuration) throws IOException {
    PropertyRetriever properties = new ConfigurationPropertiesAdapter(configuration);
    Builder optionsBuilder = BigtableOptionsFactory.fromProperties(properties);
    optionsBuilder.setUserAgent(BigtableConstants.USER_AGENT);
    return optionsBuilder.build();
  }
}
