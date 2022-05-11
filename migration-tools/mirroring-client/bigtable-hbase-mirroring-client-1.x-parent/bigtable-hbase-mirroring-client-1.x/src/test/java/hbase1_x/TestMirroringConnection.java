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
package hbase1_x;

import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.mirroring.core.TestConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMirroringConnection {
  private Configuration createConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("hbase.client.connection.impl", MirroringConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    configuration.set(MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "1");
    configuration.set(MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "2");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.prefix-path", "/tmp/test-");
    configuration.set("google.bigtable.mirroring.write-error-log.appender.max-buffer-size", "1024");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.drop-on-overflow", "false");
    return configuration;
  }

  @Test
  public void testConnectionFactoryCreatesMirroringConnection() throws IOException {
    Configuration configuration = createConfiguration();
    Connection connection = ConnectionFactory.createConnection(configuration);
    assertThat(connection).isInstanceOf(MirroringConnection.class);
    assertThat(((MirroringConnection) connection).getPrimaryConnection())
        .isInstanceOf(TestConnection.class);
    assertThat(((MirroringConnection) connection).getSecondaryConnection())
        .isInstanceOf(TestConnection.class);
  }
}
