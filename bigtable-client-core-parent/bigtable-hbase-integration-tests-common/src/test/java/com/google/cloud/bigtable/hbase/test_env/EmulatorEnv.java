/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.hbase.test_env;

import com.google.cloud.bigtable.emulator.core.EmulatorController;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;

class EmulatorEnv extends SharedTestEnv {
  private EmulatorController emulator;

  @Override
  protected void setup() throws Exception {
    emulator = EmulatorController.createBundled();
    emulator.start();

    configuration = HBaseConfiguration.create();
    configuration = BigtableConfiguration.configure(configuration, "fake-project", "fake-instance");
    configuration.set("google.bigtable.emulator.endpoint.host", "localhost:" + emulator.getPort());
  }

  @Override
  protected void teardown() {
    emulator.stop();
  }
}
