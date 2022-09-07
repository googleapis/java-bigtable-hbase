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
package com.google.cloud.bigtable.test.helper;

import com.google.cloud.bigtable.emulator.core.EmulatorController;
import org.junit.rules.ExternalResource;

public class BigtableEmulatorRule extends ExternalResource {
  private EmulatorController emulatorController;

  @Override
  protected void before() throws Throwable {
    emulatorController = EmulatorController.createBundled();
    emulatorController.start();
  }

  @Override
  protected void after() {
    emulatorController.stop();
  }

  public int getPort() {
    return emulatorController.getPort();
  }
}
