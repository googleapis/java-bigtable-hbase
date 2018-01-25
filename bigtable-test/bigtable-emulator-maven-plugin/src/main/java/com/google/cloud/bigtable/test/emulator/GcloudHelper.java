/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

package com.google.cloud.bigtable.test.emulator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

class GcloudHelper {

  private static final String BIGTABLE_EMULATOR_ID = "bigtable";

  private final ExecutorService executor;

  GcloudHelper(ExecutorService executor) {
    this.executor = executor;
  }

  File getEmulatorPath() {
    final String[] components;

    try {
      components = getComponents();
    } catch (Exception e) {
      throw new RuntimeException("Failed to query gcloud sdk components. Is gcloud sdk installed?", e);
    }

    if (!Arrays.asList(components).contains(BIGTABLE_EMULATOR_ID)) {
      throw new RuntimeException(
          String.format(
              "Bigtable emulator is not installed, please install via `gcloud components install %s`", BIGTABLE_EMULATOR_ID));
    }

    final File sdkRoot;
    try {
      sdkRoot = getSdkRoot();
    } catch (Exception e) {
      throw new RuntimeException("Failed to find gcloud sdk install directory.", e);
    }

    final File emulatorPath = Paths.get(sdkRoot.toString(), "platform", "bigtable-emulator", "cbtemulator").toFile();
    if (!emulatorPath.exists()) {
      throw new RuntimeException("Emulator executable doesn't exist, despite being installed");
    }

    return emulatorPath;
  }

  private File getSdkRoot() throws IOException, InterruptedException, ExecutionException {
    Process p = Runtime.getRuntime().exec("gcloud info --format value(installation.sdk_root)");
    Future<String> stdout = readFully(p.getInputStream());
    Future<String> stderr = readFully(p.getErrorStream());

    if (p.waitFor() != 0) {
      throw new RuntimeException("Failed to get sdk root: " + stderr.get());
    }

    return new File(stdout.get().trim());
  }

  private String[] getComponents() throws IOException, InterruptedException, ExecutionException {
    Process p = Runtime.getRuntime().exec("gcloud components list --format=value(id) --only-local-state");

    Future<String> stdout = readFully(p.getInputStream());
    Future<String> stderr = readFully(p.getErrorStream());

    if (p.waitFor() != 0) {
      throw new RuntimeException("Failed to get components list: " + stderr.get());
    }

    return stdout.get().split("\n");
  }

  private Future<String> readFully(final InputStream in) {
    FutureTask<String> task = new FutureTask<>(new Callable<String>() {
      @Override
      public String call() throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[1024];

        int nRead;
        while ((nRead = in.read(data)) > -1) {
          buffer.write(data, 0, nRead);
        }

        return buffer.toString();
      }
    });

    executor.submit(task);

    return task;
  }
}
