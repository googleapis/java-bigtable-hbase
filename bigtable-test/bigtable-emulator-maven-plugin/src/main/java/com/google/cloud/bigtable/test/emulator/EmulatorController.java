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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Properties;


class EmulatorController {

  private final File emulatorPath;
  private final File logFile;
  private Process process;
  private int port;
  private boolean isStarted;

  EmulatorController(File emulatorPath, File logFile) {
    this.emulatorPath = emulatorPath;
    this.logFile = logFile;
  }

  void start() throws IOException {
    port = getFreePort();

    // `gcloud beta emulators bigtable start` creates subprocesses and java doesn't kill process trees
    // so we have to invoke the emulator directly

    try {
      process = new ProcessBuilder(emulatorPath.toString(), "--host=localhost", "--port=" + port)
          .redirectErrorStream(true)
          .redirectOutput(logFile)
          .start();
      //process = Runtime.getRuntime().exec(emulatorPath.toString() + " --host=localhost --port=" + port);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("cbtemulator could not be found");
    }

    isStarted = true;
  }

  boolean isStarted() {
    return isStarted;
  }

  int getPort() {
    if (port == 0) {
      throw new IllegalStateException("Emulator hasn't started yet");
    }
    return port;
  }

  void stop() {
    process.destroy();
    isStarted = false;
  }

  private int getFreePort() throws IOException {
    try(ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    } catch (IOException e) {
      throw new IOException("Failed to find a free port", e);
    }
  }

  static class Builder {

    private File emulatorPath = null;
    private File logFile = null;

    Builder setEmulatorPath(File path) {
      this.emulatorPath = path;
      return this;
    }

    Builder setLogFile(File path) {
      this.logFile = path;
      return this;
    }

    EmulatorController build() {
      Preconditions.checkNotNull(emulatorPath, "emulatorPath can't be null");
      Preconditions.checkNotNull(logFile, "logFile can't be null");

      return new EmulatorController(emulatorPath, logFile);
    }

  }
}
