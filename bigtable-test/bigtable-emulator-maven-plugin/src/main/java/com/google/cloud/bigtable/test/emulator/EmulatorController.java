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
  private final File portFile;
  private final File logFile;
  private Process process;
  private int port;
  private boolean isStarted;

  EmulatorController(File emulatorPath, File portFile, File logFile) {
    this.emulatorPath = emulatorPath;
    this.portFile = portFile;
    this.logFile = logFile;
  }

  void start() throws IOException {
    if (portFile.exists()) {
      throw new FileAlreadyExistsException(portFile.toString(), null,
          "Port file already exists. If the emulator crashed, please delete it");
    }

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

    writeProps();

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
    //noinspection ResultOfMethodCallIgnored
    portFile.delete();
    isStarted = false;
  }

  void waitForTermination() throws InterruptedException {
    process.waitFor();
  }

  private void writeProps() throws IOException {
    Properties props = new Properties();
    props.setProperty("google.bigtable.endpoint.port", Integer.toString(port));
    props.setProperty("google.bigtable.instance.admin.endpoint.host", "localhost");
    props.setProperty("google.bigtable.admin.endpoint.host", "localhost");
    props.setProperty("google.bigtable.endpoint.host", "localhost");

    // emulator doesn't care about projects & instances
    props.setProperty("google.bigtable.project.id", "fakeproject");
    props.setProperty("google.bigtable.instance.id", "fakeinstance");
    // disable authentication
    props.setProperty("google.bigtable.auth.service.account.enable", "false");
    props.setProperty("google.bigtable.auth.null.credential.enable", "true");

    Files.createDirectories(portFile.toPath().getParent());

    try (FileOutputStream fout = new FileOutputStream(portFile, false)) {
      props.store(fout, "Bigtable emulator parameters");
    }
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
    private File portFilePath = null;
    private File logPath = null;

    Builder setEmulatorPath(File path) {
      this.emulatorPath = path;
      return this;
    }

    Builder setPortFilePath(File path) {
      this.portFilePath = path;
      return this;
    }

    Builder setLogPath(File path) {
      this.logPath = path;
      return this;
    }

    EmulatorController build() {
      Preconditions.checkNotNull(emulatorPath, "emulatorPath can't be null");
      Preconditions.checkNotNull(portFilePath, "portFilePath can't be null");

      File effectiveLogPath = logPath;

      if (effectiveLogPath== null) {
        effectiveLogPath = new File(portFilePath.toString().replaceFirst("\\.[^\\\\/]+$", ".log"));
        if (effectiveLogPath.equals(portFilePath)) {
          effectiveLogPath = new File(effectiveLogPath.toString() + ".log");
        }
      }


      return new EmulatorController(emulatorPath, portFilePath, effectiveLogPath);
    }

  }
}
