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
package com.google.cloud.bigtable.mirroring.core.utils.faillog;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DefaultAppenderTest {
  Path tmpdir;

  @Before
  public void createTestDirectory() throws IOException {
    tmpdir = Files.createTempDirectory("cbt_hbase_appender_test");
  }

  @After
  public void deleteTestDirectory() throws IOException {
    // Delete the temporary directory with the created files.
    Files.walkFileTree(
        tmpdir,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes)
              throws IOException {
            Files.delete(path);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
            if (e != null) {
              throw e;
            }
            Files.delete(path);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private List<String> listLogFiles() throws IOException {
    ArrayList<String> paths = new ArrayList<>();
    for (Path file : Files.newDirectoryStream(tmpdir)) {
      paths.add(file.getFileName().toString());
    }
    return paths;
  }

  private DefaultAppender createAppender() throws IOException {
    final int maxBufferSize = 4096; // just an arbitrary value
    final boolean dropOnOverflow = false;
    return new DefaultAppender(tmpdir.resolve("test").toString(), maxBufferSize, dropOnOverflow);
  }

  @Test
  public void startupAndShutdown() throws Exception {
    try (Appender appender = createAppender()) {
      appender.append("foo".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void pathNamesHaveTimestampAndTid() throws Exception {
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS");
    // File names should look like test.yyyy-MM-dd_HH-mm-ss.SSS.TID"
    final Pattern filePattern =
        Pattern.compile(
            "test\\.(20[0-9]{2}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}\\.[0-9]+)\\.([0-9]+)");
    final long thisThreadId = Thread.currentThread().getId();

    Date beforeLogCreation = new Date();
    try (Appender appender = createAppender()) {
      appender.append("foo".getBytes(StandardCharsets.UTF_8));
    }
    try (Appender appender = createAppender()) {
      appender.append("bar".getBytes(StandardCharsets.UTF_8));
    }
    Date afterLogCreation = new Date();

    List<String> paths = listLogFiles();
    assertEquals(2, paths.size());

    for (String path : paths) {
      Matcher matcher = filePattern.matcher(path);
      assertTrue(matcher.matches());
      String timestampStr = matcher.group(1);

      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      final Date timestamp = dateFormat.parse(timestampStr);
      assertThat(timestamp).isAtLeast(beforeLogCreation);
      assertThat(timestamp).isAtMost(afterLogCreation);
      assertEquals(Long.parseLong(matcher.group(2)), thisThreadId);
    }
  }

  @Test
  public void contentsAreProper() throws Exception {
    try (Appender appender = new DefaultAppender(tmpdir.resolve("test").toString(), 4096, false)) {
      appender.append("foo".getBytes(StandardCharsets.UTF_8));
      appender.append("bar".getBytes(StandardCharsets.UTF_8));
    }
    List<String> paths = listLogFiles();
    assertEquals(1, paths.size());
    assertArrayEquals("foobar".getBytes(), Files.readAllBytes(tmpdir.resolve(paths.get(0))));
  }

  @Test
  public void interruptedThreadStopsAcceptingMoreEntries() throws Exception {
    Set<Thread> threadsBeforeAppenderIsCreated = Thread.getAllStackTraces().keySet();
    try (Appender appender = new DefaultAppender(tmpdir.resolve("test").toString(), 4096, false)) {
      appender.append("foo".getBytes(StandardCharsets.UTF_8));

      Set<Thread> threadsCreatedByAppender =
          Sets.difference(Thread.getAllStackTraces().keySet(), threadsBeforeAppenderIsCreated);
      // Appender should create one thread.
      assertEquals(1, threadsCreatedByAppender.size());
      Thread appenderThread = threadsCreatedByAppender.iterator().next();
      // This should kill the thread flushing logs to disk, which should make the appender stop
      // accepting more data.
      appenderThread.interrupt();
      appenderThread.join();
      try {
        appender.append("bar".getBytes(StandardCharsets.UTF_8));
        fail("IllegalStateException expected.");
      } catch (IllegalStateException e) {
        Throwable cause = e.getCause();
        // Depending on whether the interruption happens while writing to file or while sleeping,
        // the exception is `ClosedByInterruptException` or `InterruptedException`. In order to
        // future-proof this test a bit, let's match the class' name.
        assertTrue(cause.getClass().toString().contains("Interrupt"));
      }
    }
  }
}
