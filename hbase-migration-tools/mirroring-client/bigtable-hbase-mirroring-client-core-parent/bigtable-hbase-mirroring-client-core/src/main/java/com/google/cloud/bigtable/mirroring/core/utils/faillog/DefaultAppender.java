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

import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_FAILLOG_PREFIX_PATH_KEY;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.cloud.bigtable.mirroring.core.MirroringOptions;
import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;
import java.util.TimeZone;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Write log entries asynchronously.
 *
 * <p>Objects of this class log arbitrary byte sequences to files.
 *
 * <p>Most of the time, appending data to this `Appender` should be non-blocking. Writing to disk is
 * performed by a separate thread, which objects of this class create.
 */
public class DefaultAppender implements Appender {
  protected static final Log LOG = LogFactory.getLog(DefaultAppender.class);
  private final LogBuffer buffer;
  private final Writer writer;

  public DefaultAppender(MirroringOptions.Faillog options) throws IOException {
    this(options.prefixPath, options.maxBufferSize, options.dropOnOverflow);
  }

  /**
   * Create a `DefaultAppender`.
   *
   * <p>The created `DefaultAppender` will create a thread for flushing the data asynchronously. The
   * data will be transferred to the thread via a buffer of a given maximum size (`maxBufferSize`).
   * Entries larger than `maxBufferSize` will be accepted by the `DefaultAppender`, breaking the
   * `maxBufferSize` but a single such entry fill up the whole buffer. Depending on the
   * `dropOnOverflow` parameter, appending data when the buffer is full, will either yield it
   * dropped or the calling thread blocked until the flusher thread catches up.
   *
   * @param pathPrefix the prefix used for generating the name of the log file
   * @param maxBufferSize maximum size of the buffer used for communicating with the thread flushing
   *     the data to disk
   * @param dropOnOverflow whether to drop data if the thread flushing the data to disk is not
   *     keeping up or to block until it catches up
   * @throws IOException when writing the log file fails
   */
  public DefaultAppender(String pathPrefix, int maxBufferSize, boolean dropOnOverflow)
      throws IOException {

    Preconditions.checkArgument(
        pathPrefix != null && !pathPrefix.isEmpty(),
        "DefaultAppender's %s key shouldn't be empty.",
        MIRRORING_FAILLOG_PREFIX_PATH_KEY);

    // In case of an unclean shutdown the end of the log file may contain partially written log
    // entries. In order to simplify reading the log files, we assume that everything following an
    // incomplete entry is to be discarded. In order to satisfy that assumption, we should not
    // append to files from previous runs, which is why we create a new file every time.
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    final String dateString = dateFormat.format(new Date());
    final String threadIdString = String.valueOf(Thread.currentThread().getId());
    final OutputStream stream =
        new BufferedOutputStream(
            Files.newOutputStream(
                Paths.get(String.format("%s.%s.%s", pathPrefix, dateString, threadIdString)),
                CREATE_NEW,
                SYNC,
                WRITE),
            maxBufferSize);
    buffer = new LogBuffer(maxBufferSize, dropOnOverflow);
    writer = new Writer(buffer, stream);
    writer.start();
  }

  /**
   * Append data to the log.
   *
   * <p>This method can be called from multiple threads simultaneously.
   *
   * <p>Unless `dropOnOverflow` was set in the c-tor, this method may block in case the buffer
   * cannot fit the provided data. It will be unblocked when the thread writing the data to disk
   * catches up.
   *
   * @param data the data to be appended
   * @throws InterruptedException in case the thread is interrupted
   */
  @Override
  public void append(byte[] data) throws InterruptedException {
    if (!buffer.append(data)) {
      LOG.error("Failed mutation log overflow, discarded an entry.");
    }
  }

  /**
   * Close the `DefaultAppender`.
   *
   * <p>This method may block while waiting until the pending data is flushed to disk. Any further
   * attempts to `append()` more data will fail.
   *
   * @throws Exception in case writing to disk fails
   */
  @Override
  public void close() throws Exception {
    buffer.close();
    writer.join();
  }

  private static class Writer extends Thread {
    private final LogBuffer buffer;
    private final OutputStream stream;

    Writer(LogBuffer buffer, OutputStream stream) {
      this.buffer = buffer;
      this.stream = stream;
    }

    public void run() {
      // On one hand, we want to sync the logs to disk as fast as we can, but on the other hand we
      // do not want those frequent `fsync()`s to slow the log down to a point when it has not
      // enough throughput to flush the log entries on time. In order to strike this balance, we
      // increase the write-buffer adaptively to utilize whatever throughput the disk can give us.
      // This is done by batching in `LogBuffer`'s memory all log entries which are submitted while
      // the previous batch is being flushed. That way, the faster they are submitted or the slower
      // the disk, the larger is the batch, hence less frequent `fsync()`s.
      try {
        for (; ; ) {
          Queue<byte[]> buffersToFlush = buffer.drain();
          if (buffersToFlush == null) {
            // Log buffer was closed, which means we're shutting down.
            break;
          }
          for (byte[] buffer : buffersToFlush) {
            stream.write(buffer);
          }
          stream.flush(); // the stream has been opened in SYNC mode, so this is actually persisted
        }
      } catch (Throwable e) {
        // If we can't write the failed mutations, let's close the buffer so that the user gets an
        // exception when attempting to write more data.
        LOG.error(
            "Writing failed logs failed. Exiting, no more failed mutations will be written.", e);
        buffer.closeWithCause(e);
      } finally {
        try {
          stream.flush();
          stream.close();
        } catch (IOException e) {
          // There's not much we can do - we just lost some log entries, probably.
          LOG.error("Failure to close failed messages log.", e);
        }
      }
    }
  }

  public static class Factory implements Appender.Factory {
    @Override
    public Appender create(MirroringOptions.Faillog options) throws IOException {
      return new DefaultAppender(options);
    }
  }
}
