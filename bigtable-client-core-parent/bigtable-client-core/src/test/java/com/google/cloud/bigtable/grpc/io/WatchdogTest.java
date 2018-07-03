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
package com.google.cloud.bigtable.grpc.io;

import com.google.api.client.testing.http.FixedClock;
import com.google.common.collect.Queues;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WatchdogTest {

  private FixedClock clock;
  private static final long waitTimeMs = TimeUnit.SECONDS.toMillis(10);
  private static final long idleTimeMs = TimeUnit.MINUTES.toMillis(5);

  private Watchdog watchdog;
  private FakeClientCall call;
  private ClientCall<String, String> watchedCall;
  private AccumulatingListener listener;

  @Before
  public void setUp() {
    clock = new FixedClock(0);
    watchdog = new Watchdog(clock, waitTimeMs, idleTimeMs);

    call = new FakeClientCall();
    watchedCall = watchdog.watch(call);
    listener = new AccumulatingListener();

    watchedCall.start(listener, new Metadata());
  }

  @Test
  public void testRequestPassthrough() throws Exception {
    watchedCall.request(1);
    Assert.assertEquals(1, (int) call.pulls.poll());
  }

  @Test
  public void testWaitTimeout() throws Exception {
    watchedCall.request(1);

    clock.setTime(waitTimeMs - 1);
    watchdog.run();
    Assert.assertNull(call.cancelMessage);

    clock.setTime(waitTimeMs);
    watchdog.run();
    Assert.assertNotNull(call.cancelMessage);

    call.listener.onClose(
        Status.ABORTED.withDescription("Some upstream exception representing cancellation"),
        new Metadata()
    );

    Assert.assertEquals(listener.closeStatus.getDescription(),
        "Some upstream exception representing cancellation");
  }

  @Test
  public void testIdleTimeout() throws InterruptedException {
    clock.setTime(idleTimeMs - 1);
    watchdog.run();
    Assert.assertNull(call.cancelMessage);

    clock.setTime(idleTimeMs);
    watchdog.run();
    Assert.assertNotNull(call.cancelMessage);

    call.listener
        .onClose(
            Status.CANCELLED.withDescription("Some upstream exception representing cancellation"),
            new Metadata());

    Assert.assertEquals(listener.closeStatus.getDescription(),
        "Some upstream exception representing cancellation");
  }

  static class FakeClientCall extends ClientCall<String, String> {

    Listener<String> listener;
    Queue<Integer> pulls = Queues.newArrayDeque();
    String cancelMessage;
    Throwable cancelCause;
    boolean halfClosed;
    Queue<String> sentMessages = Queues.newArrayDeque();

    @Override
    public void start(Listener<String> responseListener, Metadata headers) {
      this.listener = responseListener;
    }

    @Override
    public void request(int numMessages) {
      pulls.add(numMessages);
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
      cancelMessage = message;
      cancelCause = cause;
    }

    @Override
    public void halfClose() {
      halfClosed = true;
    }

    @Override
    public void sendMessage(String message) {
      sentMessages.add(message);
    }
  }

  static class AccumulatingListener extends ClientCall.Listener<String> {

    Queue<String> messages = Queues.newArrayDeque();
    Status closeStatus;

    @Override
    public void onMessage(String message) {
      messages.add(message);
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
      closeStatus = status;
    }
  }

}
