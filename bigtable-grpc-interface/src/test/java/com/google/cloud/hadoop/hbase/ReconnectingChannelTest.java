package com.google.cloud.hadoop.hbase;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import io.grpc.MethodDescriptor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * Tests {@link ReconnectingChannel}
 */
@RunWith(JUnit4.class)
public class ReconnectingChannelTest {

  private static final long REFRESH_MS = 500;
  @Mock
  private ReconnectingChannel.Factory factory;

  @Mock
  private CloseableChannel channel;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void test() throws IOException{
    when(factory.create()).thenReturn(channel);
    when(channel.newCall(any(MethodDescriptor.class))).thenReturn(null);
    ReconnectingChannel test =
        new ReconnectingChannel(REFRESH_MS, Executors.newFixedThreadPool(1), factory);
    Mockito.verify(factory, times(1)).create();

    test.newCall(null);
    Mockito.verify(channel, times(1)).newCall(any(MethodDescriptor.class));

    try {
      Thread.sleep(REFRESH_MS);
    } catch (InterruptedException ignored) {
      // Do nothing on interrupt.
    }

    test.newCall(null);
    Mockito.verify(channel, times(2)).newCall(any(MethodDescriptor.class));

    try {
      Thread.sleep(REFRESH_MS);
    } catch (InterruptedException ignored) {
      // Do nothing on interrupt.
    }

    Mockito.verify(factory, atLeast(2)).create();
    Mockito.verify(channel, times(1)).close();

    test.close();
    Mockito.verify(channel, times(2)).close();
    
    try {
      test.newCall(null);
      Assert.fail("Expected IllegalStateException on a closed channel");
    } catch (IllegalStateException expected) {
      // expected
    }
  }
}
