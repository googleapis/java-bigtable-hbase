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
package com.google.cloud.bigtable.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.api.client.util.BackOff;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class AsyncBackOffTest {

  @Rule
  public MockitoRule initMocks = MockitoJUnit.rule();

  @Mock
  private BackOff backOff;

  @Mock
  private Ticker ticker;

  private AsyncBackOff underTest;

  @Before
  public void setUp() {
    underTest = new AsyncBackOff(ticker, backOff);
  }

  @Test
  public void testInitialPassthrough() {
    Assert.assertTrue(underTest.tryAcquire());
  }

  @Test
  public void testNextCallWait() throws IOException {
    when(backOff.nextBackOffMillis()).thenReturn(100L);
    when(ticker.read()).thenReturn(0L);

    Preconditions.checkState(underTest.tryAcquire());

    assertFalse(underTest.tryAcquire());
  }

  @Test
  public void testLockOpens() throws Exception {
    when(backOff.nextBackOffMillis()).thenReturn(100L);
    when(ticker.read()).thenReturn(0L);

    // acquire
    underTest.tryAcquire();
    // make sure its closed
    Preconditions.checkState(!underTest.tryAcquire());

    // fast forward to the future
    when(ticker.read()).thenReturn(100 * 1_000_000L); //ns

    // lock should be open
    assertTrue(underTest.tryAcquire());
  }

  @Test
  public void testLockOpenAfterReset() throws IOException {
    when(backOff.nextBackOffMillis()).thenReturn(100L);

    // acquire
    underTest.tryAcquire();
    // make sure its closed
    Preconditions.checkState(!underTest.tryAcquire());

    underTest.reset();

    assertTrue(underTest.tryAcquire());
  }
}
