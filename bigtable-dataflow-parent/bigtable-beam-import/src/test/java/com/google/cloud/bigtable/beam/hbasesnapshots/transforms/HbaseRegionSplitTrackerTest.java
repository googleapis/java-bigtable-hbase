/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the {@link HbaseRegionSplitTracker} to ensure correct delegation and dynamic splitting
 * behavior.
 */
@RunWith(JUnit4.class)
public class HbaseRegionSplitTrackerTest {

  private ByteKeyRange range;
  private HbaseRegionSplitTracker trackerWithDynamicSplitting;
  private HbaseRegionSplitTracker trackerWithoutDynamicSplitting;

  @Before
  public void setUp() {
    range = ByteKeyRange.of(ByteKey.copyFrom("a".getBytes()), ByteKey.copyFrom("z".getBytes()));
    trackerWithDynamicSplitting = new HbaseRegionSplitTracker("snapshot", "region", range, true);
    trackerWithoutDynamicSplitting =
        new HbaseRegionSplitTracker("snapshot", "region", range, false);
  }

  /** Tests that {@link HbaseRegionSplitTracker#currentRestriction()} returns the initial range. */
  @Test
  public void testCurrentRestriction() {
    assertEquals(range, trackerWithDynamicSplitting.currentRestriction());
    assertEquals(range, trackerWithoutDynamicSplitting.currentRestriction());
  }

  /** Tests that {@link HbaseRegionSplitTracker#tryClaim(ByteKey)} succeeds for keys in range. */
  @Test
  public void testTryClaim() {
    assertTrue(trackerWithDynamicSplitting.tryClaim(ByteKey.copyFrom("b".getBytes())));
    assertTrue(trackerWithoutDynamicSplitting.tryClaim(ByteKey.copyFrom("b".getBytes())));

    // Claim outside range should fail
    assertFalse(trackerWithDynamicSplitting.tryClaim(ByteKey.copyFrom("~".getBytes())));
    assertFalse(trackerWithoutDynamicSplitting.tryClaim(ByteKey.copyFrom("~".getBytes())));
  }

  /**
   * Tests that {@link HbaseRegionSplitTracker#trySplit(double)} succeeds when dynamic splitting is
   * enabled.
   */
  @Test
  public void testTrySplit_enabled() {
    // Claim something to make remainder smaller
    trackerWithDynamicSplitting.tryClaim(ByteKey.copyFrom("m".getBytes()));

    SplitResult<ByteKeyRange> split = trackerWithDynamicSplitting.trySplit(0.5);
    assertNotNull(split);
    assertNotNull(split.getPrimary());
    assertNotNull(split.getResidual());
  }

  /**
   * Tests that {@link HbaseRegionSplitTracker#trySplit(double)} returns null when dynamic splitting
   * is disabled.
   */
  @Test
  public void testTrySplit_disabled() {
    trackerWithoutDynamicSplitting.tryClaim(ByteKey.copyFrom("m".getBytes()));

    SplitResult<ByteKeyRange> split = trackerWithoutDynamicSplitting.trySplit(0.5);
    assertNull(split);
  }

  /**
   * Tests that {@link HbaseRegionSplitTracker#checkDone()} throws exception if not all keys
   * claimed.
   */
  @Test
  public void testCheckDone_failsIfNotDone() {
    // Should fail if not done
    try {
      trackerWithDynamicSplitting.checkDone();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  /** Tests that {@link HbaseRegionSplitTracker#checkDone()} succeeds when all keys claimed. */
  @Test
  public void testCheckDone_success() {
    // Claim the start key to initialize
    trackerWithDynamicSplitting.tryClaim(ByteKey.copyFrom("a".getBytes()));
    // Claim the end key to mark as done
    trackerWithDynamicSplitting.tryClaim(ByteKey.copyFrom("z".getBytes()));

    trackerWithDynamicSplitting.checkDone(); // Should not throw
  }

  /** Tests that {@link HbaseRegionSplitTracker#isBounded()} returns BOUNDED. */
  @Test
  public void testIsBounded() {
    assertEquals(RestrictionTracker.IsBounded.BOUNDED, trackerWithDynamicSplitting.isBounded());
  }
}
