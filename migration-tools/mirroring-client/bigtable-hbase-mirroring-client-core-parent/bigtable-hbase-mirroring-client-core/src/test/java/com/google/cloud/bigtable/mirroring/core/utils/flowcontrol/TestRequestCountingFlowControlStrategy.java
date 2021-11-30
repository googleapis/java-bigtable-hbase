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
package com.google.cloud.bigtable.mirroring.core.utils.flowcontrol;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestRequestCountingFlowControlStrategy {
  @Test
  public void testBlockingWhenCounterReachesTheLimit() {
    RequestCountingFlowControlStrategy fc = new RequestCountingFlowControlStrategy(2, 1000);

    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {})))
        .isTrue(); // 0
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isTrue(); // 1
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isTrue(); // 2
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isFalse(); // 2
    fc.releaseResource(new RequestResourcesDescription(new boolean[] {true})); // 1
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isTrue(); // 2
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isFalse(); // 2
  }

  @Test
  public void testOversizedRequestIsAllowedIfNoOtherResourcesAreAcquired() {
    RequestCountingFlowControlStrategy fc = new RequestCountingFlowControlStrategy(2, 1000);

    assertThat(
            fc.tryAcquireResource(
                new RequestResourcesDescription(new boolean[] {true, true, true})))
        .isTrue();
    fc.releaseResource(new RequestResourcesDescription(new boolean[] {true, true, true}));

    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isTrue();
    assertThat(
            fc.tryAcquireResource(
                new RequestResourcesDescription(new boolean[] {true, true, true})))
        .isFalse();

    fc.releaseResource(new RequestResourcesDescription(new boolean[] {true}));
    assertThat(
            fc.tryAcquireResource(
                new RequestResourcesDescription(new boolean[] {true, true, true})))
        .isTrue();
  }

  @Test
  public void testBlockingOnRequestSize() {
    RequestCountingFlowControlStrategy fc = new RequestCountingFlowControlStrategy(1000, 16);

    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {})))
        .isTrue(); // 0
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isTrue(); // 8
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isTrue(); // 16
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isFalse(); // 16
    fc.releaseResource(new RequestResourcesDescription(new boolean[] {true})); // 8
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isTrue(); // 16
    assertThat(fc.tryAcquireResource(new RequestResourcesDescription(new boolean[] {true})))
        .isFalse(); // 16
  }
}
