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
package com.google.cloud.bigtable.grpc.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

@RunWith(JUnit4.class)
public class TestHeapSizeManager {

  @Mock
  ListenableFuture<?> future;

  @Before
  public void setup(){
    MockitoAnnotations.initMocks(this);
  }


  @Test
  public void testRpcCount() throws InterruptedException{
    HeapSizeManager underTest = new HeapSizeManager(100l, 2);

    assertFalse(underTest.isFull());
    assertFalse(underTest.hasInflightRequests());

    long id = underTest.registerOperationWithHeapSize(1);
    assertFalse(underTest.isFull());
    assertTrue(underTest.hasInflightRequests());

    long id2 = underTest.registerOperationWithHeapSize(1);
    assertTrue(underTest.hasInflightRequests());
    assertTrue(underTest.isFull());

    underTest.markCanBeCompleted(id);
    assertFalse(underTest.isFull());
    assertTrue(underTest.hasInflightRequests());

    underTest.markCanBeCompleted(id2);
    assertFalse(underTest.isFull());
    assertFalse(underTest.hasInflightRequests());
}

  @Test
  public void testSize() throws InterruptedException{
    HeapSizeManager underTest = new HeapSizeManager(10l, 1000);
    long id = underTest.registerOperationWithHeapSize(5l);
    assertTrue(underTest.hasInflightRequests());
    assertFalse(underTest.isFull());
    assertEquals(5l, underTest.getHeapSize());

    long id2 = underTest.registerOperationWithHeapSize(4l);
    assertTrue(underTest.hasInflightRequests());
    assertFalse(underTest.isFull());
    assertEquals(9l, underTest.getHeapSize());

    long id3 = underTest.registerOperationWithHeapSize(1l);
    assertTrue(underTest.hasInflightRequests());
    assertTrue(underTest.isFull());
    assertEquals(10l, underTest.getHeapSize());

    underTest.markCanBeCompleted(id);
    assertFalse(underTest.isFull());
    assertEquals(5l, underTest.getHeapSize());

    underTest.markCanBeCompleted(id2);
    underTest.markCanBeCompleted(id3);
    assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testCallback() throws InterruptedException {
    HeapSizeManager underTest = new HeapSizeManager(10l, 1000);
    long id = underTest.registerOperationWithHeapSize(5l);
    assertTrue(underTest.hasInflightRequests());

    FutureCallback<?> callback = underTest.addCallback(future, id);
    assertTrue(underTest.hasInflightRequests());
    callback.onSuccess(null);
    Thread.sleep(100);
    assertFalse(underTest.hasInflightRequests());
  }

  @Test
  /**
   * Test to make sure that HeapSizeManager does not register an operation if the HeapSizeManager
   * size limit was reached.
   */
  public void testSizeLimitReachWaits() throws InterruptedException {
    ExecutorService pool = Executors.newCachedThreadPool();
    try {
      final HeapSizeManager underTest = new HeapSizeManager(1l, 1);
      long id = underTest.registerOperationWithHeapSize(5l);
      assertTrue(underTest.hasInflightRequests());
      assertTrue(underTest.isFull());
      final AtomicBoolean secondRequestRegistered = new AtomicBoolean();
      pool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            underTest.registerOperationWithHeapSize(5l);
            secondRequestRegistered.set(true);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      // Give some time for the Runnable to be executed.
      Thread.sleep(10);
      assertFalse(secondRequestRegistered.get());
      underTest.markCanBeCompleted(id);

      // Give more time for the Runnable to be executed.
      Thread.sleep(10);
      assertTrue(secondRequestRegistered.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testWaitUntilAllOperationsAreDone() throws InterruptedException {
    ExecutorService pool = Executors.newCachedThreadPool();
    try {
      final HeapSizeManager underTest = new HeapSizeManager(1l, 1);
      long id = underTest.registerOperationWithHeapSize(5l);
      assertTrue(underTest.hasInflightRequests());
      final AtomicBoolean allOperationsDone = new AtomicBoolean();
      pool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            underTest.flush();
            allOperationsDone.set(true);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      // Give some time for the Runnable to be executed.
      Thread.sleep(100);
      assertFalse(allOperationsDone.get());
      underTest.markCanBeCompleted(id);

      // Give more time for the Runnable to be executed.
      Thread.sleep(50);
      assertTrue(allOperationsDone.get());
    } finally {
      pool.shutdownNow();
    }
  }
}