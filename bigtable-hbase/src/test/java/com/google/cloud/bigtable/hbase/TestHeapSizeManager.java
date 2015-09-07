package com.google.cloud.bigtable.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.cloud.bigtable.hbase.BigtableBufferedMutator.HeapSizeManager;
import com.google.common.util.concurrent.ListenableFuture;

@RunWith(JUnit4.class)
public class TestHeapSizeManager {

  @Mock
  ExecutorService executorService;

  @Mock
  ListenableFuture<?> future;

  @Before
  public void setup(){
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testRpcCount() throws InterruptedException{
    HeapSizeManager underTest = new HeapSizeManager(100l, 2, executorService);

    assertFalse(underTest.isFull());
    assertFalse(underTest.hasInflightRequests());

    long id = underTest.registerOperationWithHeapSize(1);
    assertFalse(underTest.isFull());
    assertTrue(underTest.hasInflightRequests());

    long id2 = underTest.registerOperationWithHeapSize(1);
    assertTrue(underTest.hasInflightRequests());
    assertTrue(underTest.isFull());

    underTest.markOperationCompleted(id);
    assertFalse(underTest.isFull());
    assertTrue(underTest.hasInflightRequests());

    underTest.markOperationCompleted(id2);
    assertFalse(underTest.isFull());
    assertFalse(underTest.hasInflightRequests());
}

  @Test
  public void testSize() throws InterruptedException{
    HeapSizeManager underTest = new HeapSizeManager(10l, 1000, executorService);
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

    underTest.markOperationCompleted(id);
    assertFalse(underTest.isFull());
    assertEquals(5l, underTest.getHeapSize());

    underTest.markOperationCompleted(id2);
    underTest.markOperationCompleted(id3);
    assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testCallback() throws InterruptedException {
    final List<Runnable> runnables = new ArrayList<>();
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        runnables.add((Runnable) invocation.getArguments()[0]);
        return null;
      }
    }).when(future).addListener(Matchers.any(Runnable.class), Matchers.eq(executorService));

    HeapSizeManager underTest = new HeapSizeManager(10l, 1000, executorService);
    long id = underTest.registerOperationWithHeapSize(5l);
    assertTrue(underTest.hasInflightRequests());

    underTest.addCallback(future, id);
    assertFalse(runnables.isEmpty());
    assertTrue(underTest.hasInflightRequests());

    for (Runnable runnable : runnables) {
      runnable.run();
    }
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
      final HeapSizeManager underTest = new HeapSizeManager(1l, 1, executorService);
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
      underTest.markOperationCompleted(id);

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
      final HeapSizeManager underTest = new HeapSizeManager(1l, 1, executorService);
      long id = underTest.registerOperationWithHeapSize(5l);
      assertTrue(underTest.hasInflightRequests());
      final AtomicBoolean allOperationsDone = new AtomicBoolean();
      pool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            underTest.waitUntilAllOperationsAreDone();
            allOperationsDone.set(true);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      // Give some time for the Runnable to be executed.
      Thread.sleep(10);
      assertFalse(allOperationsDone.get());
      underTest.markOperationCompleted(id);

      // Give more time for the Runnable to be executed.
      Thread.sleep(10);
      assertTrue(allOperationsDone.get());
    } finally {
      pool.shutdownNow();
    }
  }
}