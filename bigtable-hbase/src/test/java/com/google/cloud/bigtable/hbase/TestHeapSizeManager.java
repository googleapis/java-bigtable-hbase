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
package com.google.cloud.bigtable.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Tests {@link HeapSizeManager}.
 *
 */
public class TestHeapSizeManager {

  @Mock
  ExecutorService executorService;
  
  @Mock
  ListenableFuture future;
  
  @Before
  public void setup(){
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testRpcCount() throws InterruptedException{
    HeapSizeManager underTest = new HeapSizeManager(100l, 2, executorService);

    Assert.assertFalse(underTest.isFull());
    Assert.assertFalse(underTest.hasInflightRequests());

    long id = underTest.registerOperationWithHeapSize(1);
    Assert.assertFalse(underTest.isFull());
    Assert.assertTrue(underTest.hasInflightRequests());

    long id2 = underTest.registerOperationWithHeapSize(1);
    Assert.assertTrue(underTest.hasInflightRequests());
    Assert.assertTrue(underTest.isFull());

    underTest.operationComplete(id);
    Assert.assertFalse(underTest.isFull());
    Assert.assertTrue(underTest.hasInflightRequests());

    underTest.operationComplete(id2);
    Assert.assertFalse(underTest.isFull());
    Assert.assertFalse(underTest.hasInflightRequests());
}

  @Test
  public void testSize() throws InterruptedException{
    HeapSizeManager underTest = new HeapSizeManager(10l, 1000, executorService);
    long id = underTest.registerOperationWithHeapSize(5l);
    Assert.assertTrue(underTest.hasInflightRequests());
    Assert.assertFalse(underTest.isFull());
    Assert.assertEquals(5l, underTest.getHeapSize());

    long id2 = underTest.registerOperationWithHeapSize(4l);
    Assert.assertTrue(underTest.hasInflightRequests());
    Assert.assertFalse(underTest.isFull());
    Assert.assertEquals(9l, underTest.getHeapSize());

    long id3 = underTest.registerOperationWithHeapSize(1l);
    Assert.assertTrue(underTest.hasInflightRequests());
    Assert.assertTrue(underTest.isFull());
    Assert.assertEquals(10l, underTest.getHeapSize());

    underTest.operationComplete(id);
    Assert.assertFalse(underTest.isFull());
    Assert.assertEquals(5l, underTest.getHeapSize());

    underTest.operationComplete(id2);
    underTest.operationComplete(id3);
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCallback() throws InterruptedException {
    final List<Runnable> runnables = new ArrayList<>();
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Runnable runnable = (Runnable) invocation.getArguments()[0];
        runnables.add(runnable);
        return null;
      }
    }).when(future).addListener(Matchers.any(Runnable.class), Matchers.eq(executorService));

    HeapSizeManager underTest = new HeapSizeManager(10l, 1000, executorService);
    long id = underTest.registerOperationWithHeapSize(5l);
    Assert.assertTrue(underTest.hasInflightRequests());

    underTest.addCallback(future, id);
    Assert.assertFalse(runnables.isEmpty());
    Assert.assertTrue(underTest.hasInflightRequests());

    for (Runnable runnable : runnables) {
      runnable.run();
    }
    Assert.assertFalse(underTest.hasInflightRequests());
  }
}
