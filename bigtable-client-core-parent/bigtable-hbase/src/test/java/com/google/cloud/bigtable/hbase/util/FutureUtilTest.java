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
package com.google.cloud.bigtable.hbase.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FutureUtilTest {
  private ExecutorService executor;

  @Before
  public void setUp() throws Exception {
    executor = Executors.newCachedThreadPool();
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdown();
  }

  @Test
  public void unwrapBubblesErrorsBare() {
    Set<Class<? extends Throwable>> unwrappedErrors =
        ImmutableSet.of(IOException.class, RuntimeException.class, Error.class);

    for (Class<? extends Throwable> cls : unwrappedErrors) {
      final Future<?> future = executor.submit(new ErrorCreator(cls));
      Throwable actualException =
          assertThrows(
              cls,
              new ThrowingRunnable() {
                @Override
                public void run() throws Throwable {
                  FutureUtil.unwrap(future);
                }
              });

      assertThat(actualException).hasMessageThat().isEqualTo("fake error");
      assertThat(actualException).hasCauseThat().isNull();
    }
  }

  @Test
  public void unwrapWrapsOtherErrors() {
    final Future<?> future = executor.submit(new ErrorCreator(TimeoutException.class));
    IOException actualException =
        assertThrows(
            IOException.class,
            new ThrowingRunnable() {
              @Override
              public void run() throws Throwable {
                FutureUtil.unwrap(future);
              }
            });
    assertThat(actualException)
        .hasMessageThat()
        .isEqualTo("java.util.concurrent.TimeoutException: fake error");
    assertThat(actualException).hasCauseThat().isInstanceOf(TimeoutException.class);
    assertThat(actualException).hasCauseThat().hasMessageThat().isEqualTo("fake error");
  }

  @Test
  public void concatsStacktraces() {
    final Future<Void> future = executor.submit(new ErrorCreator(IOException.class));

    List<StackTraceElement> originalStack =
        ImmutableList.copyOf(
            assertThrows(
                    ExecutionException.class,
                    new ThrowingRunnable() {
                      @Override
                      public void run() throws Throwable {
                        future.get();
                      }
                    })
                .getCause()
                .getStackTrace());

    List<StackTraceElement> augmentedStack =
        ImmutableList.copyOf(
            assertThrows(
                    IOException.class,
                    new ThrowingRunnable() {
                      @Override
                      public void run() throws Throwable {
                        FutureUtil.unwrap(future);
                      }
                    })
                .getStackTrace());

    // Make sure that the original stacktrace is preserved
    assertThat(augmentedStack).containsAtLeastElementsIn(originalStack);

    // Ensure that the caller's portion of the stacktrace is present
    StackTraceElement[] topTrace = Thread.currentThread().getStackTrace();
    List<StackTraceElement> relevantTopTrace = Arrays.asList(topTrace).subList(2, topTrace.length);
    assertThat(augmentedStack).containsAtLeastElementsIn(relevantTopTrace);
  }

  private static class ErrorCreator implements Callable<Void> {
    private final Class errorClass;

    private ErrorCreator(Class errorClass) {
      this.errorClass = errorClass;
    }

    @Override
    public Void call() throws Exception {
      method1();
      return null;
    }

    private void method1() throws Exception {
      Throwable t = (Throwable) errorClass.getConstructor(String.class).newInstance("fake error");
      if (Error.class.isAssignableFrom(errorClass)) {
        throw (Error) t;
      } else {
        throw (Exception) t;
      }
    }
  }
}
