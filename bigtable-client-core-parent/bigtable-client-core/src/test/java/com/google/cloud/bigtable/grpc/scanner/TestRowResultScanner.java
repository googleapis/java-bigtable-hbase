/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.scanner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

@RunWith(JUnit4.class)
public class TestRowResultScanner {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  private static final String TEST_VALUE = "test-value";
  private static final String[] TEST_ARRAY = new String[0];

  private RowResultScanner<String> scanner;

  @Mock private ServerStream<String> stream;

  @Test
  public void testNextWhenValueIsPresent() {
    List<String> listOfOneElement = ImmutableList.of(TEST_VALUE);
    when(stream.iterator()).thenReturn(listOfOneElement.iterator());
    scanner = new RowResultScanner<>(stream, TEST_ARRAY);
    // Should return value
    assertEquals(TEST_VALUE, scanner.next());
    // Once all the values are iterated, Scanner should return null.
    assertNull(scanner.next());
    verify(stream).iterator();
  }

  @Test
  public void testNextWhenNoValueIsPresent() {
    List<String> listOfWithoutElement = ImmutableList.of();
    when(stream.iterator()).thenReturn(listOfWithoutElement.iterator());
    scanner = new RowResultScanner<>(stream, TEST_ARRAY);
    assertNull(scanner.next());
    verify(stream).iterator();
  }

  @Test
  public void testNextWithCount() {
    List<String> listWithMultipleElmt = ImmutableList.of("firstValue", "secondValue", "thirdValue");
    when(stream.iterator()).thenReturn(listWithMultipleElmt.iterator());
    scanner = new RowResultScanner<>(stream, TEST_ARRAY);
    String[] actualArr = scanner.next(3);
    assertArrayEquals(listWithMultipleElmt.toArray(TEST_ARRAY), actualArr);
    verify(stream).iterator();
  }

  @Test
  public void testAvailableOperation() {
    when(stream.isReceiveReady()).thenReturn(true);
    scanner = new RowResultScanner<>(stream, TEST_ARRAY);
    assertEquals(1, scanner.available());
    when(stream.isReceiveReady()).thenReturn(false);
    assertEquals(0, scanner.available());
    verify(stream, times(2)).isReceiveReady();
  }

  @Test
  public void testClose() throws Exception {
    List<String> listOfOneElement = ImmutableList.of(TEST_VALUE, TEST_VALUE);
    when(stream.iterator()).thenReturn(listOfOneElement.iterator());
    doNothing().when(stream).cancel();
    try (ResultScanner<String> closedScanner = new RowResultScanner<>(stream, TEST_ARRAY)) {
      assertEquals(TEST_VALUE, closedScanner.next());
    }
    verify(stream).iterator();
    // verifying call the RowResultScanner#close.
    verify(stream).cancel();
  }
}
