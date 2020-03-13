/*
 * Copyright 2017 Google Inc.
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
package com.google.cloud.bigtable.hbase.adapters.read;

import static org.hamcrest.CoreMatchers.containsString;

import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestRowCell {

  @Test
  public void testToString() {
    RowCell rowCell =
        new RowCell(
            "mykey".getBytes(),
            "myfamily".getBytes(),
            "myqualifier".getBytes(),
            1487963474314L,
            "myvalue".getBytes());
    String result = rowCell.toString();

    MatcherAssert.assertThat(result, containsString("key"));
    MatcherAssert.assertThat(result, containsString("family"));
    MatcherAssert.assertThat(result, containsString("qualifier"));
    MatcherAssert.assertThat(result, containsString("1487963474314"));
  }
}
