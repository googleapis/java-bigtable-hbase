/*
 * Copyright 2020 Google LLC.
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

import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBigtableHBaseVersion {

  @Test
  public void testVersionNumber() {
    // This patterns matches string with "1.14+" versions, The version must have 3 parts i.e. x.y.z
    Pattern versionPattern =
        Pattern.compile("^(([1]\\.([1][4-9]|[2-9]\\d))|([2-9]\\.\\d{1,2}))\\.");
    assertTrue(versionPattern.matcher(BigtableHBaseVersion.CLIENT_VERSION).find());
  }
}
