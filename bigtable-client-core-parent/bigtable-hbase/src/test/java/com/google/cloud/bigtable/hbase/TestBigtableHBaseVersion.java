/*
 * Copyright 2020 Google LLC
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

import com.google.common.collect.ComparisonChain;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBigtableHBaseVersion {

  @Test
  public void testVersionNumber() {
    // Version must have 3 parts i.e. x.y.z and can have an optional of -SNAPSHOT suffixed.
    Pattern versionPattern = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)([-\\w]+)?");
    Matcher versionMatcher = versionPattern.matcher(BigtableHBaseVersion.getVersion());

    assertTrue("incorrect version format", versionMatcher.matches());

    int result =
        ComparisonChain.start()
            .compare(2, Integer.parseInt(versionMatcher.group(1)))
            .compare(0, Integer.parseInt(versionMatcher.group(2)))
            .compare(0, Integer.parseInt(versionMatcher.group(3)))
            .result();

    assertTrue(
        "Expected BigtableHBaseVersion.getVersion() to be at least 2.0.0-alpha-1", result <= 0);
  }
}
