/*
 * Copyright (c) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestCreateTable extends AbstractTest {
  /**
   * Requirement 1.8 - Table names must match [\w_][\w_\-\.]*
   */
  @Test
  public void testTableNames() throws IOException {
    String[] goodnames = {
        "a",
        "1",
        "_", // Really?  Yuck.
        "_x",
        "a-._5x",
        "_a-._5x",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890_-."
    };
    String[] badnames = {
        "-x",
        ".x",
        "a!",
        "a@",
        "a#",
        "a$",
        "a%",
        "a^",
        "a&",
        "a*",
        "a(",
        "a+",
        "a=",
        "a~",
        "a`",
        "a{",
        "a[",
        "a|",
        "a\\",
        "a/",
        "a<",
        "a,",
        "a?",
        "a" + RandomStringUtils.random(10, false, false)
    };

    for (String badname : badnames) {
      boolean failed = false;
      try {
        TEST_UTIL.createTable(Bytes.toBytes(badname), COLUMN_FAMILY);
      } catch (IllegalArgumentException e) {
        failed = true;
      }
      Assert.assertTrue("Should fail as table name: '" + badname + "'", failed);
    }

    for(String goodname : goodnames) {
      TEST_UTIL.createTable(Bytes.toBytes(goodname), COLUMN_FAMILY);
      TEST_UTIL.deleteTable(Bytes.toBytes(goodname));
    }
  }
}
