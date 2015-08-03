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
package com.google.cloud.bigtable.hbase.adapters;


import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestUnsupportedOperationAdapter {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testUnsupportedOperationExceptionIsThrownOnUse() {
    UnsupportedOperationAdapter<Append> unsupportedOperationAdapter =
        new UnsupportedOperationAdapter<Append>("append");

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("operation is unsupported");
    expectedException.expectMessage("append");

    unsupportedOperationAdapter.adapt(new Append(Bytes.toBytes("rk1")));
  }
}
