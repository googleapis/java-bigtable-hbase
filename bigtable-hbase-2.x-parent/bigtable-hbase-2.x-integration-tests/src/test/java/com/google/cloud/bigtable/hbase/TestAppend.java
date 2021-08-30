/*
 * Copyright 2015 Google LLC
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

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;

public class TestAppend extends AbstractTestAppend {

  public void getGetAddColumnVersion(Get get, int version, byte[] rowKey, byte[] qualifier)
      throws IOException {
    get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    get.readVersions(version);
  }

  public void getGetAddFamilyVersion(Get get, int version, byte[] columnFamily) throws IOException {
    get.addFamily(columnFamily);
    get.readVersions(version);
  }

  public void appendAdd(Append append, byte[] columnFamily, byte[] qualifier, byte[] value) {
    append.addColumn(columnFamily, qualifier, value);
  }
}
