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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class AbstractTest {

  public SharedTestEnvRule sharedTestEnv = SharedTestEnvRule.getInstance();

  protected static DataGenerationHelper dataHelper = new DataGenerationHelper();
  protected Logger logger = new Logger(this.getClass());

  // This is for when we need to look at the results outside of the current connection
  protected Connection createNewConnection() throws IOException {
    return sharedTestEnv.createConnection();
  }

  protected Connection getConnection() {
    return sharedTestEnv.getConnection();
  }

  protected Table getDefaultTable() throws IOException {
    return sharedTestEnv.getDefaultTable();
  }

  protected static class QualifierValue implements Comparable<QualifierValue> {

    protected final byte[] qualifier;
    protected final byte[] value;

    public QualifierValue(byte[] qualifier, byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }

    @Override
    public int compareTo(QualifierValue qualifierValue) {
      return Bytes.compareTo(this.qualifier, qualifierValue.qualifier);
    }
  }
}
