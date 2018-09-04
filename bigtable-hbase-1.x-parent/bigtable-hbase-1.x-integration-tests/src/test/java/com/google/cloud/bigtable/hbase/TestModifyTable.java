/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import java.io.IOException;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;

@RunWith(JUnit4.class)
public class TestModifyTable extends AbstractTestModifyTable {

  @Test
  public void testModifyTable() throws IOException {
    super.testModifyTable(new Function<HTableDescriptor, Void>() {
      @Nullable @Override
      public Void apply(@Nullable HTableDescriptor descriptor) {
        try (Admin admin = getConnection().getAdmin()) {
          admin.modifyTable(descriptor.getTableName(), descriptor);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    });
  }
}
