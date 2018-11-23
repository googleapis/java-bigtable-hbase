/*
 * Copyright 2018 Google LLC. All Rights Reserved.
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

import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;
import java.util.Set;

import static com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter.buildGarbageCollectionRule;
/**
 * Utilitiy to create {@link ModifyColumnFamiliesRequest} from HBase {@link HColumnDescriptor}s.
 */
public class ModifyTableBuilder {

  private static Set<String> getColumnNames(HTableDescriptor tableDescriptor) {
    Set<String> names = new HashSet<>();
    for (byte[] name : tableDescriptor.getFamiliesKeys()) {
      names.add(Bytes.toString(name));
    }
    return names;
  }

  /**
   * This method will build {@link ModifyColumnFamiliesRequest} objects based on a diff of the
   * new and existing set of column descriptors.  This is for use in
   * {@link org.apache.hadoop.hbase.client.Admin#modifyTable(TableName, HTableDescriptor)}.
   */
  public static ModifyColumnFamiliesRequest buildModifications(
          HTableDescriptor newTableDescriptor,
          HTableDescriptor currentTableDescriptors,
          ModifyColumnFamiliesRequest request) {
    Set<String> currentColumnNames = getColumnNames(currentTableDescriptors);
    Set<String> newColumnNames = getColumnNames(newTableDescriptor);

    for (HColumnDescriptor hColumnDescriptor : newTableDescriptor.getFamilies()) {
      String columnName = hColumnDescriptor.getNameAsString();
      if (currentColumnNames.contains(columnName)) {
        request.updateFamily(columnName,
                buildGarbageCollectionRule(hColumnDescriptor));
      } else {
        request.addFamily(columnName,
                buildGarbageCollectionRule(hColumnDescriptor));
      }
    }

    Set<String> columnsToRemove = new HashSet<>(currentColumnNames);
    columnsToRemove.removeAll(newColumnNames);

    for (String column : columnsToRemove) {
      request.dropFamily(column);
    }
    return request;
  }
}
