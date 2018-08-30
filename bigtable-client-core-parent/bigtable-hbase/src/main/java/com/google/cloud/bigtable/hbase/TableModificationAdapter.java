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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class TableModificationAdapter {

  private final static ColumnDescriptorAdapter columnDescriptorAdapter =
      new ColumnDescriptorAdapter();

  private static Set<String> getColumnNames(HTableDescriptor tableDescriptor) {
    Set<String> names = new HashSet<>();
    for (byte[] name : tableDescriptor.getFamiliesKeys()) {
      names.add(Bytes.toString(name));
    }
    return names;
  }

  private static Modification updateModification(HColumnDescriptor colFamilyDesc) {
    return Modification
        .newBuilder()
        .setId(colFamilyDesc.getNameAsString())
        .setUpdate(columnDescriptorAdapter.adapt(colFamilyDesc).build())
        .build();
  }

  private static Modification creationModification(HColumnDescriptor colFamilyDesc) {
    return Modification
        .newBuilder()
        .setId(colFamilyDesc.getNameAsString())
        .setCreate(columnDescriptorAdapter.adapt(colFamilyDesc).build())
        .build();
  }

  private static Modification deleteModification(String columnName) {
    return Modification
        .newBuilder()
        .setId(columnName)
        .setDrop(true)
        .build();
  }

  /**
   * This method will build list of of protobuf {@link Modification} objects based on a diff of the
   * new and existing set of column descriptors.  This is for use in
   * {@link org.apache.hadoop.hbase.client.Admin#modifyTable(TableName, HTableDescriptor)}.
   */
  public Modification[] buildModifications(HTableDescriptor newTableDescriptor,
      HTableDescriptor currentTableDescriptors) {
    Set<String> currentColumnNames = getColumnNames(currentTableDescriptors);
    Set<String> newColumnNames = getColumnNames(newTableDescriptor);

    final List<Modification> modifications = new ArrayList<>();
    for (HColumnDescriptor hColumnDescriptor : newTableDescriptor.getFamilies()) {
      if (currentColumnNames.contains(hColumnDescriptor.getNameAsString())) {
        modifications.add(updateModification(hColumnDescriptor));
      } else {
        modifications.add(creationModification(hColumnDescriptor));
      }
    }

    Set<String> columnsToRemove = new HashSet<>(currentColumnNames);
    columnsToRemove.removeAll(newColumnNames);

    for (String column : columnsToRemove) {
      modifications.add(deleteModification(column));
    }
    return modifications.toArray(new Modification[modifications.size()]);
  }
}
