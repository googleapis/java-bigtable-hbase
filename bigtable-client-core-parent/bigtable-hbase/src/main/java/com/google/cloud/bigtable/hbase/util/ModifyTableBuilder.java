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

import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utilitiy to create {@link ModifyColumnFamiliesRequest} from HBase {@link HColumnDescriptor}s.
 */
public class ModifyTableBuilder {

  private static final ColumnDescriptorAdapter ADAPTER = new ColumnDescriptorAdapter();

  public static ModifyTableBuilder create() {
    return new ModifyTableBuilder();
  }

  private static Set<String> getColumnNames(HTableDescriptor tableDescriptor) {
    Set<String> names = new HashSet<>();
    for (byte[] name : tableDescriptor.getFamiliesKeys()) {
      names.add(Bytes.toString(name));
    }
    return names;
  }

  /**
   * This method will build list of of protobuf {@link ModifyColumnFamiliesRequest.Modification} objects based on a diff of the
   * new and existing set of column descriptors.  This is for use in
   * {@link org.apache.hadoop.hbase.client.Admin#modifyTable(TableName, HTableDescriptor)}.
   */
  public static ModifyTableBuilder buildModifications(
      HTableDescriptor newTableDescriptor,
      HTableDescriptor currentTableDescriptors) {
    ModifyTableBuilder builder = new ModifyTableBuilder();
    Set<String> currentColumnNames = getColumnNames(currentTableDescriptors);
    Set<String> newColumnNames = getColumnNames(newTableDescriptor);

    for (HColumnDescriptor hColumnDescriptor : newTableDescriptor.getFamilies()) {
      if (currentColumnNames.contains(hColumnDescriptor.getNameAsString())) {
        builder.modify(hColumnDescriptor);
      } else {
        builder.add(hColumnDescriptor);
      }
    }

    Set<String> columnsToRemove = new HashSet<>(currentColumnNames);
    columnsToRemove.removeAll(newColumnNames);

    for (String column : columnsToRemove) {
      builder.delete(column);
    }
    return builder;
  }

  private final List<ModifyColumnFamiliesRequest.Modification> modifications = new ArrayList<>();

  public ModifyTableBuilder add(HColumnDescriptor descriptor) {
    String columnName = descriptor.getNameAsString();
    modifications.add(ModifyColumnFamiliesRequest.Modification
        .newBuilder()
        .setId(columnName)
        .setCreate(ADAPTER.adapt(descriptor))
        .build());
    return this;
  }

  public ModifyTableBuilder modify(HColumnDescriptor descriptor) {
    String columnName = descriptor.getNameAsString();
    modifications.add(ModifyColumnFamiliesRequest.Modification
        .newBuilder()
        .setId(columnName)
        .setUpdate(ADAPTER.adapt(descriptor))
        .build());
    return this;
  }

  public ModifyTableBuilder delete(String name) {
    modifications.add(ModifyColumnFamiliesRequest.Modification
        .newBuilder()
        .setId(name)
        .setDrop(true)
        .build());
    return this;
  }

  public ModifyColumnFamiliesRequest toProto(String name) {
    return ModifyColumnFamiliesRequest
        .newBuilder()
        .setName(name)
        .addAllModifications(modifications)
        .build();
  }
}
