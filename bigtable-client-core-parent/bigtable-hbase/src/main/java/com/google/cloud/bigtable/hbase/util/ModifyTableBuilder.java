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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.common.base.Preconditions;
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
@InternalApi
public class ModifyTableBuilder {


  private ModifyTableBuilder(String tableId){
    this.request = ModifyColumnFamiliesRequest.of(tableId);
  }

  public static ModifyTableBuilder newBuilder(TableName tableName){
    return new ModifyTableBuilder(tableName.getNameAsString());
  }

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
  public static ModifyTableBuilder buildModifications(
          HTableDescriptor newTableDesc,
          HTableDescriptor currentTableDesc) {
    Preconditions.checkNotNull(newTableDesc);
    Preconditions.checkNotNull(currentTableDesc);

    ModifyTableBuilder requestBuilder = ModifyTableBuilder.newBuilder(currentTableDesc.getTableName());
    Set<String> currentColumnNames = getColumnNames(currentTableDesc);
    Set<String> newColumnNames = getColumnNames(newTableDesc);

    for (HColumnDescriptor hColumnDescriptor : newTableDesc.getFamilies()) {
      String columnName = hColumnDescriptor.getNameAsString();
      if (currentColumnNames.contains(columnName)) {
        requestBuilder.modify(hColumnDescriptor);
      } else {
        requestBuilder.add(hColumnDescriptor);
      }
    }

    Set<String> columnsToRemove = new HashSet<>(currentColumnNames);
    columnsToRemove.removeAll(newColumnNames);

    for (String column : columnsToRemove) {
      requestBuilder.delete(column);
    }
    return requestBuilder;
  }

  private final ModifyColumnFamiliesRequest request;

  public ModifyTableBuilder add(HColumnDescriptor addColumnFamily) {
    this.request.addFamily(addColumnFamily.getNameAsString(),
            buildGarbageCollectionRule(addColumnFamily));
    return this;
  }

  public ModifyTableBuilder modify(HColumnDescriptor modifyColumnFamily) {
    this.request.updateFamily(modifyColumnFamily.getNameAsString(),
            buildGarbageCollectionRule(modifyColumnFamily));
    return this;
  }

  public ModifyTableBuilder delete(String familyId) {
    this.request.dropFamily(familyId);
    return this;
  }

  public ModifyColumnFamiliesRequest build(){
    return request;
  }
}
