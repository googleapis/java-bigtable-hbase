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
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;

import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification;

public abstract class AbstractBigtableModifyTable {

  /**
   * This method will build list of either create or modify Modifications objects..
   * 
   * @param tableName
   * @param tableDescriptor
   * @param modifications an array of {@link com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification} object.
   * @throws IOException
   */
  protected List<Modification> buildModificationsForModifyFamilies(TableName tableName,
      List<HColumnDescriptor> modifyColumnDescriptors, List<HColumnDescriptor> currentColumnDescriptors)
      throws IOException {
    final List<Modification> modifications = new ArrayList<>();
    List<String> currentColumnNames = getAllColumnDescritpors(currentColumnDescriptors);

    for (HColumnDescriptor hColumnDescriptor : modifyColumnDescriptors) {
      if (currentColumnNames.contains(hColumnDescriptor.getNameAsString())) {
        Modification modification = getModificationUpdate(hColumnDescriptor);
        modifications.add(modification);
      } else {
        Modification modification = getModificationCreate(hColumnDescriptor);
        modifications.add(modification);
      }
    }
    return modifications;
  }

  private List<String> getAllColumnDescritpors(List<HColumnDescriptor> columnDescriptors) {
    List<String> names = new ArrayList<>();
    for (HColumnDescriptor hColumnDescriptor : columnDescriptors) {
      names.add(hColumnDescriptor.getNameAsString());
    }
    return names;
  }
  
  /**
   * This method will build list of delete Modifications objects.
   * 
   * @param tableName
   * @param tableDescriptor
   * @param modifications an array of {@link com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification} object.
   * @throws IOException
   */
  protected List<Modification> buildModifcationsForDeleteFamilies(TableName tableName,
      List<HColumnDescriptor> tableDescriptor, List<HColumnDescriptor> currentColumnDescriptors) throws IOException {
    final List<Modification> modifications = new ArrayList<>();
    List<String> requestedColumnNames = getAllColumnDescritpors(tableDescriptor);

    for (HColumnDescriptor hColumnDescriptor : currentColumnDescriptors) {
      if (!requestedColumnNames.contains(hColumnDescriptor.getNameAsString())) {
        Modification modification = getModificationDelete(hColumnDescriptor);
        modifications.add(modification);
      }
    }
    return modifications;
  }

  protected Modification getModificationDelete(HColumnDescriptor colFamilyDesc) throws IOException {
    return Modification
        .newBuilder()
        .setId(colFamilyDesc.getNameAsString())
        .setDrop(true)
        .build();
  }
  
  protected abstract Modification getModificationUpdate(HColumnDescriptor colFamilyDesc) throws IOException;

  protected abstract Modification getModificationCreate(HColumnDescriptor colFamilyDesc) throws IOException;

}
