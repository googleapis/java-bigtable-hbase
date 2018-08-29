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

public class TableModificationAdapter {

  private final ColumnDescriptorAdapter columnDescriptorAdapter = new ColumnDescriptorAdapter();
  
  /**
   * This method will build list of either create or modify Modifications objects..
   * 
   * @param tableDescriptor
   * @param modifications an array of {@link com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification} object.
   * @throws IOException
   */
  public List<Modification> buildModifications(List<HColumnDescriptor> modifyColumnDescriptors, List<HColumnDescriptor> currentColumnDescriptors)
      throws IOException {
    final List<Modification> modifications = new ArrayList<>();
    Set<String> currentColumnNames = getAllColumnDescriptors(currentColumnDescriptors);

    for (HColumnDescriptor hColumnDescriptor : modifyColumnDescriptors) {
      if (currentColumnNames.contains(hColumnDescriptor.getNameAsString())) {
        Modification modification = getModificationUpdate(hColumnDescriptor);
        modifications.add(modification);
      } else {
        Modification modification = getModificationCreate(hColumnDescriptor);
        modifications.add(modification);
      }
    }
    modifications.addAll(buildModifcationsForDeleteFamilies(modifyColumnDescriptors,currentColumnDescriptors));
    return modifications;
  }

  private Set<String> getAllColumnDescriptors(List<HColumnDescriptor> columnDescriptors) {
    Set<String> names = new HashSet<>();
    for (HColumnDescriptor hColumnDescriptor : columnDescriptors) {
      names.add(hColumnDescriptor.getNameAsString());
    }
    return names;
  }
  
  /**
   * This method will build list of delete Modifications objects.
   * 
   * @param tableDescriptor
   * @param modifications an array of {@link com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification} object.
   * @throws IOException
   */
  private List<Modification> buildModifcationsForDeleteFamilies(List<HColumnDescriptor> tableDescriptor, List<HColumnDescriptor> currentColumnDescriptors) throws IOException {
    final List<Modification> modifications = new ArrayList<>();
    Set<String> requestedColumnNames = getAllColumnDescriptors(tableDescriptor);

    for (HColumnDescriptor hColumnDescriptor : currentColumnDescriptors) {
      if (!requestedColumnNames.contains(hColumnDescriptor.getNameAsString())) {
        Modification modification = getModificationDelete(hColumnDescriptor);
        modifications.add(modification);
      }
    }
    return modifications;
  }

  private Modification getModificationUpdate(HColumnDescriptor colFamilyDesc) {
    return Modification
        .newBuilder()
        .setId(colFamilyDesc.getNameAsString())
        .setUpdate(columnDescriptorAdapter.adapt(colFamilyDesc).build())
        .build();
  }

  private Modification getModificationCreate(HColumnDescriptor colFamilyDesc) {
    return Modification
        .newBuilder()
        .setId(colFamilyDesc.getNameAsString())
        .setCreate(columnDescriptorAdapter.adapt(colFamilyDesc).build())
        .build();
  }
  
  private Modification getModificationDelete(HColumnDescriptor colFamilyDesc) throws IOException {
    return Modification
        .newBuilder()
        .setId(colFamilyDesc.getNameAsString())
        .setDrop(true)
        .build();
  }
}
