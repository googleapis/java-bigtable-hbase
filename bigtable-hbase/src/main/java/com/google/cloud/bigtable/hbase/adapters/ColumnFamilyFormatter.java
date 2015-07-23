/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import org.apache.hadoop.hbase.TableName;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.naming.BigtableTableName;
import com.google.common.base.Preconditions;

public class ColumnFamilyFormatter {

  final static String COLUMN_FAMILIES_SEPARARATOR = "columnFamilies";

  public static ColumnFamilyFormatter from(TableName tableName, BigtableOptions options) {
    return new ColumnFamilyFormatter(options.getClusterName().toTableName(
      tableName.getNameAsString()));
  }

  private final BigtableTableName bigtableTableName;

  public ColumnFamilyFormatter(BigtableTableName bigtableTableName) {
    this.bigtableTableName = bigtableTableName;
  }

  public String formatForBigtable(String hbaseColumnFamily) {
    return String.format("%s/%s/%s", bigtableTableName.getTableName(),
      COLUMN_FAMILIES_SEPARARATOR, hbaseColumnFamily);
  }

  public String formatForHBase(String bigtableColumnFamily) {
    String tableName = bigtableTableName.getTableName();
    String familyPrefix = String.format("%s/%s", tableName, COLUMN_FAMILIES_SEPARARATOR);
    Preconditions.checkState(bigtableColumnFamily.startsWith(familyPrefix),
      "Column family '%s' needs to start with '%s'", bigtableColumnFamily, tableName);
    return bigtableColumnFamily.substring(familyPrefix.length());
  }
}
