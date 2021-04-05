/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.hbase.tools;

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;

// TODO Maybe use AutoValue here?

/** A simple POJO representing the HBase schema for a whole cluster. */
@InternalApi
public class ClusterSchemaDefinition {

  /** POJO containing all the information about a table schema including the splits. */
  public static class TableSchemaDefinition {
    // The name of the table
    public String name;
    // Serialized HTableDescriptor object.
    public byte[] tableDescriptor;
    // Start key for all the regions in the table.
    public byte[][] splits;

    public TableSchemaDefinition() {}

    public TableSchemaDefinition(HTableDescriptor tableDescriptor, byte[][] splits) {
      this.name = tableDescriptor.getNameAsString();
      this.tableDescriptor = tableDescriptor.toByteArray();
      this.splits = splits;
    }

    public HTableDescriptor getHbaseTableDescriptor() throws DeserializationException, IOException {
      return HTableDescriptor.parseFrom(tableDescriptor);
    }
  }

  /** Schema definitions for the tables in the cluster. */
  public List<TableSchemaDefinition> tableSchemaDefinitions;

  public ClusterSchemaDefinition() {
    tableSchemaDefinitions = new ArrayList<>();
  }

  public void addTableSchemaDefinition(HTableDescriptor tableDescriptor, byte[][] splits) {
    tableSchemaDefinitions.add(new TableSchemaDefinition(tableDescriptor, splits));
  }
}
