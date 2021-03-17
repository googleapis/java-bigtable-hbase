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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;

// TODO Maybe use AutoValue here?
public class ClusterSchemaDefinition {
  public static class TableSchemaDefinition {
    public String name;
    public byte[] tableDescriptor;
    public byte[][] splits;

    public TableSchemaDefinition() {}

    public TableSchemaDefinition(HTableDescriptor tableDescriptor, byte[][] splits) {
      this.name = tableDescriptor.getNameAsString();
      this.tableDescriptor = tableDescriptor.toByteArray();
      this.splits = splits;
    }

    @JsonIgnore
    public HTableDescriptor getHbaseTableDescriptor() throws DeserializationException, IOException {
      return HTableDescriptor.parseFrom(tableDescriptor);
    }
  }

  public ClusterSchemaDefinition() {
    tableSchemaDefinitions = new ArrayList<>();
  }

  public List<TableSchemaDefinition> tableSchemaDefinitions;
}
