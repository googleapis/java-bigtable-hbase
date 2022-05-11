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
package com.google.cloud.bigtable.hbase.mirroring.utils.compat;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Integration tests in bigtable-hbase-mirroring-client-1.x-integration-tests project are used to
 * test both 1.x MirroringConnection and 2.x MirroringConnection. This interface provides a
 * compatibility layer between 1.x and 2.x for creating a table and is used only in ITs.
 */
public interface TableCreator {
  void createTable(Connection connection, String tableName, byte[]... columnFamilies)
      throws IOException;
}
