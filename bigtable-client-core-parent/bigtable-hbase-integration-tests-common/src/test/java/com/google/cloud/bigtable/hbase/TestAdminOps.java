/*
 * Copyright 2019 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAdminOps extends AbstractTest {

  @Test
  public void testIsTableEnabledOrDisabled() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      Exception actualError = null;
      try {
        admin.isTableEnabled(null);
      } catch (Exception e) {
        actualError = e;
      }
      assertNotNull(actualError);
      actualError = null;

      try {
        admin.isTableDisabled(null);
      } catch (Exception e) {
        actualError = e;
      }
      assertNotNull(actualError);
    }
  }

  @Test
  public void testDisableTablesWithNullOrEmpty() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      Exception actualError = null;
      try {
        admin.disableTables((String) null);
      } catch (Exception e) {
        actualError = e;
      }
      assertNotNull(actualError);
      assertTrue(actualError instanceof NullPointerException);

      assertEquals(0, admin.disableTables("").length);
    }
  }

  @Test
  public void testEnableTablesWithNull() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      Exception actualError = null;
      try {
        admin.enableTables((String) null);
      } catch (Exception e) {
        actualError = e;
      }
      assertNotNull(actualError);
      assertTrue(actualError instanceof NullPointerException);

      assertEquals(0, admin.enableTables("").length);
    }
  }

  @Test
  public void testGetTableDescriptorsByTableNameWithNullAndEmptyList() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      assertTrue(admin.getTableDescriptorsByTableName(null).length > 0);

      assertTrue(admin.getTableDescriptorsByTableName(Collections.emptyList()).length > 0);
    }
  }

  @Test
  public void testGetTableDescriptorsWithNullAndEmptyList() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      Exception actualError = null;
      try {
        admin.getTableDescriptors(null);
      } catch (Exception e) {
        actualError = e;
      }
      assertNotNull(actualError);
      assertTrue(actualError instanceof NullPointerException);

      assertTrue(admin.getTableDescriptors(Collections.emptyList()).length > 0);
    }
  }
}
