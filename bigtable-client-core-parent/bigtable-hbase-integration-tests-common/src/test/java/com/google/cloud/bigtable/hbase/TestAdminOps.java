/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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
        HTableDescriptor[] descriptors = admin.disableTables((String) null);
        assertTrue(descriptors.length > 0);
      } catch (Exception e) {
        actualError = e;
        e.printStackTrace(System.out);
      }
      assertNotNull(actualError);
      actualError = null;

      try {
        assertEquals(0, admin.disableTables("").length);
      } catch (Exception e) {
        actualError = e;
        e.printStackTrace(System.out);
      }
      assertNull(actualError);
    }
  }

  @Test
  public void testEnableTablesWithNull() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      Exception actualError = null;
      try {
        HTableDescriptor[] descriptors = admin.enableTables((String) null);
        assertTrue(descriptors.length > 0);
      } catch (Exception e) {
        e.printStackTrace(System.out);
        actualError = e;
      }
      assertNotNull(actualError);
      actualError = null;

      try {
        assertEquals(0, admin.enableTables("").length);
      } catch (Exception e) {
        e.printStackTrace(System.out);
        actualError = e;
      }
      assertNull(actualError);
    }
  }

  @Test
  public void testGetTableDescriptorsByTableNameWithNullAndEmptyList() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      Exception actualError = null;
      try {
        HTableDescriptor[] descriptor = admin.getTableDescriptorsByTableName(null);
        assertTrue(descriptor.length > 0);
      } catch (Exception e) {
        actualError = e;
      }
      assertNull(actualError);

      try {
        HTableDescriptor[] descriptor =
            admin.getTableDescriptorsByTableName(ImmutableList.<TableName>of());
        assertTrue(descriptor.length > 0);
      } catch (Exception e) {
        e.printStackTrace(System.out);
        actualError = e;
      }
      assertNull(actualError);
    }
  }

  @Test
  public void testGetTableDescriptorsWithNullAndEmptyList() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      Exception actualError = null;
      try {
        HTableDescriptor[] descriptors = admin.getTableDescriptors(null);
        assertTrue(descriptors.length > 0);
      } catch (Exception e) {
        actualError = e;
      }
      assertNotNull(actualError);
      actualError = null;

      try {
        HTableDescriptor[] descriptors = admin.getTableDescriptors(ImmutableList.<String>of());
        assertTrue(descriptors.length > 0);
      } catch (Exception e) {
        e.printStackTrace(System.out);
        actualError = e;
      }
      assertNull(actualError);
    }
  }
}
