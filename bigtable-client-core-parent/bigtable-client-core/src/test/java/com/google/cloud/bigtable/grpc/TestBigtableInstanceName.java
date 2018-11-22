/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import com.google.cloud.bigtable.data.v2.models.InstanceName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;

@RunWith(JUnit4.class)
public class TestBigtableInstanceName {

  public static BigtableInstanceName bigtableInstanceName =
      new BigtableInstanceName("some-project", "some-instance");
  private String instanceName = "projects/some-project/instances/some-instance";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testFormat() {
    Assert.assertEquals(instanceName, bigtableInstanceName.toString());
  }

  @Test
  public void testListTablesRequest() {
    ListTablesRequest.Builder builder = ListTablesRequest.newBuilder();
    builder.setParent(bigtableInstanceName.toString());
    Assert.assertEquals(instanceName, builder.getParent());
  }

  @Test
  public void testCreateTablesRequest() {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setParent(bigtableInstanceName.toString());
    Assert.assertEquals(instanceName, builder.getParent());
  }

  @Test
  public void testGoodTableQualifier() {
    bigtableInstanceName
        .toTableId(instanceName + BigtableInstanceName.TABLE_SEPARATOR + "foo");
  }

  @Test
  public void testNullQualifier() {
    expectedException.expect(NullPointerException.class);
    bigtableInstanceName.toTableId(null);
  }

  @Test
  public void testBadQualifier() {
    expectedException.expect(IllegalStateException.class);
    bigtableInstanceName.toTableId(instanceName.replace("some-instance", "another-instance")
        + BigtableInstanceName.TABLE_SEPARATOR + "foo");
  }

  @Test
  public void testBlankTableName() {
    expectedException.expect(IllegalStateException.class);
    bigtableInstanceName.toTableId(instanceName + BigtableInstanceName.TABLE_SEPARATOR);
  }

  @Test
  public void testNoTableName() {
    expectedException.expect(IllegalStateException.class);
    bigtableInstanceName.toTableId(instanceName + BigtableInstanceName.TABLE_SEPARATOR);
  }

  @Test
  public void testNoProjectName() {
    expectedException.expect(IllegalArgumentException.class);
    new BigtableInstanceName("", "instance");
  }

  @Test
  public void testNoInstanceName() {
    expectedException.expect(IllegalArgumentException.class);
    new BigtableInstanceName("project", "");
  }

  @Test
  public void testGcbInstanceName(){
    Assert.assertTrue(bigtableInstanceName.toGcbInstanceName() instanceof InstanceName);
  }

  @Test
  public void testAdminInstanceName(){
    Assert.assertTrue(bigtableInstanceName.toAdminInstanceName() instanceof
            com.google.bigtable.admin.v2.InstanceName);
  }
}
