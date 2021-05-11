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

import com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator.SchemaTranslationOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HBaseSchemaTranslationOptionsTest {

  @Before
  public void before() {
    // Clear all the system properties so that each tests don't interfere with each other.
    System.clearProperty(HBaseSchemaTranslator.INSTANCE_ID_KEY);
    System.clearProperty(HBaseSchemaTranslator.PROJECT_ID_KEY);
    System.clearProperty(HBaseSchemaTranslator.ZOOKEEPER_PORT_KEY);
    System.clearProperty(HBaseSchemaTranslator.ZOOKEEPER_QUORUM_KEY);
    System.clearProperty(HBaseSchemaTranslator.INPUT_FILE_KEY);
    System.clearProperty(HBaseSchemaTranslator.OUTPUT_FILE_KEY);
    System.clearProperty(HBaseSchemaTranslator.TABLE_NAME_FILTER_KEY);
  }

  ////////////////////////// Invalid Schema Source ///////////////////////////////////
  @Test(expected = IllegalArgumentException.class)
  public void validateSchemaTranlationOptionsNoInput() {
    SchemaTranslationOptions options = new SchemaTranslationOptions();
    options.outputFilePath = "/tmp/output";
    options.validateOptions();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateSchemaTranlationOptionsInputFileAndQuorum() {
    SchemaTranslationOptions options = new SchemaTranslationOptions();
    options.outputFilePath = "/tmp/output";
    options.inputFilePath = "/tmp/input";
    options.zookeeperQuorum = "localhost";
    options.validateOptions();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateSchemaTranlationOptionsInputFileAndZookeeperPort() {
    SchemaTranslationOptions options = new SchemaTranslationOptions();
    options.outputFilePath = "/tmp/output";
    options.inputFilePath = "/tmp/input";
    options.zookeeperPort = 1080;
    options.validateOptions();
  }

  ////////////////////////// Invalid Schema Destination ///////////////////////////////////
  @Test(expected = IllegalArgumentException.class)
  public void validateSchemaTranlationOptionsNoOutput() {
    SchemaTranslationOptions options = new SchemaTranslationOptions();
    options.inputFilePath = "/tmp/input";
    options.validateOptions();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateSchemaTranlationOptionsOutputFileAndProjectId() {
    SchemaTranslationOptions options = new SchemaTranslationOptions();
    options.inputFilePath = "/tmp/input";
    options.outputFilePath = "/tmp/output";
    options.projectId = "project";
    options.validateOptions();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateSchemaTranlationOptionsOutputFileAndInstanceId() {
    SchemaTranslationOptions options = new SchemaTranslationOptions();
    options.inputFilePath = "/tmp/input";
    options.outputFilePath = "/tmp/output";
    options.instanceId = "my-instance";
    options.validateOptions();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateSchemaTranlationOptionsInputFileAndTableFilter() {
    SchemaTranslationOptions options = new SchemaTranslationOptions();
    options.inputFilePath = "/tmp/input";
    options.outputFilePath = "/tmp/output";
    options.tableNameFilter = ".*";
    options.validateOptions();
  }

  //////////////////////////////////// HAPPY CASES ////////////////////////////

  @Test
  public void parseSchemaTranslationOptionsHbaseToFile() {
    System.setProperty(HBaseSchemaTranslator.ZOOKEEPER_QUORUM_KEY, "localhost");
    System.setProperty(HBaseSchemaTranslator.ZOOKEEPER_PORT_KEY, "1080");
    System.setProperty(HBaseSchemaTranslator.TABLE_NAME_FILTER_KEY, "hbase-.*");
    System.setProperty(HBaseSchemaTranslator.OUTPUT_FILE_KEY, "/tmp/output");

    SchemaTranslationOptions options = SchemaTranslationOptions.loadOptionsFromSystemProperties();
    Assert.assertEquals("localhost", options.zookeeperQuorum);
    Assert.assertEquals((Integer) 1080, options.zookeeperPort);
    Assert.assertEquals("/tmp/output", options.outputFilePath);
    Assert.assertEquals("hbase-.*", options.tableNameFilter);
  }

  @Test
  public void parseSchemaTranslationOptionsFileToBigtable() {
    System.setProperty(HBaseSchemaTranslator.INPUT_FILE_KEY, "/tmp/input");
    System.setProperty(HBaseSchemaTranslator.PROJECT_ID_KEY, "my-project");
    System.setProperty(HBaseSchemaTranslator.INSTANCE_ID_KEY, "my-instance");

    SchemaTranslationOptions options = SchemaTranslationOptions.loadOptionsFromSystemProperties();
    Assert.assertEquals("/tmp/input", options.inputFilePath);
    Assert.assertEquals("my-project", options.projectId);
    Assert.assertEquals("my-instance", options.instanceId);
  }

  @Test
  public void parseSchemaTranslationOptionsHBaseToBigtable() {
    System.setProperty(HBaseSchemaTranslator.ZOOKEEPER_QUORUM_KEY, "localhost");
    System.setProperty(HBaseSchemaTranslator.ZOOKEEPER_PORT_KEY, "1080");
    System.setProperty(HBaseSchemaTranslator.PROJECT_ID_KEY, "my-project");
    System.setProperty(HBaseSchemaTranslator.INSTANCE_ID_KEY, "my-instance");

    SchemaTranslationOptions options = SchemaTranslationOptions.loadOptionsFromSystemProperties();
    Assert.assertEquals("localhost", options.zookeeperQuorum);
    Assert.assertEquals((Integer) 1080, options.zookeeperPort);
    Assert.assertEquals("my-project", options.projectId);
    Assert.assertEquals("my-instance", options.instanceId);
  }
}
