/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.hbase.replication.configuration;

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.TABLE_NAME_MAPPING_FILE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.TABLE_NAME_MAPPING_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.TABLE_NAME_MAPPING_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TableNameMapperTest {

  private static final String MAPPING_JSON =
      "{\"smartbrain:oraperi_new\": \"smartbrain_oraperi_new\", \"plain\": \"renamed-plain\"}";

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private File writeFile(String name, byte[] contents) throws IOException {
    File file = tempFolder.newFile(name);
    try (OutputStream out = new FileOutputStream(file)) {
      out.write(contents);
    }
    return file;
  }

  @Test
  public void identityWhenNothingConfigured() throws IOException {
    TableNameMapper mapper = TableNameMapper.fromConfiguration(new Configuration(false));
    assertTrue(mapper.isIdentity());
    assertEquals("ns:table", mapper.map("ns:table"));
    assertEquals("table", mapper.map("table"));
  }

  @Test
  public void identityWhenConfigurationIsNull() throws IOException {
    assertTrue(TableNameMapper.fromConfiguration(null).isIdentity());
  }

  @Test
  public void identityWhenKeysAreBlank() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_KEY, "   ");
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, "");
    assertTrue(TableNameMapper.fromConfiguration(conf).isIdentity());
  }

  @Test
  public void loadsInlineJson() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_KEY, MAPPING_JSON);

    TableNameMapper mapper = TableNameMapper.fromConfiguration(conf);

    assertFalse(mapper.isIdentity());
    assertEquals("smartbrain_oraperi_new", mapper.map("smartbrain:oraperi_new"));
    assertEquals("renamed-plain", mapper.map("plain"));
    // Unmapped tables keep their HBase name.
    assertEquals("other:table", mapper.map("other:table"));
    assertEquals(2, mapper.getMappings().size());
  }

  @Test
  public void loadsJsonFromLocalFile() throws IOException {
    File file = writeFile("mapping.json", MAPPING_JSON.getBytes(StandardCharsets.UTF_8));
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, file.toURI().toString());

    TableNameMapper mapper = TableNameMapper.fromConfiguration(conf);

    assertEquals("smartbrain_oraperi_new", mapper.map("smartbrain:oraperi_new"));
    assertEquals("renamed-plain", mapper.map("plain"));
  }

  @Test
  public void loadsJsonFromLocalFileWithoutScheme() throws IOException {
    File file = writeFile("mapping.json", MAPPING_JSON.getBytes(StandardCharsets.UTF_8));
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, file.getAbsolutePath());

    TableNameMapper mapper = TableNameMapper.fromConfiguration(conf);

    assertEquals("smartbrain_oraperi_new", mapper.map("smartbrain:oraperi_new"));
  }

  @Test
  public void rejectsBothInlineAndFile() throws IOException {
    File file = writeFile("mapping.json", MAPPING_JSON.getBytes(StandardCharsets.UTF_8));
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_KEY, MAPPING_JSON);
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, file.getAbsolutePath());

    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsMissingFile() {
    Configuration conf = new Configuration(false);
    conf.set(
        TABLE_NAME_MAPPING_FILE_KEY,
        new File(tempFolder.getRoot(), "does-not-exist.json").getAbsolutePath());

    expectThrows(IOException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsDirectory() throws IOException {
    File dir = tempFolder.newFolder("mapping-dir");
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, dir.getAbsolutePath());

    expectThrows(IOException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsOversizedFile() throws IOException {
    byte[] contents = new byte[(int) TableNameMapper.MAX_MAPPING_FILE_SIZE_BYTES + 1];
    Arrays.fill(contents, (byte) ' ');
    File file = writeFile("huge.json", contents);
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, file.getAbsolutePath());

    expectThrows(IOException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsMalformedJson() {
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{not json", "test"));
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("[\"a\", \"b\"]", "test"));
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson("", "test"));
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson("null", "test"));
  }

  @Test
  public void acceptsEmptyObject() {
    assertTrue(TableNameMapper.fromJson("{}", "test").isIdentity());
  }

  @Test
  public void rejectsInvalidBigtableTableIds() {
    // Namespace separator is not allowed in a Bigtable table ID.
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"ns:t\"}", "test"));
    // Empty / null target.
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": \"\"}", "test"));
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": null}", "test"));
    // Leading '-' is not allowed.
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"-bad\"}", "test"));
    // Whitespace and slashes are not allowed.
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"bad id\"}", "test"));
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"a/b\"}", "test"));
    // Too long (51 characters).
    StringBuilder tooLong = new StringBuilder();
    for (int i = 0; i < 51; i++) {
      tooLong.append('a');
    }
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"" + tooLong + "\"}", "test"));
  }

  @Test
  public void rejectsEmptyHbaseTableName() {
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"\": \"t\"}", "test"));
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\" \": \"t\"}", "test"));
  }

  @Test
  public void acceptsValidBigtableTableIds() {
    TableNameMapper mapper =
        TableNameMapper.fromJson(
            "{\"a\": \"_leading_underscore\", \"b\": \"with.dots-and-dashes_1\", \"c\": \"9\"}",
            "test");
    assertEquals("_leading_underscore", mapper.map("a"));
    assertEquals("with.dots-and-dashes_1", mapper.map("b"));
    assertEquals("9", mapper.map("c"));
  }

  // --- Tests for Finding 1(a): Canonicalization & 1:1 Injectivity ---

  @Test
  public void normalizesDefaultNamespaceToCanonicalHBaseTable() {
    TableNameMapper mapper =
        TableNameMapper.fromJson("{\"default:my_table\": \"mapped_table\"}", "test");
    // Both default:my_table and my_table resolve to mapped_table.
    assertEquals("mapped_table", mapper.map("default:my_table"));
    assertEquals("mapped_table", mapper.map("my_table"));
    assertEquals("mapped_table", mapper.map(" default:my_table "));
  }

  @Test
  public void rejectsConflictingMappingsForSameCanonicalHBaseTable() {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            TableNameMapper.fromJson(
                "{\"my_table\": \"bt1\", \"default:my_table\": \"bt2\"}", "test"));
  }

  @Test
  public void rejectsDuplicateMappingForSameCanonicalHBaseTable() {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            TableNameMapper.fromJson(
                "{\"my_table\": \"bt1\", \"default:my_table\": \"bt1\"}", "test"));
  }

  @Test
  public void rejectsNToOneMappingAcrossDistinctHBaseTables() {
    // Distinct canonical HBase tables mapping to same Bigtable table ID.
    expectThrows(
        IllegalArgumentException.class,
        () ->
            TableNameMapper.fromJson(
                "{\"ns1:table\": \"shared_bt\", \"ns2:table\": \"shared_bt\"}", "test"));

    expectThrows(
        IllegalArgumentException.class,
        () ->
            TableNameMapper.fromJson(
                "{\"table1\": \"shared_bt\", \"default:table2\": \"shared_bt\"}", "test"));
  }

  @Test
  public void rejectsUnmappedIdentityFallbackCollidingWithMappedTarget() {
    TableNameMapper mapper = TableNameMapper.fromJson("{\"ns:table_B\": \"table_B\"}", "test");

    // The explicitly mapped table maps as expected.
    assertEquals("table_B", mapper.map("ns:table_B"));

    // An unmapped table literally named "table_B" collides with the mapped target "table_B".
    try {
      mapper.map("table_B");
      fail("Expected IllegalArgumentException on collision with mapped target");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Error message must mention collision and conflicting source: " + e.getMessage(),
          e.getMessage()
              .contains(
                  "Unmapped HBase table 'table_B' collides with target Cloud Bigtable table"
                      + " 'table_B' already mapped from HBase table 'ns:table_B'"));
    }

    // Default namespace alias of table_B also collides.
    try {
      mapper.map("default:table_B");
      fail("Expected IllegalArgumentException on collision with mapped target");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Error message must mention collision: " + e.getMessage(),
          e.getMessage()
              .contains(
                  "Unmapped HBase table 'table_B' collides with target Cloud Bigtable table"
                      + " 'table_B' already mapped from HBase table 'ns:table_B'"));
    }

    // An unmapped table with a distinct name does NOT collide.
    assertEquals("table_C", mapper.map("table_C"));
  }

  @Test
  public void allowsDisambiguatedMappingsForCollidingNames() {
    TableNameMapper mapper =
        TableNameMapper.fromJson(
            "{\"ns:table_B\": \"table_B\", \"table_B\": \"table_B_default\"}", "test");

    assertEquals("table_B", mapper.map("ns:table_B"));
    assertEquals("table_B_default", mapper.map("table_B"));
    assertEquals("table_B_default", mapper.map("default:table_B"));
  }

  @Test
  public void allowsExplicitIdentityMappingWithoutCollision() {
    TableNameMapper mapper = TableNameMapper.fromJson("{\"table_B\": \"table_B\"}", "test");

    assertEquals("table_B", mapper.map("table_B"));
    assertEquals("table_B", mapper.map("default:table_B"));
  }

  // --- Tests for Finding 3: Local FS Fallback & Clear Error Messages ---

  @Test
  public void loadsLocalFileWhenSchemeOmittedAndMissingOnDefaultFs() throws IOException {
    File file = writeFile("mapping.json", MAPPING_JSON.getBytes(StandardCharsets.UTF_8));
    Configuration conf = new Configuration(false);
    // Simulate a cluster where defaultFS points to a remote/nonexistent location.
    conf.set("fs.defaultFS", "file:///non/existent/default/fs/mount");
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, file.getAbsolutePath());

    TableNameMapper mapper = TableNameMapper.fromConfiguration(conf);
    assertEquals("smartbrain_oraperi_new", mapper.map("smartbrain:oraperi_new"));
    assertEquals("renamed-plain", mapper.map("plain"));
  }

  @Test
  public void informativeFileNotFoundExceptionMessage() {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, "/etc/hbase/conf/nonexistent-mapping.json");

    try {
      TableNameMapper.fromConfiguration(conf);
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue(
          "Message should explain resolved FileSystem URI and scheme options: " + e.getMessage(),
          e.getMessage().contains("resolved against FileSystem URI")
              && e.getMessage().contains("file://")
              && e.getMessage().contains("hdfs://"));
    } catch (IOException e) {
      fail("Expected FileNotFoundException but caught " + e.getClass().getName());
    }
  }

  // --- Tests for Finding 4: Enforce Size Cap on Inline JSON ---

  @Test
  public void rejectsOversizedInlineJson() {
    char[] chars = new char[(int) TableNameMapper.MAX_MAPPING_FILE_SIZE_BYTES + 10];
    Arrays.fill(chars, ' ');
    String hugeJson = "{\"ns:t\": \"val\"" + new String(chars) + "}";

    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson(hugeJson, "inline"));
  }

  // --- Tests for Finding 5: Strict JSON Parsing ---

  @Test
  public void rejectsNumberValueInJson() {
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": 123}", "test"));
  }

  @Test
  public void rejectsBooleanValueInJson() {
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": true}", "test"));
  }

  @Test
  public void rejectsNullValueInJson() {
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": null}", "test"));
  }

  @Test
  public void rejectsArrayValueInJson() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": [\"target\"]}", "test"));
  }

  @Test
  public void rejectsNestedObjectValueInJson() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": {\"inner\": \"val\"}}", "test"));
  }

  @Test
  public void rejectsDuplicateRawKeyInJson() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"a\", \"ns:t\": \"b\"}", "test"));
  }

  @Test
  public void rejectsTrailingGarbageAfterJsonObject() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"a\"} trailing", "test"));
  }

  // --- Tests for Finding 6: Prefix Configuration Support ---

  @Test
  public void loadsPrefixConfiguration() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_PREFIX + "smartbrain:oraperi_new", "smartbrain_oraperi_new");
    conf.set(TABLE_NAME_MAPPING_PREFIX + "plain", "renamed-plain");

    TableNameMapper mapper = TableNameMapper.fromConfiguration(conf);

    assertFalse(mapper.isIdentity());
    assertEquals("smartbrain_oraperi_new", mapper.map("smartbrain:oraperi_new"));
    assertEquals("renamed-plain", mapper.map("plain"));
    assertEquals("other:table", mapper.map("other:table"));
  }

  @Test
  public void prefixConfigurationNormalizesDefaultNamespace() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_PREFIX + "default:plain", "renamed-plain");

    TableNameMapper mapper = TableNameMapper.fromConfiguration(conf);

    assertEquals("renamed-plain", mapper.map("plain"));
    assertEquals("renamed-plain", mapper.map("default:plain"));
  }

  @Test
  public void rejectsMutualExclusivityInlineAndPrefix() {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_KEY, "{\"ns:t\": \"mapped\"}");
    conf.set(TABLE_NAME_MAPPING_PREFIX + "other", "mapped_other");

    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsMutualExclusivityFileAndPrefix() throws IOException {
    File file = writeFile("mapping.json", MAPPING_JSON.getBytes(StandardCharsets.UTF_8));
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_FILE_KEY, file.getAbsolutePath());
    conf.set(TABLE_NAME_MAPPING_PREFIX + "other", "mapped_other");

    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsPrefixWithInvalidTargetTableId() {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_PREFIX + "ns:table", "-invalid-prefix");

    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsPrefixNToOneCollisions() {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_PREFIX + "ns1:t", "shared_target");
    conf.set(TABLE_NAME_MAPPING_PREFIX + "ns2:t", "shared_target");

    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsPrefixConflictingDefaultNamespace() {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_PREFIX + "plain", "target1");
    conf.set(TABLE_NAME_MAPPING_PREFIX + "default:plain", "target2");

    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromConfiguration(conf));
  }

  @Test
  public void rejectsPrefixWithEmptyHBaseTableName() {
    Configuration conf1 = new Configuration(false);
    conf1.set(TABLE_NAME_MAPPING_PREFIX, "some_target");
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromConfiguration(conf1));

    Configuration conf2 = new Configuration(false);
    conf2.set(TABLE_NAME_MAPPING_PREFIX + "   ", "some_target");
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromConfiguration(conf2));
  }

  /** JUnit 4.12 (pulled in transitively by hbase-client) does not have assertThrows. */
  private static void expectThrows(Class<? extends Throwable> expected, ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Throwable t) {
      if (expected.isInstance(t)) {
        return;
      }
      throw new AssertionError(
          "Expected " + expected.getName() + " but got " + t.getClass().getName(), t);
    }
    fail("Expected " + expected.getName() + " to be thrown.");
  }

  private interface ThrowingRunnable {
    void run() throws Throwable;
  }
}
