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

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.TABLE_NAME_MAPPING_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TableNameMapperTest {

  private static final String MAPPING_JSON =
      "{\"my_ns:my_table\": \"my_ns_my_table\", \"plain\": \"renamed-plain\"}";

  @Test
  public void identityWhenNothingConfigured() {
    TableNameMapper mapper = TableNameMapper.fromConfiguration(new Configuration(false));
    assertTrue(mapper.isIdentity());
    assertEquals("ns:table", mapper.map("ns:table"));
    assertEquals("table", mapper.map("table"));
  }

  @Test
  public void identityWhenConfigurationIsNull() {
    assertTrue(TableNameMapper.fromConfiguration(null).isIdentity());
  }

  @Test
  public void identityWhenKeysAreBlank() {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_KEY, "   ");
    assertTrue(TableNameMapper.fromConfiguration(conf).isIdentity());
  }

  @Test
  public void loadsInlineJson() {
    Configuration conf = new Configuration(false);
    conf.set(TABLE_NAME_MAPPING_KEY, MAPPING_JSON);

    TableNameMapper mapper = TableNameMapper.fromConfiguration(conf);

    assertFalse(mapper.isIdentity());
    assertEquals("my_ns_my_table", mapper.map("my_ns:my_table"));
    assertEquals("renamed-plain", mapper.map("plain"));
    // Unmapped tables keep their HBase name.
    assertEquals("other:table", mapper.map("other:table"));
    assertEquals(2, mapper.getMappings().size());
  }

  @Test
  public void rejectsMalformedJson() {
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson("{not json"));
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson("[\"a\", \"b\"]"));
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson("null"));
  }

  @Test
  public void acceptsEmptyObjectAndBlankJson() {
    assertTrue(TableNameMapper.fromJson("{}").isIdentity());
    assertTrue(TableNameMapper.fromJson("").isIdentity());
    assertTrue(TableNameMapper.fromJson("   ").isIdentity());
    assertTrue(TableNameMapper.fromJson(null).isIdentity());
  }

  @Test
  public void rejectsInvalidBigtableTableIds() {
    // Namespace separator is not allowed in a Bigtable table ID.
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": \"ns:t\"}"));
    // Empty / null target.
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": \"\"}"));
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": null}"));
    // Leading '-' is not allowed.
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": \"-bad\"}"));
    // Whitespace and slashes are not allowed.
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": \"bad id\"}"));
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": \"a/b\"}"));
    // Too long (51 characters).
    StringBuilder tooLong = new StringBuilder();
    for (int i = 0; i < 51; i++) {
      tooLong.append('a');
    }
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"" + tooLong + "\"}"));
  }

  @Test
  public void rejectsEmptyHbaseTableName() {
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"\": \"t\"}"));
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\" \": \"t\"}"));
  }

  @Test
  public void acceptsValidBigtableTableIds() {
    TableNameMapper mapper =
        TableNameMapper.fromJson(
            "{\"a\": \"_leading_underscore\", \"b\": \"with.dots-and-dashes_1\", \"c\": \"9\"}");
    assertEquals("_leading_underscore", mapper.map("a"));
    assertEquals("with.dots-and-dashes_1", mapper.map("b"));
    assertEquals("9", mapper.map("c"));
  }

  @Test
  public void normalizesDefaultNamespaceToCanonicalHBaseTable() {
    TableNameMapper mapper = TableNameMapper.fromJson("{\"default:my_table\": \"mapped_table\"}");
    // Both default:my_table and my_table resolve to mapped_table.
    assertEquals("mapped_table", mapper.map("default:my_table"));
    assertEquals("mapped_table", mapper.map("my_table"));
    assertEquals("mapped_table", mapper.map(" default:my_table "));
  }

  @Test
  public void rejectsConflictingMappingsForSameCanonicalHBaseTable() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"my_table\": \"bt1\", \"default:my_table\": \"bt2\"}"));
  }

  @Test
  public void rejectsDuplicateMappingForSameCanonicalHBaseTable() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"my_table\": \"bt1\", \"default:my_table\": \"bt1\"}"));
  }

  @Test
  public void rejectsNToOneMappingAcrossDistinctHBaseTables() {
    // Distinct canonical HBase tables mapping to same Bigtable table ID.
    expectThrows(
        IllegalArgumentException.class,
        () ->
            TableNameMapper.fromJson(
                "{\"ns1:table\": \"shared_bt\", \"ns2:table\": \"shared_bt\"}"));

    expectThrows(
        IllegalArgumentException.class,
        () ->
            TableNameMapper.fromJson(
                "{\"table1\": \"shared_bt\", \"default:table2\": \"shared_bt\"}"));
  }

  @Test
  public void rejectsUnmappedTableCollidingWithMappingTarget() {
    TableNameMapper mapper = TableNameMapper.fromJson("{\"ns:table_B\": \"table_B\"}");

    assertEquals("table_B", mapper.map("ns:table_B"));
    expectThrows(IllegalStateException.class, () -> mapper.map("table_B"));
    expectThrows(IllegalStateException.class, () -> mapper.map("default:table_B"));
    assertEquals("table_C", mapper.map("table_C"));
  }

  @Test
  public void allowsDisambiguatedMappingsForCollidingNames() {
    TableNameMapper mapper =
        TableNameMapper.fromJson("{\"ns:table_B\": \"table_B\", \"table_B\": \"table_B_default\"}");

    assertEquals("table_B", mapper.map("ns:table_B"));
    assertEquals("table_B_default", mapper.map("table_B"));
    assertEquals("table_B_default", mapper.map("default:table_B"));
  }

  @Test
  public void allowsExplicitIdentityMappingWithoutCollision() {
    TableNameMapper mapper = TableNameMapper.fromJson("{\"table_B\": \"table_B\"}");

    assertEquals("table_B", mapper.map("table_B"));
    assertEquals("table_B", mapper.map("default:table_B"));
  }

  @Test
  public void rejectsOversizedInlineJson() {
    char[] chars = new char[(int) TableNameMapper.MAX_MAPPING_SIZE_BYTES + 10];
    Arrays.fill(chars, ' ');
    String hugeJson = "{\"ns:t\": \"val\"" + new String(chars) + "}";

    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson(hugeJson));
  }

  @Test
  public void rejectsNumberValueInJson() {
    expectThrows(IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": 123}"));
  }

  @Test
  public void rejectsBooleanValueInJson() {
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": true}"));
  }

  @Test
  public void rejectsNullValueInJson() {
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": null}"));
  }

  @Test
  public void rejectsArrayValueInJson() {
    expectThrows(
        IllegalArgumentException.class, () -> TableNameMapper.fromJson("{\"ns:t\": [\"target\"]}"));
  }

  @Test
  public void rejectsNestedObjectValueInJson() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": {\"inner\": \"val\"}}"));
  }

  @Test
  public void rejectsDuplicateRawKeyInJson() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"a\", \"ns:t\": \"b\"}"));
  }

  @Test
  public void rejectsTrailingGarbageAfterJsonObject() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TableNameMapper.fromJson("{\"ns:t\": \"a\"} trailing"));
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
