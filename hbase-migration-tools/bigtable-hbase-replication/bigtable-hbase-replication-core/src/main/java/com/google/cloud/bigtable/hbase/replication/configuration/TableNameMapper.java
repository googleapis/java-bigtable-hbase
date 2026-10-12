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

import com.google.bigtable.repackaged.com.google.gson.stream.JsonReader;
import com.google.bigtable.repackaged.com.google.gson.stream.JsonToken;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps HBase table names to Cloud Bigtable table IDs during replication.
 *
 * <p>The mapping defines canonical HBase table names to Cloud Bigtable table IDs and is loaded from
 * {@link HBaseToCloudBigtableReplicationConfiguration#TABLE_NAME_MAPPING_KEY} as a JSON string
 * (e.g. {@code {"ns:table": "mapped_table"}}).
 *
 * <p>Source HBase table names are normalized to canonical form (for example, {@code default:table}
 * resolves to {@code table}). Mappings must be injective (1:1): multiple HBase tables cannot map to
 * the same Cloud Bigtable table ID.
 *
 * <p>Tables not present in the mapping keep their canonical HBase name, preserving default
 * behavior.
 *
 * <p>Instances are immutable and thread safe.
 */
public final class TableNameMapper {

  private static final Logger LOG = LoggerFactory.getLogger(TableNameMapper.class);

  /**
   * Cloud Bigtable table ID constraints. See
   * https://cloud.google.com/bigtable/docs/reference/admin/rpc/google.bigtable.admin.v2#createtablerequest
   */
  private static final Pattern BIGTABLE_TABLE_ID_PATTERN =
      Pattern.compile("[_a-zA-Z0-9][-_.a-zA-Z0-9]*");

  private static final int MAX_BIGTABLE_TABLE_ID_LENGTH = 50;

  /**
   * Upper bound on the size of the mapping document. A mapping configuration is a small list of
   * table names; anything larger is almost certainly a misconfiguration and must not be buffered in
   * the region server heap.
   */
  static final long MAX_MAPPING_SIZE_BYTES = 1024 * 1024;

  private static final TableNameMapper IDENTITY =
      new TableNameMapper(
          Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap());

  private final Map<String, String> mappings;
  private final Map<String, String> reservedTargets;

  private TableNameMapper(Map<String, String> mappings, Map<String, String> reservedTargets) {
    this.mappings = Collections.unmodifiableMap(new HashMap<>(mappings));
    this.reservedTargets = Collections.unmodifiableMap(new HashMap<>(reservedTargets));
  }

  /** Returns a mapper that leaves every table name unchanged. */
  public static TableNameMapper identity() {
    return IDENTITY;
  }

  /**
   * Builds a mapper from the replication configuration.
   *
   * @throws IllegalArgumentException if the mapping is malformed.
   */
  public static TableNameMapper fromConfiguration(Configuration configuration) {
    if (configuration == null) {
      return IDENTITY;
    }

    String inlineJson = configuration.get(TABLE_NAME_MAPPING_KEY);
    if (inlineJson == null || inlineJson.trim().isEmpty()) {
      return IDENTITY;
    }
    return fromJson(inlineJson.trim());
  }

  /**
   * Builds a mapper from a JSON object of {@code "hbaseTableName": "bigtableTableId"} pairs.
   *
   * @param json the JSON document.
   * @throws IllegalArgumentException if the JSON is malformed or contains an invalid table name.
   */
  public static TableNameMapper fromJson(String json) {
    if (json == null || json.trim().isEmpty()) {
      LOG.warn(
          "Table name mapping from {} is empty; continuing with identity mapping.",
          TABLE_NAME_MAPPING_KEY);
      return IDENTITY;
    }
    Map<String, String> rawMappings = parseJsonMappings(json);
    return validateAndBuild(rawMappings);
  }

  @SuppressWarnings("deprecation")
  private static Map<String, String> parseJsonMappings(String json) {
    if (json.length() > MAX_MAPPING_SIZE_BYTES
        || (json.length() > MAX_MAPPING_SIZE_BYTES / 4
            && json.getBytes(StandardCharsets.UTF_8).length > MAX_MAPPING_SIZE_BYTES)) {
      throw new IllegalArgumentException(
          "Table name mapping from "
              + TABLE_NAME_MAPPING_KEY
              + " exceeds maximum allowed size of "
              + MAX_MAPPING_SIZE_BYTES
              + " bytes.");
    }

    Map<String, String> rawMappings = new LinkedHashMap<>();
    try (JsonReader reader = new JsonReader(new StringReader(json))) {
      reader.setLenient(false);
      JsonToken firstToken = reader.peek();
      if (firstToken != JsonToken.BEGIN_OBJECT) {
        throw new IllegalArgumentException(
            "Table name mapping from "
                + TABLE_NAME_MAPPING_KEY
                + " is not a valid JSON object: expected BEGIN_OBJECT, found "
                + firstToken);
      }
      reader.beginObject();
      while (reader.hasNext()) {
        String hbaseTableName = reader.nextName();
        JsonToken valueToken = reader.peek();
        if (valueToken != JsonToken.STRING) {
          throw new IllegalArgumentException(
              "Table name mapping from "
                  + TABLE_NAME_MAPPING_KEY
                  + " has invalid non-string value for HBase table '"
                  + hbaseTableName
                  + "': expected STRING, found "
                  + valueToken);
        }
        String bigtableTableId = reader.nextString();
        if (rawMappings.put(hbaseTableName, bigtableTableId) != null) {
          throw new IllegalArgumentException(
              "Table name mapping from "
                  + TABLE_NAME_MAPPING_KEY
                  + " contains duplicate key: '"
                  + hbaseTableName
                  + "'.");
        }
      }
      reader.endObject();
      if (reader.peek() != JsonToken.END_DOCUMENT) {
        throw new IllegalArgumentException(
            "Table name mapping from "
                + TABLE_NAME_MAPPING_KEY
                + " has trailing characters after JSON object.");
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Table name mapping from " + TABLE_NAME_MAPPING_KEY + " is not a valid JSON object.", e);
    }
    return rawMappings;
  }

  private static TableNameMapper validateAndBuild(Map<String, String> rawMappings) {
    if (rawMappings == null || rawMappings.isEmpty()) {
      LOG.warn(
          "Table name mapping from {} contains no entries; continuing with identity mapping.",
          TABLE_NAME_MAPPING_KEY);
      return IDENTITY;
    }

    Map<String, String> validated = new HashMap<>(rawMappings.size());
    Map<String, String> targetToSource = new HashMap<>(rawMappings.size());

    for (Map.Entry<String, String> entry : rawMappings.entrySet()) {
      String rawHBaseTable = entry.getKey();
      String bigtableTableId = entry.getValue();

      String canonicalHBaseTable = canonicalizeHBaseTableName(rawHBaseTable);

      if (validated.containsKey(canonicalHBaseTable)) {
        throw new IllegalArgumentException(
            "Table name mapping from "
                + TABLE_NAME_MAPPING_KEY
                + " contains duplicate or conflicting mapping for HBase table '"
                + canonicalHBaseTable
                + "' (source key: '"
                + rawHBaseTable
                + "').");
      }

      validateBigtableTableId(canonicalHBaseTable, bigtableTableId);

      if (targetToSource.containsKey(bigtableTableId)) {
        String existingSource = targetToSource.get(bigtableTableId);
        throw new IllegalArgumentException(
            "Table name mapping from "
                + TABLE_NAME_MAPPING_KEY
                + " maps multiple HBase tables to the same Cloud Bigtable table '"
                + bigtableTableId
                + "': '"
                + existingSource
                + "' and '"
                + canonicalHBaseTable
                + "'. N:1 table mappings are not supported.");
      }

      validated.put(canonicalHBaseTable, bigtableTableId);
      targetToSource.put(bigtableTableId, canonicalHBaseTable);
    }

    LOG.info(
        "Loaded {} table name mapping(s) from {}: {}",
        validated.size(),
        TABLE_NAME_MAPPING_KEY,
        validated);
    return new TableNameMapper(validated, targetToSource);
  }

  private static String canonicalizeHBaseTableName(String rawHBaseTable) {
    if (rawHBaseTable == null) {
      throw new IllegalArgumentException(
          "Table name mapping from "
              + TABLE_NAME_MAPPING_KEY
              + " contains an empty HBase table name.");
    }
    String trimmed = rawHBaseTable.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException(
          "Table name mapping from "
              + TABLE_NAME_MAPPING_KEY
              + " contains an empty HBase table name.");
    }
    try {
      return TableName.valueOf(trimmed).getNameAsString();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Table name mapping from "
              + TABLE_NAME_MAPPING_KEY
              + " contains an invalid HBase table name: '"
              + rawHBaseTable
              + "'",
          e);
    }
  }

  private static void validateBigtableTableId(String hbaseTableName, String bigtableTableId) {
    if (bigtableTableId == null
        || bigtableTableId.isEmpty()
        || bigtableTableId.length() > MAX_BIGTABLE_TABLE_ID_LENGTH
        || !BIGTABLE_TABLE_ID_PATTERN.matcher(bigtableTableId).matches()) {
      throw new IllegalArgumentException(
          "Table name mapping from "
              + TABLE_NAME_MAPPING_KEY
              + " maps HBase table '"
              + hbaseTableName
              + "' to '"
              + bigtableTableId
              + "', which is not a valid Cloud Bigtable table ID. Table IDs must match "
              + BIGTABLE_TABLE_ID_PATTERN.pattern()
              + " and be at most "
              + MAX_BIGTABLE_TABLE_ID_LENGTH
              + " characters.");
    }
  }

  /**
   * Returns the Cloud Bigtable table ID that WAL entries for {@code hbaseTableName} should be
   * written to. Returns {@code hbaseTableName} (or its canonical form) unchanged if it is not
   * present in the mapping, unless its name is already reserved as the mapping target of another
   * HBase table.
   *
   * @throws IllegalStateException if {@code hbaseTableName} is unmapped and its canonical name is
   *     already reserved as the target of a mapped HBase table.
   */
  public String map(String hbaseTableName) {
    if (hbaseTableName == null) {
      return null;
    }
    String canonical = canonicalizeHBaseTableName(hbaseTableName);
    String mapped = mappings.get(canonical);
    if (mapped != null) {
      return mapped;
    }
    String owner = reservedTargets.get(canonical);
    if (owner != null) {
      throw new IllegalStateException(
          "Unmapped HBase table '"
              + canonical
              + "' would replicate to Cloud Bigtable table '"
              + canonical
              + "', which is already the mapping target of '"
              + owner
              + "'. Add an explicit mapping for '"
              + canonical
              + "' (e.g. \""
              + canonical
              + "\": \""
              + canonical
              + "_default\").");
    }
    return canonical;
  }

  /** Returns true if this mapper does not rename any table. */
  public boolean isIdentity() {
    return mappings.isEmpty();
  }

  /** Returns an unmodifiable view of the configured mappings. */
  public Map<String, String> getMappings() {
    return mappings;
  }

  @Override
  public String toString() {
    return "TableNameMapper" + mappings;
  }
}
