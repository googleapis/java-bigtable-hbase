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

import com.google.bigtable.repackaged.com.google.gson.stream.JsonReader;
import com.google.bigtable.repackaged.com.google.gson.stream.JsonToken;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps HBase table names to Cloud Bigtable table IDs during replication.
 *
 * <p>The mapping defines canonical HBase table names to Cloud Bigtable table IDs and can be loaded
 * from one of three mutually exclusive mechanisms:
 *
 * <ul>
 *   <li>{@link HBaseToCloudBigtableReplicationConfiguration#TABLE_NAME_MAPPING_KEY}: inline JSON
 *       string (e.g. {@code {"ns:table": "mapped_table"}}).
 *   <li>{@link HBaseToCloudBigtableReplicationConfiguration#TABLE_NAME_MAPPING_FILE_KEY}: path to a
 *       JSON file resolvable via Hadoop {@code FileSystem} (e.g. {@code file:///path/to/file.json}
 *       or {@code hdfs://...}).
 *   <li>{@link HBaseToCloudBigtableReplicationConfiguration#TABLE_NAME_MAPPING_PREFIX}: individual
 *       properties in {@code Configuration} prefixed with {@code
 *       google.bigtable.replication.table_name_mapping.<hbase_table>=<bigtable_table>}.
 * </ul>
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
  static final long MAX_MAPPING_FILE_SIZE_BYTES = 1024 * 1024;

  private static final TableNameMapper IDENTITY =
      new TableNameMapper(
          Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap());

  private final Map<String, String> mappings;
  private final Map<String, String> targetToSource;

  private TableNameMapper(Map<String, String> mappings, Map<String, String> targetToSource) {
    this.mappings = Collections.unmodifiableMap(new HashMap<>(mappings));
    this.targetToSource = Collections.unmodifiableMap(new HashMap<>(targetToSource));
  }

  /** Returns a mapper that leaves every table name unchanged. */
  public static TableNameMapper identity() {
    return IDENTITY;
  }

  /**
   * Builds a mapper from the replication configuration.
   *
   * @throws IllegalArgumentException if more than one mapping mechanism is configured or if the
   *     mapping is malformed.
   * @throws IOException if the mapping file cannot be read.
   */
  public static TableNameMapper fromConfiguration(Configuration configuration) throws IOException {
    if (configuration == null) {
      return IDENTITY;
    }

    String inlineJson = configuration.getTrimmed(TABLE_NAME_MAPPING_KEY);
    String mappingFile = configuration.getTrimmed(TABLE_NAME_MAPPING_FILE_KEY);

    Map<String, String> prefixMappings = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : configuration) {
      String key = entry.getKey();
      if (key != null && key.startsWith(TABLE_NAME_MAPPING_PREFIX)) {
        String hbaseTable = key.substring(TABLE_NAME_MAPPING_PREFIX.length()).trim();
        if (hbaseTable.isEmpty()) {
          throw new IllegalArgumentException(
              "Configuration property '"
                  + key
                  + "' has an empty HBase table name after prefix '"
                  + TABLE_NAME_MAPPING_PREFIX
                  + "'.");
        }
        prefixMappings.put(hbaseTable, configuration.getTrimmed(key));
      }
    }

    boolean hasInline = inlineJson != null && !inlineJson.isEmpty();
    boolean hasFile = mappingFile != null && !mappingFile.isEmpty();
    boolean hasPrefix = !prefixMappings.isEmpty();

    int sourceCount = (hasInline ? 1 : 0) + (hasFile ? 1 : 0) + (hasPrefix ? 1 : 0);
    if (sourceCount > 1) {
      throw new IllegalArgumentException(
          "Only one of inline JSON ("
              + TABLE_NAME_MAPPING_KEY
              + "), mapping file ("
              + TABLE_NAME_MAPPING_FILE_KEY
              + "), or prefixed properties ("
              + TABLE_NAME_MAPPING_PREFIX
              + "<hbase_table>) can be set.");
    }

    if (hasInline) {
      return fromJson(inlineJson, TABLE_NAME_MAPPING_KEY);
    }
    if (hasFile) {
      return fromJson(readMappingFile(configuration, mappingFile), mappingFile);
    }
    if (hasPrefix) {
      return validateAndBuild(
          prefixMappings, "prefixed properties (" + TABLE_NAME_MAPPING_PREFIX + ")");
    }
    return IDENTITY;
  }

  /**
   * Builds a mapper from a JSON object of {@code "hbaseTableName": "bigtableTableId"} pairs.
   *
   * @param json the JSON document.
   * @param source human readable description of where the JSON came from, used in error messages.
   * @throws IllegalArgumentException if the JSON is malformed or contains an invalid table name.
   */
  public static TableNameMapper fromJson(String json, String source) {
    Map<String, String> rawMappings = parseJsonMappings(json, source);
    return validateAndBuild(rawMappings, source);
  }

  @SuppressWarnings("deprecation")
  private static Map<String, String> parseJsonMappings(String json, String source) {
    if (json == null || json.trim().isEmpty()) {
      throw new IllegalArgumentException("Table name mapping from " + source + " is empty.");
    }
    if (json.length() > MAX_MAPPING_FILE_SIZE_BYTES
        || json.getBytes(StandardCharsets.UTF_8).length > MAX_MAPPING_FILE_SIZE_BYTES) {
      throw new IllegalArgumentException(
          "Table name mapping from "
              + source
              + " exceeds maximum allowed size of "
              + MAX_MAPPING_FILE_SIZE_BYTES
              + " bytes.");
    }

    Map<String, String> rawMappings = new LinkedHashMap<>();
    try (JsonReader reader = new JsonReader(new StringReader(json))) {
      reader.setLenient(false);
      JsonToken firstToken = reader.peek();
      if (firstToken != JsonToken.BEGIN_OBJECT) {
        throw new IllegalArgumentException(
            "Table name mapping from "
                + source
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
                  + source
                  + " has invalid non-string value for HBase table '"
                  + hbaseTableName
                  + "': expected STRING, found "
                  + valueToken);
        }
        String bigtableTableId = reader.nextString();
        if (rawMappings.put(hbaseTableName, bigtableTableId) != null) {
          throw new IllegalArgumentException(
              "Table name mapping from "
                  + source
                  + " contains duplicate key: '"
                  + hbaseTableName
                  + "'.");
        }
      }
      reader.endObject();
      if (reader.peek() != JsonToken.END_DOCUMENT) {
        throw new IllegalArgumentException(
            "Table name mapping from " + source + " has trailing characters after JSON object.");
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Table name mapping from " + source + " is not a valid JSON object.", e);
    }
    return rawMappings;
  }

  private static TableNameMapper validateAndBuild(Map<String, String> rawMappings, String source) {
    if (rawMappings == null || rawMappings.isEmpty()) {
      return IDENTITY;
    }

    Map<String, String> validated = new HashMap<>(rawMappings.size());
    Map<String, String> targetToSource = new HashMap<>(rawMappings.size());

    for (Map.Entry<String, String> entry : rawMappings.entrySet()) {
      String rawHBaseTable = entry.getKey();
      String bigtableTableId = entry.getValue();

      String canonicalHBaseTable = canonicalizeHBaseTableName(rawHBaseTable, source);

      if (validated.containsKey(canonicalHBaseTable)) {
        throw new IllegalArgumentException(
            "Table name mapping from "
                + source
                + " contains duplicate or conflicting mapping for HBase table '"
                + canonicalHBaseTable
                + "' (source key: '"
                + rawHBaseTable
                + "').");
      }

      validateBigtableTableId(canonicalHBaseTable, bigtableTableId, source);

      if (targetToSource.containsKey(bigtableTableId)) {
        String existingSource = targetToSource.get(bigtableTableId);
        throw new IllegalArgumentException(
            "Table name mapping from "
                + source
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

    LOG.info("Loaded {} table name mapping(s) from {}: {}", validated.size(), source, validated);
    return new TableNameMapper(validated, targetToSource);
  }

  private static String canonicalizeHBaseTableName(String rawHBaseTable, String source) {
    if (rawHBaseTable == null) {
      throw new IllegalArgumentException(
          "Table name mapping from " + source + " contains an empty HBase table name.");
    }
    String trimmed = rawHBaseTable.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException(
          "Table name mapping from " + source + " contains an empty HBase table name.");
    }
    try {
      return TableName.valueOf(trimmed).getNameAsString();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Table name mapping from "
              + source
              + " contains an invalid HBase table name: '"
              + rawHBaseTable
              + "'",
          e);
    }
  }

  private static void validateBigtableTableId(
      String hbaseTableName, String bigtableTableId, String source) {
    if (bigtableTableId == null
        || bigtableTableId.isEmpty()
        || bigtableTableId.length() > MAX_BIGTABLE_TABLE_ID_LENGTH
        || !BIGTABLE_TABLE_ID_PATTERN.matcher(bigtableTableId).matches()) {
      throw new IllegalArgumentException(
          "Table name mapping from "
              + source
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

  private static String readMappingFile(Configuration configuration, String location)
      throws IOException {
    Configuration fsConf = new Configuration(configuration);
    for (Map.Entry<String, String> entry : configuration) {
      fsConf.set(entry.getKey(), entry.getValue());
    }
    Path path = new Path(location);
    FileSystem fs = path.getFileSystem(fsConf);

    boolean existsOnDefaultFs = false;
    IOException defaultFsException = null;
    try {
      existsOnDefaultFs = fs.exists(path);
    } catch (IOException e) {
      defaultFsException = e;
      LOG.debug("Failed to check existence on default filesystem {}", fs.getUri(), e);
    }

    if (!existsOnDefaultFs) {
      if (path.toUri().getScheme() == null) {
        FileSystem localFs = FileSystem.getLocal(fsConf);
        Path localPath = new Path(new File(location).getAbsolutePath());
        if (localFs.exists(localPath)) {
          fs = localFs;
          path = localPath;
        } else {
          FileNotFoundException fnfe =
              new FileNotFoundException(
                  "Table name mapping file does not exist: "
                      + location
                      + " (resolved against FileSystem URI: "
                      + fs.getUri()
                      + "; if referencing a local file on region servers, prefix with 'file://' or ensure the file exists; if referencing HDFS, prefix with 'hdfs://').");
          if (defaultFsException != null) {
            fnfe.initCause(defaultFsException);
          }
          throw fnfe;
        }
      } else {
        FileNotFoundException fnfe =
            new FileNotFoundException(
                "Table name mapping file does not exist: "
                    + location
                    + " (resolved against FileSystem URI: "
                    + fs.getUri()
                    + "; prefix with 'file://' for local files or 'hdfs://' for HDFS).");
        if (defaultFsException != null) {
          fnfe.initCause(defaultFsException);
        }
        throw fnfe;
      }
    }

    FileStatus status = fs.getFileStatus(path);
    if (status.isDirectory()) {
      throw new IOException("Table name mapping file " + location + " is a directory.");
    }
    if (status.getLen() > MAX_MAPPING_FILE_SIZE_BYTES) {
      throw new IOException(
          "Table name mapping file "
              + location
              + " is "
              + status.getLen()
              + " bytes; the maximum supported size is "
              + MAX_MAPPING_FILE_SIZE_BYTES
              + " bytes.");
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    long totalRead = 0;
    try (FSDataInputStream in = fs.open(path)) {
      int bytesRead;
      while ((bytesRead = in.read(buffer)) != -1) {
        totalRead += bytesRead;
        if (totalRead > MAX_MAPPING_FILE_SIZE_BYTES) {
          throw new IOException(
              "Table name mapping file "
                  + location
                  + " exceeds maximum allowed size of "
                  + MAX_MAPPING_FILE_SIZE_BYTES
                  + " bytes.");
        }
        out.write(buffer, 0, bytesRead);
      }
    }
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  /**
   * Returns the Cloud Bigtable table ID that WAL entries for {@code hbaseTableName} should be
   * written to. Returns {@code hbaseTableName} (or its canonical form) unchanged if it is not
   * present in the mapping.
   */
  public String map(String hbaseTableName) {
    if (hbaseTableName == null) {
      return null;
    }
    String canonical;
    try {
      canonical = TableName.valueOf(hbaseTableName.trim()).getNameAsString();
    } catch (IllegalArgumentException e) {
      canonical = hbaseTableName.trim();
    }
    String mapped = mappings.get(canonical);
    if (mapped != null) {
      return mapped;
    }
    String conflictingSource = targetToSource.get(canonical);
    if (conflictingSource != null) {
      throw new IllegalArgumentException(
          "Unmapped HBase table '"
              + canonical
              + "' collides with target Cloud Bigtable table '"
              + canonical
              + "' already mapped from HBase table '"
              + conflictingSource
              + "'. Map '"
              + canonical
              + "' explicitly to a distinct Cloud Bigtable table ID.");
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
