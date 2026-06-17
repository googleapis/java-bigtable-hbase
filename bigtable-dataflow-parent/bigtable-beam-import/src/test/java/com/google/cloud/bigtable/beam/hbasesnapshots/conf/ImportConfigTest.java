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
package com.google.cloud.bigtable.beam.hbasesnapshots.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ImportConfigTest {

  private ImportConfig createValidConfig() {
    ImportConfig config = new ImportConfig();
    config.setSourcepath("gs://my-bucket/hbase-export");
    java.util.Map<String, String> snapshots = new java.util.HashMap<>();
    snapshots.put("snapshot1", "table1");
    config.setSnapshotsFromMap(snapshots);
    return config;
  }

  @Test
  public void testDefaultConfigIsValid() {
    ImportConfig config = createValidConfig();
    config.validate();
    assertEquals(5000L, config.getBackoffInitialIntervalInMillis());
    assertEquals(180000L, config.getBackoffMaxIntervalInMillis());
    assertEquals(3, config.getBackoffMaxretries());
  }

  @Test
  public void testSetValidBackoffValues() {
    ImportConfig config = createValidConfig();
    config.setBackoffInitialIntervalInMillis(1000L);
    config.setBackoffMaxIntervalInMillis(5000L);
    config.setBackoffMaxretries(5);

    assertEquals(1000L, config.getBackoffInitialIntervalInMillis());
    assertEquals(5000L, config.getBackoffMaxIntervalInMillis());
    assertEquals(5, config.getBackoffMaxretries());
  }

  @Test
  public void testInitialIntervalGreaterThanMaxInterval_ThrowsException() {
    ImportConfig config = createValidConfig();
    try {
      config.setBackoffInitialIntervalInMillis(200000L); // Default max is 180000L
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      config.setBackoffMaxIntervalInMillis(1000L); // Default initial is 5000L
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
  }

  @Test
  public void testNegativeAndZeroValues_ThrowsException() {
    ImportConfig config = createValidConfig();
    try {
      config.setBackoffInitialIntervalInMillis(-10L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      config.setBackoffInitialIntervalInMillis(0L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      config.setBackoffMaxIntervalInMillis(-10L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      config.setBackoffMaxIntervalInMillis(0L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      config.setBackoffMaxretries(-1);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
  }

  @Test
  public void testValidateMethodCatchesDivergentValues() {
    ImportConfig config = createValidConfig();

    config.setBackoffMaxIntervalInMillis(10000L);
    config.setBackoffInitialIntervalInMillis(2000L);
    config.validate(); // Should pass
  }

  @Test
  public void testValidate_EmptySourcePath_ThrowsException() {
    ImportConfig config = createValidConfig();
    config.setSourcepath("  ");
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for blank sourcepath");
    } catch (IllegalArgumentException expected) {
      assertEquals(
          "Source Path containing hbase snapshots must be specified.", expected.getMessage());
    }
  }

  @Test
  public void testValidate_EmptyRestorePath_ThrowsException() {
    ImportConfig config = createValidConfig();
    config.setRestorepath("   ");
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for blank restorepath");
    } catch (IllegalArgumentException expected) {
      assertEquals("restorepath cannot be empty if specified", expected.getMessage());
    }

    // null restorepath should be valid (will use default path during setup)
    config.setRestorepath(null);
    config.validate();
  }

  @Test
  public void testValidate_EmptyRunStatusPath_ThrowsException() {
    ImportConfig config = createValidConfig();
    config.setRunstatuspath("   ");
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for blank runstatuspath");
    } catch (IllegalArgumentException expected) {
      assertEquals("runstatuspath cannot be empty if specified", expected.getMessage());
    }

    config.setRunstatuspath(null);
    config.validate();
  }

  @Test
  public void testValidate_EmptySnapshots_ThrowsException() {
    ImportConfig config = createValidConfig();
    config.setSnapshots(new java.util.ArrayList<>());
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for empty snapshots");
    } catch (IllegalArgumentException expected) {
      assertEquals(
          "Snapshots must be specified. Allowed values are '*' (indicating all snapshots under"
              + " source path) or 'prefix*' (snapshots matching certain prefix) or"
              + " 'snapshotname1:tablename1,snapshotname2:tablename2' (comma seperated list of"
              + " snapshots)",
          expected.getMessage());
    }
  }

  @Test
  public void testValidate_EmptySnapshotNameOrTableName_ThrowsException() {
    ImportConfig config = createValidConfig();

    // Empty snapshot name
    java.util.List<ImportConfig.SnapshotInfo> invalidSnapshots = new java.util.ArrayList<>();
    invalidSnapshots.add(new ImportConfig.SnapshotInfo("", "table1"));
    config.setSnapshots(invalidSnapshots);
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for empty snapshot name");
    } catch (IllegalArgumentException expected) {
      assertEquals("snapshotName inside snapshots cannot be null or empty", expected.getMessage());
    }

    // Empty table name
    invalidSnapshots.clear();
    invalidSnapshots.add(new ImportConfig.SnapshotInfo("snapshot1", "  "));
    config.setSnapshots(invalidSnapshots);
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for empty table name");
    } catch (IllegalArgumentException expected) {
      assertEquals(
          "bigtableTableName inside snapshots cannot be null or empty", expected.getMessage());
    }
  }

  @Test
  public void testValidate_InvalidConfigurationKeysOrValues_ThrowsException() {
    ImportConfig config = createValidConfig();

    // Invalid hbaseConfiguration key
    java.util.Map<String, String> hbaseConfig = new java.util.HashMap<>();
    hbaseConfig.put("", "value");
    config.setHbaseConfiguration(hbaseConfig);
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for empty hbaseConfiguration key");
    } catch (IllegalArgumentException expected) {
      assertEquals("hbaseConfiguration keys cannot be null or empty", expected.getMessage());
    }

    // Invalid hbaseConfiguration value
    hbaseConfig.clear();
    hbaseConfig.put("key", null);
    config.setHbaseConfiguration(hbaseConfig);
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for null hbaseConfiguration value");
    } catch (IllegalArgumentException expected) {
      assertEquals("hbaseConfiguration values cannot be null", expected.getMessage());
    }

    // Reset hbaseConfig to valid
    config.setHbaseConfiguration(null);

    // Invalid bigtableConfiguration key
    java.util.Map<String, String> btConfig = new java.util.HashMap<>();
    btConfig.put(" ", "value");
    config.setBigtableConfiguration(btConfig);
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for empty bigtableConfiguration key");
    } catch (IllegalArgumentException expected) {
      assertEquals("bigtableConfiguration keys cannot be null or empty", expected.getMessage());
    }

    // Invalid bigtableConfiguration value
    btConfig.clear();
    btConfig.put("key", null);
    config.setBigtableConfiguration(btConfig);
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for null bigtableConfiguration value");
    } catch (IllegalArgumentException expected) {
      assertEquals("bigtableConfiguration values cannot be null", expected.getMessage());
    }
  }

  @Test
  public void testValidate_InvalidURIs_ThrowsException() {
    ImportConfig config = createValidConfig();

    // Invalid sourcepath URI
    config.setSourcepath("gs://bucket[with]invalidchars");
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for invalid sourcepath URI");
    } catch (IllegalArgumentException expected) {
      // Expected, URISyntaxException message
    }

    // Invalid restorepath URI
    config = createValidConfig();
    config.setRestorepath("gs://bucket[with]invalidchars");
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for invalid restorepath URI");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    // Invalid runstatuspath URI
    config = createValidConfig();
    config.setRunstatuspath("gs://bucket[with]invalidchars");
    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for invalid runstatuspath URI");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
  }

  @Test
  public void testValidate_AutoRestorepathLacksParent_ThrowsException() {
    ImportConfig config = createValidConfig();

    // gs:// scheme without object path (i.e. bucket root)
    config.setSourcepath("gs://my-bucket");
    config.setRestorepath(null);
    try {
      config.validate();
      fail(
          "Should have thrown IllegalArgumentException for gs bucket root sourcepath when"
              + " restorepath is null");
    } catch (IllegalArgumentException expected) {
      assertEquals(
          "sourcepath must have a parent directory to auto-generate restorepath: gs://my-bucket",
          expected.getMessage());
    }

    config.setSourcepath("gs://my-bucket/");
    try {
      config.validate();
      fail(
          "Should have thrown IllegalArgumentException for gs bucket root sourcepath (with slash)"
              + " when restorepath is null");
    } catch (IllegalArgumentException expected) {
      assertEquals(
          "sourcepath must have a parent directory to auto-generate restorepath: gs://my-bucket/",
          expected.getMessage());
    }

    // local relative path without parent
    config.setSourcepath("sourcedir");
    try {
      config.validate();
      fail(
          "Should have thrown IllegalArgumentException for relative path without parent when"
              + " restorepath is null");
    } catch (IllegalArgumentException expected) {
      assertEquals(
          "sourcepath must have a parent directory to auto-generate restorepath: sourcedir",
          expected.getMessage());
    }

    // local relative path with parent should succeed
    config.setSourcepath("parent/sourcedir");
    config.validate(); // Should pass
  }

  @Test
  public void testValidate_DuplicateSnapshotName_ThrowsException() {
    ImportConfig config = createValidConfig();
    java.util.List<ImportConfig.SnapshotInfo> snapshots = new java.util.ArrayList<>();
    snapshots.add(new ImportConfig.SnapshotInfo("snap1", "table1"));
    snapshots.add(new ImportConfig.SnapshotInfo("snap1", "table2"));
    config.setSnapshots(snapshots);

    try {
      config.validate();
      fail("Should have thrown IllegalArgumentException for duplicate snapshot name");
    } catch (IllegalArgumentException expected) {
      assertEquals("Duplicate snapshot name detected: snap1", expected.getMessage());
    }
  }

  @Test
  public void testValidate_NullSourcePath_ThrowsNullPointerException() {
    ImportConfig config = createValidConfig();
    config.setSourcepath(null);
    try {
      config.validate();
      fail("Should have thrown NullPointerException for null sourcepath");
    } catch (NullPointerException expected) {
      assertEquals(
          "Source Path containing hbase snapshots must be specified.", expected.getMessage());
    }
  }

  @Test
  public void testValidate_NullSnapshots_ThrowsNullPointerException() {
    ImportConfig config = createValidConfig();
    config.setSnapshots(null);
    try {
      config.validate();
      fail("Should have thrown NullPointerException for null snapshots");
    } catch (NullPointerException expected) {
      assertEquals(
          "Snapshots must be specified. Allowed values are '*' (indicating all snapshots under"
              + " source path) or 'prefix*' (snapshots matching certain prefix) or"
              + " 'snapshotname1:tablename1,snapshotname2:tablename2' (comma seperated list of"
              + " snapshots)",
          expected.getMessage());
    }
  }
}
