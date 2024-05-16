/*
 * Copyright 2024 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.ImportConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test cases for the {@link SnapshotUtils} class. */
@RunWith(JUnit4.class)
public class SnapshotUtilsTest {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotUtilsTest.class);

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();
  // Preferred way to instantiate mocks in JUnit4 is via the JUnit rule MockitoJUnit
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock GcsUtil gcsUtilMock;
  @Mock Objects gcsObjects;

  @Test
  public void testRemoveSuffixSlashIfExists() {
    String path = "gs://bucket/prefix";

    assertThat(SnapshotUtils.removeSuffixSlashIfExists(path), is(path));
    assertThat(SnapshotUtils.removeSuffixSlashIfExists(path + "/"), is(path));
  }

  @Test
  public void testAppendCurrentTimestamp() {
    String path = "gs://bucket/prefix";
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneId.of("UTC"));
    long currentTime = Long.parseLong(formatter.format(Instant.now()));
    long returnTime =
        Long.parseLong(SnapshotUtils.appendCurrentTimestamp(path).replace(path + "/", ""));
    assertThat((returnTime - currentTime), lessThan(2L));
  }

  @Test
  public void testgetNamedDirectory() {
    String path = "gs://bucket/subdir1";
    String subFolder = "subdir2";
    String expectedPath = "gs://bucket/subdir2";
    String retValue = SnapshotUtils.getNamedDirectory(path, subFolder);
    assertThat(retValue.startsWith(expectedPath), is(true));
  }

  @Test
  public void testGetConfigurationWithDataflowRunner() {
    String projectId = "testproject";
    Map<String, String> configurations =
        SnapshotUtils.getConfiguration("DataflowRunner", projectId, "/path/to/sourcedir", null);
    assertThat(configurations.get("fs.gs.project.id"), is(projectId));
    assertThat(configurations.get("s.gs.auth.type"), nullValue());
  }

  @Test
  public void testGetConfigurationWithDirectRunner() {
    Map<String, String> hbaseConfiguration =
        SnapshotTestHelper.buildMapFromList(
            new String[] {"fs.AbstractFileSystem.gs.impl", "org.apache.hadoop.fs.hdfs"});
    Map<String, String> configurations =
        SnapshotUtils.getConfiguration(
            "DirectRunner", "testproject", "/path/to/sourcedir", hbaseConfiguration);
    assertThat(
        configurations.get("fs.AbstractFileSystem.gs.impl"),
        is(hbaseConfiguration.get("fs.AbstractFileSystem.gs.impl")));
    assertThat(configurations.get("fs.gs.auth.type"), is("APPLICATION_DEFAULT"));
  }

  @Test
  public void testGetHbaseConfiguration() {
    Map<String, String> configurations =
        SnapshotTestHelper.buildMapFromList(
            new String[] {"throttling.enable", "true", "throttling.threshold.ms", "200"});
    Configuration hbaseConfiguration = SnapshotUtils.getHBaseConfiguration(configurations);
    assertThat(hbaseConfiguration.getBoolean("throttling.enable", false), is(true));
    assertThat(hbaseConfiguration.get("throttling.threshold.ms"), is("200"));
  }

  @Test
  public void testBuildSnapshotConfigs() {
    String projectId = "testproject";
    String sourcePath = "/path/to/sourcedir";
    String restorePath = "/path/to/restoredir";
    List<ImportConfig.SnapshotInfo> snapshotInfoList =
        Arrays.asList(
            new ImportConfig.SnapshotInfo("snapdemo", "btdemo"),
            new ImportConfig.SnapshotInfo("bookcontent-9087", "bookcontent"));

    Map<String, String> conbfiguration =
        SnapshotTestHelper.buildMapFromList(
            new String[] {"bigtable.row.size", "100", "bigtable.auth.type", "private"});

    List<SnapshotConfig> snapshotConfigs =
        SnapshotUtils.buildSnapshotConfigs(
            snapshotInfoList, new HashMap<>(), projectId, sourcePath, restorePath);

    assertThat(snapshotConfigs.size(), is(2));
    assertThat(snapshotConfigs.get(0).getProjectId(), is(projectId));
    assertThat(snapshotConfigs.get(0).getSnapshotName(), is("snapdemo"));
    assertThat(snapshotConfigs.get(1).getSourceLocation(), is(sourcePath));
    assertThat(snapshotConfigs.get(1).getTableName(), is("bookcontent"));
  }

  @Test
  public void testGetSnapshotsFromStringReturnsSameTableName() {
    String snapshotsWithBigtableTableName = "bookmark-2099";
    Map<String, String> snapshots =
        SnapshotUtils.getSnapshotsFromString(snapshotsWithBigtableTableName);
    assertThat(snapshots.size(), is(equalTo(1)));
    assertThat(snapshots.get("bookmark-2099"), is("bookmark-2099"));
  }

  @Test
  public void testGetSnapshotsFromStringReturnsMultipleTables() {
    String snapshotsWithBigtableTableName = "snapshot1,snapshot2,snapshot3:mytable3,snapshot4";
    Map<String, String> snapshots =
        SnapshotUtils.getSnapshotsFromString(snapshotsWithBigtableTableName);
    assertThat(snapshots.size(), is(equalTo(4)));
    assertThat(snapshots.get("snapshot1"), is("snapshot1"));
    assertThat(snapshots.get("snapshot2"), is("snapshot2"));
    assertThat(snapshots.get("snapshot3"), is("mytable3"));
    assertThat(snapshots.get("snapshot4"), is("snapshot4"));
  }

  @Test
  public void testGetSnapshotsFromStringReturnsParsedValues() {
    String snapshotsWithBigtableTableName =
        "bookmark-2099:bookmark,malwarescanstate-9087:malwarescan";
    Map<String, String> snapshots =
        SnapshotUtils.getSnapshotsFromString(snapshotsWithBigtableTableName);
    assertThat(snapshots.size(), is(equalTo(2)));
    assertThat(snapshots.get("malwarescanstate-9087"), is("malwarescan"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSnapshotsFromStringThrowsException() {
    String snapshotsWithBigtableTableName =
        "bookmark-2099:bookmark,malwarescanstate-9087:malwarescan:snapdemo1";
    Map<String, String> snapshots =
        SnapshotUtils.getSnapshotsFromString(snapshotsWithBigtableTableName);
  }

  private void setUpGcsObjectMocks(List<StorageObject> fakeStorageObjects) throws IOException {
    Mockito.when(gcsObjects.getItems()).thenReturn(fakeStorageObjects);
    Mockito.when(gcsUtilMock.listObjects(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
        .thenReturn(gcsObjects);
  }

  private Map<String, String> getMatchingSnapshotsFromSnapshotPath(
      List<String> snapshotList, String prefix) throws IOException {
    String baseObjectPath = "snapshots/20220309230526";
    String importSnapshotpath = String.format("gs://sym-bucket/%s", baseObjectPath);
    List<StorageObject> fakeStorageObjects =
        SnapshotTestHelper.createFakeStorageObjects(baseObjectPath, snapshotList);
    setUpGcsObjectMocks(fakeStorageObjects);
    return SnapshotUtils.getSnapshotsFromSnapshotPath(importSnapshotpath, gcsUtilMock, prefix);
  }

  @Test
  public void testgetAllSnapshotsFromSnapshotPath() throws IOException {
    List<String> snapshotList = Arrays.asList("audit-events", "dlpInfo", "ce-metrics-manifest");
    Map<String, String> snapshots = getMatchingSnapshotsFromSnapshotPath(snapshotList, "*");
    assertThat(snapshots.size(), is(equalTo(3)));
    assertThat(snapshots.keySet(), containsInAnyOrder(snapshotList.toArray(new String[0])));
  }

  @Test
  public void testgetSubSetSnapshotsFromSnapshotPath() throws IOException {
    List<String> snapshotList =
        Arrays.asList(
            "audit-events",
            "symphony-attachments",
            "ce-metrics-manifest",
            "symphony-attachments-streams");
    Map<String, String> snapshots =
        getMatchingSnapshotsFromSnapshotPath(snapshotList, ".*attachments.*");
    List<String> expectedResult =
        snapshotList.stream().filter(e -> e.contains("attachments")).collect(Collectors.toList());
    // LOG.info("Matched:{} and expected:{}", snapshots.size(), expectedResult.size());
    assertThat(snapshots.size(), is(equalTo(expectedResult.size())));
    assertThat(snapshots.keySet(), containsInAnyOrder(expectedResult.toArray(new String[0])));
  }

  @Test(expected = IllegalStateException.class)
  public void testgetSubSetSnapshotsFromSnapshotPathThrowsException() throws IOException {
    Map<String, String> snapshots = getMatchingSnapshotsFromSnapshotPath(null, "*");
  }
}
