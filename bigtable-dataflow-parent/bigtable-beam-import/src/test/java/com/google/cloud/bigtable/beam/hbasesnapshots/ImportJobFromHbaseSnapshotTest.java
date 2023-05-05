package com.google.cloud.bigtable.beam.hbasesnapshots;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.ImportConfig;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test cases for the {@link ImportJobFromHbaseSnapshot} class. */
@RunWith(JUnit4.class)
public class ImportJobFromHbaseSnapshotTest {
  private static final Logger LOG = LoggerFactory.getLogger(ImportJobFromHbaseSnapshotTest.class);

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock GcsOptions gcsOptions;
  @Mock GcsUtil gcsUtilMock;
  @Mock Objects gcsObjects;

  @Test
  public void testBuildImportConfigWithMissingSourcePathThrowsException() throws Exception {
    ImportJobFromHbaseSnapshot.ImportOptions options =
        SnapshotTestHelper.getPipelineOptions(
            new String[] {
              "--snapshots='bookmark-2099:bookmark,malwarescanstate-9087:malwarescan'"
            });

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(ImportJobFromHbaseSnapshot.MISSING_SNAPSHOT_SOURCEPATH);
    ImportJobFromHbaseSnapshot.buildImportConfigFromPipelineOptions(options, gcsOptions);
  }

  @Test
  public void testBuildImportConfigWithMissingSnapshotsThrowsException() throws Exception {
    ImportJobFromHbaseSnapshot.ImportOptions options =
        SnapshotTestHelper.getPipelineOptions(
            new String[] {"--hbaseSnapshotSourceDir=gs://bucket/data/"});

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(ImportJobFromHbaseSnapshot.MISSING_SNAPSHOT_NAMES);
    ImportJobFromHbaseSnapshot.buildImportConfigFromPipelineOptions(options, gcsOptions);
  }

  @Test
  public void testBuildImportConfigFromSnapshotsString() throws Exception {
    String sourcePath = "gs://bucket/data/";
    ImportJobFromHbaseSnapshot.ImportOptions options =
        SnapshotTestHelper.getPipelineOptions(
            new String[] {
              "--hbaseSnapshotSourceDir=" + sourcePath,
              "--snapshots='bookmark-2099:bookmark,malwarescanstate-9087:malwarescan'"
            });

    ImportConfig importConfig =
        ImportJobFromHbaseSnapshot.buildImportConfigFromPipelineOptions(options, gcsOptions);
    assertThat(importConfig.getSourcepath(), is(sourcePath));
    assertThat(importConfig.getRestorepath(), notNullValue());
    assertThat(importConfig.getSnapshots().size(), is(2));
  }

  private void setUpGcsObjectMocks(List<StorageObject> fakeStorageObjects) throws Exception {
    Mockito.when(gcsObjects.getItems()).thenReturn(fakeStorageObjects);
    Mockito.when(gcsUtilMock.listObjects(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
        .thenReturn(gcsObjects);
  }

  @Test
  public void testBuildImportConfigForAllSnapshots() throws Exception {
    String baseObjectPath = "snapshots/20220309230526";
    String importSnapshotpath = String.format("gs://sym-bucket/%s", baseObjectPath);
    ImportJobFromHbaseSnapshot.ImportOptions options =
        SnapshotTestHelper.getPipelineOptions(
            new String[] {"--hbaseSnapshotSourceDir=" + importSnapshotpath, "--snapshots=*"});
    Mockito.when(gcsOptions.getGcsUtil()).thenReturn(gcsUtilMock);

    List<String> snapshotList = Arrays.asList("audit-events", "dlpInfo", "ce-metrics-manifest");
    List<StorageObject> fakeStorageObjects =
        SnapshotTestHelper.createFakeStorageObjects(baseObjectPath, snapshotList);
    setUpGcsObjectMocks(fakeStorageObjects);

    ImportConfig importConfig =
        ImportJobFromHbaseSnapshot.buildImportConfigFromPipelineOptions(options, gcsOptions);
    assertThat(importConfig.getSourcepath(), is(importSnapshotpath));
    assertThat(importConfig.getRestorepath(), notNullValue());
    assertThat(importConfig.getSnapshots().size(), is(snapshotList.size()));
  }

  @Test
  public void testBuildImportConfigFromJsonFileWithMissingPathThrowsException() throws Exception {
    String config =
        "{\n"
            + "  \"snapshots\": {\n"
            + "    \"snap_demo1\": \"snap_demo1\",\n"
            + "    \"snap_demo2\": \"snap_demo2\"\n"
            + "  }\n"
            + "}";
    File file = tempFolder.newFile();
    SnapshotTestHelper.writeToFile(file.getAbsolutePath(), config);
    ImportJobFromHbaseSnapshot.ImportOptions options =
        SnapshotTestHelper.getPipelineOptions(
            new String[] {"--importConfigFilePath=" + file.getAbsolutePath()});

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage(ImportJobFromHbaseSnapshot.MISSING_SNAPSHOT_SOURCEPATH);

    ImportConfig importConfig =
        ImportJobFromHbaseSnapshot.buildImportConfigFromConfigFile(
            options.getImportConfigFilePath());
  }

  @Test
  public void testBuildImportConfigFromJsonFile() throws Exception {
    String importSnapshotpath = "gs://sym-datastore/snapshots/data/snap_demo";
    String restoreSnapshotpath = "gs://sym-datastore/snapshots/data/restore";
    String config =
        String.format(
            "{\n"
                + "  \"sourcepath\": \"%s\",\n"
                + "  \"restorepath\": \"%s\",\n"
                + "  \"snapshots\": {\n"
                + "    \"snap_demo1\": \"demo1\",\n"
                + "    \"snap_demo2\": \"demo2\"\n"
                + "  }\n"
                + "}",
            importSnapshotpath, restoreSnapshotpath);

    File file = tempFolder.newFile();
    SnapshotTestHelper.writeToFile(file.getAbsolutePath(), config);
    ImportJobFromHbaseSnapshot.ImportOptions options =
        SnapshotTestHelper.getPipelineOptions(
            new String[] {"--importConfigFilePath=" + file.getAbsolutePath()});
    ImportConfig importConfig =
        ImportJobFromHbaseSnapshot.buildImportConfigFromConfigFile(
            options.getImportConfigFilePath());
    assertThat(importConfig.getSourcepath(), is(importSnapshotpath));
    assertThat(importConfig.getRestorepath().startsWith(restoreSnapshotpath), is(true));
    assertThat(importConfig.getSnapshots().get(0).getbigtableTableName(), is("demo1"));
  }
}
