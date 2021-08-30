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
package com.google.cloud.bigtable.mapreduce.hbasesnapshots;

public class ImportJobCommon {

  public static final String SNAPSHOTNAME_KEY = "google.bigtable.import.snapshot.name";

  public static final String TABLENAME_KEY = "google.bigtable.import.bigtable.tablename";

  public static final String SNAPSHOT_RESTOREDIR_KEY =
      "google.bigtable.import.snapshot.restore.dir";

  public static final String IMPORT_SNAPSHOT_JOBNAME_KEY = "google.bigtable.import.jobname";

  public static final String SNAPSHOT_SPLITS_PER_REGION_KEY =
      "google.bigtable.import.snapshot.splits.per.region";

  public static final int SNAPSHOT_SPLITS_PER_REGION_DEFAULT = 2;
}
