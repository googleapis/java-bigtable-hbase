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
package com.google.cloud.bigtable.dataflowimport;

import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;

/**
 * An extension of {@link com.google.cloud.bigtable.dataflow.CloudBigtableOptions} that contains additional configuration
 * for importing HBase sequence files into Cloud Bigtable.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface HBaseImportOptions extends CloudBigtableOptions {
  /**
   * <p>getFilePattern.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  @Description("Location of the input file(s). For example, '/path_to_data_dir/part-m*'"
      + " refers to all local files in the specified directory that match the file name pattern, "
      + "and gs://gcp_bucket/path_to_data_dir/my_data_file.dat refers to a single data file.")
  String getFilePattern();

  /**
   * <p>setFilePattern.</p>
   *
   * @param filePattern a {@link java.lang.String} object.
   */
  void setFilePattern(String filePattern);

  /**
   * <p>isHBase094DataFormat.</p>
   *
   * @return a boolean.
   */
  @Description("Set to True to indicate that the file format is HBase 0.94 or earlier.")
  @Default.Boolean(false)
  boolean isHBase094DataFormat();

  /**
   * <p>setHBase094DataFormat.</p>
   *
   * @param value a boolean.
   */
  void setHBase094DataFormat(boolean value);
}
