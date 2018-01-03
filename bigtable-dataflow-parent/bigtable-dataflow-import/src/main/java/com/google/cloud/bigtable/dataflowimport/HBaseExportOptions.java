/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
import com.google.cloud.dataflow.sdk.options.Validation.Required;

/**
 * An extension of {@link com.google.cloud.bigtable.dataflow.CloudBigtableOptions} that contains additional configuration
 * for exporting Bigtable tables into GCS as sequence files.
 *
 * @author igorbernstein2
 * @version $Id: $Id
 */
public interface HBaseExportOptions extends CloudBigtableOptions {

  @Description("Directory where to export the sequence files to")
  @Required
  String getDestination();

  void setDestination(String path);

  @Description("The row where to start the export from")
  @Default.String("")
  String getStartRow();

  void setStartRow(String startRow);

  @Description("The row where to stop the export")
  @Default.String("")
  String getStopRow();

  void setStopRow(String stopRow);

  @Description("Maximum number of cell versions")
  @Default.Integer(Integer.MAX_VALUE)
  int getMaxVersions();

  void setMaxVersions(int maxVersions);

  @Description("Filter string. See: http://hbase.apache.org/0.94/book/thrift.html#thrift.filter-language")
  @Default.String("")
  String getFilter();

  void setFilter(String filter);
}
