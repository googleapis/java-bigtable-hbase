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
package com.google.cloud.bigtable.dataflow;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;


/**
 * CloudBigtableOptions is an extension of {@link DataflowPipelineOptions} containing the
 * information required for a connection to Cloud Bigtable.
 */
@Description("Options used to configure CloudBigtable.  " +
   "see https://cloud.google.com/bigtable/ for more information.  " +
   "See https://cloud.google.com/bigtable/docs/creating-cluster for getting started with Bigtable.")
public interface CloudBigtableOptions extends DataflowPipelineOptions {

  @Description("The Google Cloud projectId for the Cloud Bigtable cluster.")
  String getBigtableProjectId();

  void setBigtableProjectId(String bigtableProjectId);

  @Description("The Cloud Bigtable cluster id.")
  String getBigtableClusterId();

  void setBigtableClusterId(String bigtableClusterId);

  @Description("The Google Cloud zoneId in which the cluster resides.")
  String getBigtableZoneId();

  void setBigtableZoneId(String bigtableZoneId);

  @Description("Optional - The id of the Cloud Bigtable table." )
  String getBigtableTableId();

  void setBigtableTableId(String bigtableTableId);
}
