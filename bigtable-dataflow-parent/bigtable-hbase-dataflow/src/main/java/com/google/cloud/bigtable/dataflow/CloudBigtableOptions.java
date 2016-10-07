/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
 * options to configure a Dataflow pipeline that uses Cloud Bigtable.
 */
@Description("Options used to configure a Dataflow pipeline that uses Cloud Bigtable. " +
   "See https://cloud.google.com/bigtable/ for more information. " +
   "See https://cloud.google.com/bigtable/docs/creating-cluster for getting started with Cloud Bigtable.")
public interface CloudBigtableOptions extends DataflowPipelineOptions {

  @Description("The Google Cloud project ID for the Cloud Bigtable cluster.")
  String getBigtableProjectId();

  void setBigtableProjectId(String bigtableProjectId);

  @Description("The Google Cloud Bigtable instance ID .")
  String getBigtableInstanceId();

  void setBigtableInstanceId(String bigtableInstanceId);

  @Description("The Cloud Bigtable cluster ID.")
  String getBigtableClusterId();

  void setBigtableClusterId(String bigtableClusterId);

  @Description("The Google Cloud zone ID in which the cluster resides.")
  String getBigtableZoneId();

  void setBigtableZoneId(String bigtableZoneId);

  @Description("The Cloud Bigtable table ID in the cluster." )
  String getBigtableTableId();

  void setBigtableTableId(String bigtableTableId);
}
