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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration.MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration.MIRRORING_FLOW_CONTROLLER_STRATEGY_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration.MIRRORING_MISMATCH_DETECTOR_CLASS;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestCountingFlowControlStrategy;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.DefaultMismatchDetector;
import org.apache.hadoop.conf.Configuration;

@InternalApi("For internal use only")
public class MirroringOptions {
  public final String mismatchDetectorClass;
  public final String flowControllerStrategyClass;
  public final int flowControllerMaxOutstandingRequests;

  MirroringOptions(Configuration configuration) {
    this.mismatchDetectorClass =
        configuration.get(
            MIRRORING_MISMATCH_DETECTOR_CLASS, DefaultMismatchDetector.class.getCanonicalName());
    this.flowControllerStrategyClass =
        configuration.get(
            MIRRORING_FLOW_CONTROLLER_STRATEGY_CLASS,
            RequestCountingFlowControlStrategy.class.getCanonicalName());
    this.flowControllerMaxOutstandingRequests =
        Integer.parseInt(
            configuration.get(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "500"));
  }
}
