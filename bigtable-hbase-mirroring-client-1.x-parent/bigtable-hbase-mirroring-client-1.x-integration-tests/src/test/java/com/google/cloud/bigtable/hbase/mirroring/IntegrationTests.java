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
package com.google.cloud.bigtable.hbase.mirroring;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.PrometheusStatsCollectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.ZipkinTracingRule;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  TestErrorDetection.class,
  TestBlocking.class,
  TestBufferedMutator.class,
  TestMirroringTable.class,
  TestReadVerificationSampling.class,
})
public class IntegrationTests {
  // Classes in test suites should use their own ConnectionRule, the one here serves to keep a
  // single HBase MiniCluster connection up for all tests (if one is needed).
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();
  @ClassRule public static ZipkinTracingRule zipkinTracingRule = new ZipkinTracingRule();

  @ClassRule
  public static PrometheusStatsCollectionRule prometheusStatsCollectionRule =
      new PrometheusStatsCollectionRule();
}
