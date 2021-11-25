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

/**
 * Integration tests for 1.x MirroringClient.
 *
 * <p>Tests can be run with any combination of Bigtable and HBase as a primary or secondary
 * database. They rely solely on the HBase Connection interface and test the behaviour of
 * MirroringClient, not any particular underlying implementation. The only exception from that rule
 * are tests that test the behaviour of MirroringClient in the presence of database errors (more on
 * this later) - those tests can be only run in a specific environment and are skipped when they
 * cannot be performed. Tests connect to actual database instances - either a locally hosted HBase,
 * in memory MiniCluster (automatically started by ITs when {@code use-hbase-mini-cluster} system
 * property is set to true), local Bigtable emulator (remember to set appropriate {@code
 * BIGTABLE_EMULATOR_HOST} environment variable for java-bigtable-hbase client) or remote HBase or
 * Bigtable clusters.
 *
 * <p>ITs create mirroring connection and its components in the same way as the user using this
 * library would do it - by reading an XML file with configuration (and might modify or add some
 * values to test specific cases). {@code resources} directory contains two configuration files -
 * one that configures the MirroringConnection to use HBase as a primary database and Bigtable as a
 * secondary, the second file configures it the other way. Path to configuration XML file should be
 * provided in {@code integration-tests-config-file-name} system property.
 *
 * <p>To test cases when one of the databases fails to perform an operation, can only be tested when
 * one of the tested databases is a in-memory HBase MiniCluster instance that is automatically
 * started by ITs. We use custom HBase {@link org.apache.hadoop.hbase.regionserver.Region}
 * implementation ({@link
 * com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion}) and
 * inject it into the MiniCluster to reject some of operations.
 *
 * <p>ITs can be run using Maven and one of test profiles defined in {@code
 * bigtable-hbase-mirroring-client-1.x-integration-tests/pom.xml} file - {@code
 * HBaseToBigtableLocalIntegrationTests} and {@code BigtableToHBaseLocalIntegrationTests}. They use
 * in-memory HBase MiniCluster and automatically start local Bigtable emulator. {@code mvn compile
 * verify -Penable-integration-tests,HBaseToBigtableLocalIntegrationTests} command can be run to run
 * ITs using maven.
 *
 * <p>Those tests are also used to verify 2.x synchronous {@link
 * com.google.cloud.bigtable.mirroring.hbase2_x.MirroringConnection}. Because there are small
 * differences between 1.x and 2.x tools used by ITs a compatibility layer have been introduced to
 * keep tests consistent and specific implementations of those layers are selected by setting a
 * appropriate system property ({@code integrations.compat.table-creator-impl}, {@code
 * integrations.compat.failingregion.impl}), their correct values can be found in {@code pom.xml}
 * files of appropriate integration test modules.
 *
 * <p>Integration tests are integrated with Prometheus for metrics and Zipkin for tracing. Setting
 * {@code PROMETHEUS_SERVER_PORT} environment variable will start Prometheus server (configured by
 * resources/prometheus.yml). Setting {@code ZIPKIN_API_URL} environment variable ({@code
 * host:port}) will enable tracing reporting to Zipkin server (see {@link ZipkinTracingRule} for
 * details).
 */
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
