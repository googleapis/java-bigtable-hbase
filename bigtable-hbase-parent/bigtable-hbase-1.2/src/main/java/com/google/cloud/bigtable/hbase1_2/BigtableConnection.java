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
package com.google.cloud.bigtable.hbase1_2;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.security.User;

/**
 * HBase 1.2 specific implementation of {@link org.apache.hadoop.hbase.client.AbstractBigtableConnection}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableConnection extends AbstractBigtableConnection {

  /**
   * <p>Constructor for BigtableConnection.</p>
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object.
   * @throws java.io.IOException if any.
   */
  public BigtableConnection(Configuration conf) throws IOException {
    super(conf);
  }

  BigtableConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws IOException {
    super(conf, managed, pool, user);
  }

  /** {@inheritDoc} */
  @Override
  public Admin getAdmin() throws IOException {
    return new AbstractBigtableAdmin(getOptions(), getConfiguration(), this,
        getBigtableTableAdminClient(), getDisabledTables()) {

          @Override
          public boolean isBalancerEnabled() throws IOException {
            throw new UnsupportedOperationException("isBalancerEnabled");  // TODO
          }

          @Override
          public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
            throw new UnsupportedOperationException("getLastMajorCompactionTimestamp");  // TODO
          }

          @Override
          public long getLastMajorCompactionTimestampForRegion(byte[] regionName)
              throws IOException {
            throw new UnsupportedOperationException("getLastMajorCompactionTimestampForRegion");  // TODO
          }

          @Override
          public void setQuota(QuotaSettings quota) throws IOException {
            throw new UnsupportedOperationException("setQuota");  // TODO
          }

          @Override
          public QuotaRetriever getQuotaRetriever(QuotaFilter filter) throws IOException {
            throw new UnsupportedOperationException("getQuotaRetriever");  // TODO
          }

          @Override
          public boolean normalize() throws IOException {
            throw new UnsupportedOperationException("normalize");  // TODO
          }

          @Override
          public boolean isNormalizerEnabled() throws IOException {
            throw new UnsupportedOperationException("isNormalizerEnabled");  // TODO
          }

          @Override
          public boolean setNormalizerRunning(boolean on) throws IOException {
            throw new UnsupportedOperationException("setNormalizerRunning");  // TODO
          }

          @Override
          public boolean abortProcedure(long procId, boolean mayInterruptIfRunning)
              throws IOException {
            throw new UnsupportedOperationException("abortProcedure");  // TODO
          }

          @Override
          public ProcedureInfo[] listProcedures() throws IOException {
            throw new UnsupportedOperationException("listProcedures");  // TODO
          }

          @Override
          public Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning)
              throws IOException {
            throw new UnsupportedOperationException("abortProcedureAsync");  // TODO
          }

          @Override
          public List<SecurityCapability> getSecurityCapabilities() throws IOException {
            throw new UnsupportedOperationException("getSecurityCapabilities");  // TODO
          }
    };
  }
}
