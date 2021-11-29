/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.bigtable.hbase1_x;

import com.google.api.core.InternalApi;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CommonConnection;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;

/**
 * This is an hbase 1.x implementation of {@link AbstractBigtableAdmin}. Most methods in this class
 * unsupported
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
@SuppressWarnings("deprecation")
public abstract class BigtableAdmin extends AbstractBigtableAdmin {

  public BigtableAdmin(CommonConnection connection) throws IOException {
    super(connection);
  }

  /** {@inheritDoc} */
  @Override
  public MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    snapshotTable(snapshot.getName(), TableName.valueOf(snapshot.getTable()));
    LOG.warn(
        "isSnapshotFinished() is not currently supported by BigtableAdmin.\n"
            + "You may poll for existence of the snapshot with listSnapshots(snapshotName)");
    return MasterProtos.SnapshotResponse.newBuilder()
        .setExpectedTimeout(TimeUnit.MINUTES.toMillis(5))
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public void disableTableAsync(TableName tableName) throws IOException {
    disableTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public void enableTableAsync(TableName tableName) throws IOException {
    enableTable(tableName);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The snapshot will be created with the ttl configured by {@link
   * com.google.cloud.bigtable.hbase.BigtableOptionsFactory#BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_KEY}
   * key in the configuration. If not configured, the ttl will be set to serverside default.
   */
  @Override
  public void snapshot(
      String snapshotId, TableName tableName, HBaseProtos.SnapshotDescription.Type type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotId, tableName);
  }

  /** {@inheritDoc} */
  @Override
  public void snapshot(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshot.getName(), TableName.valueOf(snapshot.getTable()));
  }

  @Override
  public void cloneSnapshot(String s, TableName tableName, boolean b)
      throws IOException, TableExistsException, RestoreSnapshotException {
    if (!b) {
      this.cloneSnapshot(s, tableName);
    }
    throw new UnsupportedOperationException("cloneSnapshot"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException {
    List<String> backups =
        Futures.getChecked(
            adminClientWrapper.listBackupsAsync(getBackupClusterId()), IOException.class);

    List<HBaseProtos.SnapshotDescription> response = new ArrayList<>();

    for (String snapshot : backups) {
      response.add(HBaseProtos.SnapshotDescription.newBuilder().setName(snapshot).build());
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(String regex) throws IOException {
    return listSnapshots(Pattern.compile(regex));
  }

  /** {@inheritDoc} */
  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    if (pattern == null || pattern.matcher("").matches()) {
      return ImmutableList.of();
    }

    List<HBaseProtos.SnapshotDescription> response = new ArrayList<>();
    for (HBaseProtos.SnapshotDescription description : listSnapshots()) {
      if (pattern.matcher(description.getName()).matches()) {
        response.add(description);
      }
    }
    return response;
  }

  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    return new ClusterStatus() {
      @Override
      public Collection<ServerName> getServers() {
        return Collections.emptyList();
      }
    };
  }

  private static Class<? extends BigtableAdmin> adminClass = null;

  /**
   * This is a workaround for incompatible changes in hbase minor versions. Dynamically generates a
   * class that extends BigtableAdmin so incompatible methods won't be accessed unless the methods
   * are called. If a method is implemented by BigtableAdmin, the generated class will invoke the
   * implementation in BigtableAdmin. Otherwise it'll throw {@link UnsupportedOperationException}.
   */
  private static synchronized Class<? extends BigtableAdmin> getSubclass() {
    if (adminClass == null) {
      adminClass =
          new ByteBuddy()
              .subclass(BigtableAdmin.class)
              .method(ElementMatchers.isAbstract())
              .intercept(
                  InvocationHandlerAdapter.of(
                      new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
              .make()
              .load(BigtableAdmin.class.getClassLoader())
              .getLoaded();
    }
    return adminClass;
  }

  public static Admin createInstance(CommonConnection connection) throws IOException {
    try {
      return getSubclass().getDeclaredConstructor(CommonConnection.class).newInstance(connection);
    } catch (InvocationTargetException e) {
      // Unwrap and throw IOException or RuntimeException as is, and convert all other exceptions to
      // IOException because
      // org.apache.hadoop.hbase.client.Connection#getAdmin() only throws
      // IOException
      Throwables.throwIfInstanceOf(e.getTargetException(), IOException.class);
      Throwables.throwIfInstanceOf(e.getTargetException(), RuntimeException.class);
      throw new IOException(e);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
