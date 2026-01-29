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
package com.google.cloud.bigtable.hbase2_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.security.User;

/**
 * HBase 2.x specific implementation of {@link AbstractBigtableConnection}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableConnection extends AbstractBigtableConnection {

  /**
   * Constructor for BigtableConnection.
   *
   * @param conf a {@link Configuration} object.
   * @throws IOException if any.
   */
  public BigtableConnection(Configuration conf) throws IOException {
    super(conf);
  }

  public BigtableConnection(Configuration conf, ExecutorService pool, User user)
      throws IOException {
    super(conf, false, pool, user);
  }

  public BigtableConnection(
      Configuration conf, ExecutorService pool, User user, Map<String, byte[]> connectionAttributes)
      throws IOException {
    super(conf, false, pool, user);
  }

  /**
   * Due to hbase 1.x to 2.x binary incompatibilities. {@link
   * HRegionLocation#HRegionLocation(org.apache.hadoop.hbase.client.RegionInfo, ServerName)} will
   * fail with NoSuchMethodException if not recompiled with hbase 2.0 dependencies. Hence the
   * override. See {@link SampledRowKeysAdapter} for more details.
   */
  @Override
  protected SampledRowKeysAdapter createSampledRowKeysAdapter(
      TableName tableName, ServerName serverName) {
    return new SampledRowKeysAdapter(tableName, serverName) {
      @Override
      protected HRegionLocation createRegionLocation(byte[] startKey, byte[] endKey) {
        RegionInfo regionInfo =
            RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey).build();
        return new HRegionLocation(regionInfo, serverName);
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public Admin getAdmin() throws IOException {
    return BigtableAdmin.createInstance(this);
  }

  /** {@inheritDoc} */
  @Override
  public TableBuilder getTableBuilder(final TableName tableName, final ExecutorService pool) {
    return new TableBuilder() {

      @Override
      public TableBuilder setWriteRpcTimeout(int arg0) {
        return this;
      }

      @Override
      public TableBuilder setRpcTimeout(int arg0) {
        return this;
      }

      @Override
      public TableBuilder setReadRpcTimeout(int arg0) {
        return this;
      }

      @Override
      public TableBuilder setOperationTimeout(int arg0) {
        return this;
      }

      @Override
      public TableBuilder setRequestAttribute(String s, byte[] bytes) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public Table build() {
        try {
          return getTable(tableName, pool);
        } catch (IOException e) {
          throw new RuntimeException("Could not create the table", e);
        }
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public Table getTable(TableName tableName, ExecutorService ignored) throws IOException {
    return BigtableTable.create(this, createAdapter(tableName));
  }

  @Override
  public void clearRegionLocationCache() {
    throw new UnsupportedOperationException("clearRegionLocationCache");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.CommonConnection#getAllRegionInfos(org.apache.hadoop.hbase.TableName)
   */
  @Override
  public List<HRegionInfo> getAllRegionInfos(TableName tableName) throws IOException {
    List<HRegionInfo> regionInfos = new CopyOnWriteArrayList<>();
    for (HRegionLocation location : getRegionLocator(tableName).getAllRegionLocations()) {
      regionInfos.add(location.getRegionInfo());
    }
    return regionInfos;
  }

  @Override
  public Hbck getHbck() throws IOException {
    return createUnsupportedProxy(Hbck.class);
  }

  @Override
  public Hbck getHbck(ServerName masterServer) throws IOException {
    return createUnsupportedProxy(Hbck.class);
  }

  /**
   * Create a dynamic proxy for the passed in interface.
   *
   * <p>The proxy will implement all methods by throwing an {@link UnsupportedOperationException}.
   * This is useful for deferring the exception being thrown. For example hbase shell will acquire a
   * reference toHbck, but wont actually use it. So it would be useful to allow hbase shell to
   * initialize and defer the UnsupportOperationExcetion on Hbck is actually invoked.
   */
  private <T> T createUnsupportedProxy(Class<T> cls) {
    @SuppressWarnings("unchecked")
    T proxy =
        (T)
            Proxy.newProxyInstance(
                cls.getClassLoader(),
                new Class<?>[] {cls},
                new UnsupportedInvocationHandler("Unsupported" + cls.getSimpleName()));

    return proxy;
  }

  private static class UnsupportedInvocationHandler implements InvocationHandler {
    private final String name;

    private UnsupportedInvocationHandler(String name) {
      this.name = name;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      int argCount = (args == null) ? 0 : args.length;

      if (argCount == 1
          && method.getName().equals("equals")
          && method.getParameterTypes()[0] == Object.class) {
        Object arg = args[0];
        if (arg == null) {
          return false;
        }
        return proxy == arg;
      }
      if (argCount == 0 && method.getName().equals("hashCode")) {
        return super.hashCode();
      }
      if (argCount == 0 && method.getName().equals("toString")) {
        return name;
      }

      throw new UnsupportedOperationException(name + "." + method.getName());
    }
  }
}
