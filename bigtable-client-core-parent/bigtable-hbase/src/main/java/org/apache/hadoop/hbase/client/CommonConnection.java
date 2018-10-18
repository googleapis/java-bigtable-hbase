package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;

public interface CommonConnection {

  /**
   * returns an instance of BigtableSession
   *
   * @return {@link BigtableSession}
   */
  BigtableSession getSession() throws IOException;

  /**
   * return current connection's configuration
   *
   * @return {@link Configuration}
   */
  Configuration getConfiguration() throws IOException;

  /**
   * returns an instance of Options object
   *
   * @return {@link BigtableOptions}
   */
  BigtableOptions getOptions() throws IOException;

  /**
   * returns a collection of disabled TableName
   * @return Set<TableName>
   */
  Set<TableName> getDisabledTables() throws IOException;

}
