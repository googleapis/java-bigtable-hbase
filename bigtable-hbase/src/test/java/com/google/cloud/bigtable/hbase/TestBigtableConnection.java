package com.google.cloud.bigtable.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;

import com.google.cloud.bigtable.config.BigtableOptions;

public class TestBigtableConnection extends AbstractBigtableConnection {

  public TestBigtableConnection(Configuration conf, BigtableOptions options) throws IOException {
    super(conf, options);
  }

  @Override
  public Admin getAdmin() throws IOException {
    return new AbstractBigtableAdmin(getOptions(), getConfiguration(), this,
        getBigtableTableAdminClient(), getDisabledTables()) {
    };
  }

}
