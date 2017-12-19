package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by garyelliott on 3/27/17.
 */
public class TestReplicationAppProfile extends AbstractTest {
  @Test
  public void testAppProfiles() throws IOException {
    Configuration configuration = sharedTestEnv.getConnection().getConfiguration();
    configuration.set("google.bigtable.appprofile.id", configuration.get("google.bigtable.replication.cluster.id"));
    Connection goodConnection = ConnectionFactory.createConnection(configuration);
    configuration.set("google.bigtable.appprofile.id", "junk");
    Connection badConnection = ConnectionFactory.createConnection(configuration);

    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = dataHelper.randomData("testValue-");

    Table goodTable = goodConnection.getTable(sharedTestEnv.getDefaultTableName());
    Table badTable = badConnection.getTable(sharedTestEnv.getDefaultTableName());

    // Put good
    Put put = new Put(rowKey);
    put.addColumn(sharedTestEnv.COLUMN_FAMILY, testQualifier, testValue);
    goodTable.put(put);

    // Put
    put = new Put(rowKey);
    put.addColumn(sharedTestEnv.COLUMN_FAMILY, testQualifier, testValue);
    try {
      badTable.put(put);
      Assert.fail("No exception for bad app profile");
    } catch (Exception expected) {}

    // Get
    Get get = new Get(rowKey);
    get.addColumn(sharedTestEnv.COLUMN_FAMILY, testQualifier);
    try {
      badTable.get(get);
      Assert.fail("No exception for bad app profile");
    } catch (Exception expected) {}

    // Delete
    Delete delete = new Delete(rowKey);
    delete.addColumns(sharedTestEnv.COLUMN_FAMILY, testQualifier);
    try {
      badTable.delete(delete);
    } catch (Exception expected) {}


    badTable.close();
    goodTable.close();
  }
}
