package com.google.cloud.bigtable.hbase;

import com.google.bigtable.repackaged.com.google.api.gax.rpc.ClientContext;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import com.google.cloud.bigtable.hbase1_x.BigtableConnection;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(KnownHBaseGap.class)
public class TestCachedConnections extends AbstractTest {

  private byte[] rowName = dataHelper.randomData("testrow-");
  private byte[] columnQualifier = dataHelper.randomData("qual-");
  private byte[] columnValue = dataHelper.randomData("value-");

  @Before
  public void setup() throws IOException {
    Table table = getDefaultTable();

    table.put(
        new Put(rowName)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, columnQualifier, 100000L, columnValue));
  }

  @After
  public void teardown() throws IOException {
    Table table = getDefaultTable();

    table.delete(new Delete(rowName));
  }

  @Test
  public void testConnectionsAreCaching() throws IOException {
    String[] connectionEndpoints = {"bigtable.googleapis.com", "bigtable2.googleapis.com"};

    Configuration configuration = sharedTestEnv.getConfiguration();
    configuration.set(BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, "true");

    HashMap<String, ClientContext> context = getContext(configuration, connectionEndpoints[0]);
    Assert.assertEquals(context.size(), 1);
    Assert.assertTrue(context.containsKey(connectionEndpoints[0]));
    ClientContext Context_0 = context.get(connectionEndpoints[0]);

    context = getContext(configuration, connectionEndpoints[1]);
    Assert.assertEquals(context.size(), 2);
    Assert.assertTrue(context.containsKey(connectionEndpoints[1]));
    Assert.assertEquals(Context_0, context.get(connectionEndpoints[0]));
    ClientContext Context_1 = context.get(connectionEndpoints[1]);

    context = getContext(configuration, connectionEndpoints[1]);
    Assert.assertEquals(context.size(), 2);
    Assert.assertEquals(Context_1, context.get(connectionEndpoints[1]));
    Assert.assertEquals(Context_0, context.get(connectionEndpoints[0]));

    context = getContext(configuration, connectionEndpoints[0]);
    Assert.assertEquals(context.size(), 2);
    Assert.assertEquals(Context_1, context.get(connectionEndpoints[1]));
    Assert.assertEquals(Context_0, context.get(connectionEndpoints[0]));
  }

  private HashMap<String, ClientContext> getContext(Configuration configuration, String endpoint)
      throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, endpoint);
    BigtableConnection connection =
        (BigtableConnection) BigtableConfiguration.connect(configuration);
    checkRows(connection);
    return connection.getSession().getGCJClientContext();
  }

  private void checkRows(BigtableConnection connection) throws IOException {
    Table table = connection.getTable(sharedTestEnv.getDefaultTableName());
    ResultScanner scanner = table.getScanner(new Scan());
    Result row = scanner.next();
    while (row != null) {
      row = scanner.next();
    }
  }
}
