package com.google.cloud.bigtable.hbase;

import com.google.api.client.util.Strings;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestAppend.class,
    TestBasicOps.class,
    TestBatch.class,
    TestBufferedMutator.class,
    TestCheckAndMutate.class,
    TestColumnFamilyAdmin.class,
    TestCreateTable.class,
    TestDisableTable.class,
    TestDelete.class,
    TestDurability.class,
    TestFilters.class,
    TestGet.class,
    TestGetTable.class,
    TestScan.class,
    TestIncrement.class,
    TestListTables.class,
    TestPut.class,
    TestTimestamp.class,
    TestImport.class
})
public class IntegrationTests {

  public static final TableName TABLE_NAME;
  public static final int MAX_VERSIONS = 6;
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
  public static final Configuration BASE_CONFIGURATION = HBaseConfiguration.create();

  // testingUtility, connection, and configuration are provided via the connectionResource later
  protected static HBaseTestingUtility testingUtility;
  protected static Configuration configuration;

  static {
    TABLE_NAME = newTestTableName();
    addExtraResources(BASE_CONFIGURATION);
  }

  public static TableName newTestTableName() {
    return TableName.valueOf("test_table-" + UUID.randomUUID().toString());
  }

  protected static void addExtraResources(Configuration configuration) {
    String extraResources = System.getProperty("bigtable.test.extra.resources");
    if (extraResources != null) {
      InputStream resourceStream =
          AbstractTest.class.getClassLoader().getResourceAsStream(extraResources);
      if (resourceStream != null) {
        configuration.addResource(resourceStream);
      }
    }
  }

  protected static boolean useMiniCluster() {
    return Strings.isNullOrEmpty(
        BASE_CONFIGURATION.get(HConnection.HBASE_CLIENT_CONNECTION_IMPL, ""));
  }

  protected static boolean isBigtable() {
    return !useMiniCluster();
  }

  public static MiniDFSCluster getMiniCluster() {
    return testingUtility.getDFSCluster();
  }

  public static Configuration getConfiguration() {
    // If the class rule hasn't run, we probably haven't created our table yet either.
    Preconditions.checkState(configuration != null,
        "Integration test class rule not run, configuration not initialized.");
    return configuration;
  }

  @ClassRule
  public static Timeout timeoutRule = new Timeout((int) TimeUnit.MINUTES.toMillis(5));

  public static void createTable(Admin admin, TableName tableName) throws IOException {
    HColumnDescriptor hcd = new HColumnDescriptor(COLUMN_FAMILY).setMaxVersions(MAX_VERSIONS);
    admin.createTable(new HTableDescriptor(tableName).addFamily(hcd));
  }

  @ClassRule
  public static ExternalResource connectionResource = new ExternalResource() {

    @Override
    protected void before() throws Throwable {
      if (useMiniCluster()) {
        testingUtility = new HBaseTestingUtility();
        testingUtility.startMiniCluster(1);
        setConfiguration(testingUtility.getConfiguration());
      } else {
        setConfiguration(BASE_CONFIGURATION);
      }
      connection = ConnectionFactory.createConnection(configuration);
      admin = connection.getAdmin();
      createTable(admin, TABLE_NAME);
    }

    @Override
    protected void after() {
      try {
        admin.disableTable(TABLE_NAME);
        admin.deleteTable(TABLE_NAME);
        if (useMiniCluster()) {
          testingUtility.shutdownMiniCluster();
        }
        admin.close();
        connection.close();
      } catch (Exception e) {
        // shutdownMiniCluster throws Exception, while getAdmin and others throw IOException.
        // Both result in the same desired outcome here.
        throw new RuntimeException("Error shutting down test cluster", e);
      }
    }
  };

  private static Connection connection;
  private static Admin admin;

  public static Connection getConnection() {
    return connection;
  }

  public static Admin getAdmin() {
    return admin;
  }

  public static void setConfiguration(Configuration configuration) {
    IntegrationTests.configuration = configuration;
  }
}
