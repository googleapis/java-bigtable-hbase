package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static org.junit.Assert.assertEquals;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.Assert;
import org.junit.Test;
import com.google.cloud.bigtable.hbase.AbstractTest;
import com.google.cloud.bigtable.hbase.Logger;

/**
 * @author spollapally
 */
public class TestAsyncAdmin extends AbstractTest {
  private final Logger LOG = new Logger(getClass());
  
  @Test
  public void testAsyncConnection() throws Exception {
    AsyncConnection asyncCon = getAsyncConnection();
    Assert.assertNotNull("async connection should not be null", asyncCon);
    
    AsyncAdmin asycAdmin = asyncCon.getAdmin();
    Assert.assertNotNull("asycAdmin should not be null", asycAdmin);
  }
  
  @Test
  public void testCreateTable() throws Exception {
    AsyncAdmin asycAdmin = getAsyncConnection().getAdmin();

    TableName tableName = TableName.valueOf("TestTable" + UUID.randomUUID().toString());
    try {
      asycAdmin.createTable(TableDescriptorBuilder.newBuilder(tableName)
          .addColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY)).build()).get();
    } finally {
      try {
        asycAdmin.disableTable(tableName).get();
        asycAdmin.deleteTable(tableName).get();
      } catch (Throwable t) {
        LOG.warn("Error cleaning up the table", t);
      }
    }
  }
  
  @Test
  public void testTableExist() throws Exception {
    AsyncAdmin asycAdmin = getAsyncConnection().getAdmin();
    boolean exist;

    TableName tableName = TableName.valueOf("TestTable" + UUID.randomUUID().toString());
    try {
      exist = asycAdmin.tableExists(tableName).get();
      assertEquals(false, exist);
      asycAdmin.createTable(TableDescriptorBuilder.newBuilder(tableName)
          .addColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY)).build()).get();
      exist = asycAdmin.tableExists(tableName).get();
      assertEquals(true, exist);
    } finally {
      try {
        asycAdmin.disableTable(tableName).get();
        asycAdmin.deleteTable(tableName).get();
      } catch (Throwable t) {
        LOG.warn("Error cleaning up the table", t);
      }
    }
  }
  
  @Test
  public void testDeleteExist() throws Exception {
    AsyncAdmin asycAdmin = getAsyncConnection().getAdmin();
    boolean exist;
    
    TableName tableName = TableName.valueOf("TestTable" + UUID.randomUUID().toString());
    try {
    asycAdmin.createTable(TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY)).build()).get(1, TimeUnit.SECONDS);
    exist = asycAdmin.tableExists(tableName).get();
    assertEquals(true, exist);
    asycAdmin.deleteTable(tableName).get();
    exist = asycAdmin.tableExists(tableName).get();
    assertEquals(false, exist);
    } finally {
      try {
        asycAdmin.disableTable(tableName).get();
        asycAdmin.deleteTable(tableName).get();
      } catch (Throwable t) {
        LOG.warn("Error cleaning up the table", t);
      }
    }

  }

}
