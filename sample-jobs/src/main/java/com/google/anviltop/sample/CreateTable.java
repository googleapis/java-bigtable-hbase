package com.google.anviltop.sample;

import java.io.IOException;
import java.security.SecureRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Writes the results of WordCount to a new table
 * @author sduskis
 */
public class CreateTable {

  public static final String PROJECT_ID_KEY = "google.anviltop.project.id";

  public static final byte[] CF = "cf".getBytes();
  public static final byte[] COUNT = "count".getBytes();


  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 1) {
      System.err.println("Usage: <table-name>");
      System.exit(2);
    }

    TableName tableName = TableName.valueOf(otherArgs[otherArgs.length - 1]);
    createTable(tableName, conf);
  }

  public static void createTable(TableName tableName, Configuration conf) throws IOException {
    Connection connection = null;
    Admin admin = null;

    try {
      DebugUtil.printSystemProperties();
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
      // TODO: re-add this once admin.isTableAvailable() is implemented in anviltop
//      if (!admin.isTableAvailable(tableName)) {
      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      tableDescriptor.addFamily(new HColumnDescriptor(CF));
      admin.createTable(tableDescriptor);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // TODO: remove the try/catch once admin.close() doesn't throw exceptions anymore.
      try {
        if (admin != null) admin.close();
      } catch(Exception e) {
        e.printStackTrace();
      }
      if (connection != null) connection.close();
    }
  }
}
