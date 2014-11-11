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
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

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
    String projectId = conf.get(PROJECT_ID_KEY);
    if (projectId != null) {
      SecureRandom random = new SecureRandom();
      conf.set(PROJECT_ID_KEY, projectId + "-" + random.nextInt());
    }

    TableName tableName = TableName.valueOf(otherArgs[otherArgs.length - 1]);
    System.out.println("Creating " + tableName);
    Logger.getRootLogger().getLoggerRepository().resetConfiguration();
    ConsoleAppender console = new ConsoleAppender(); //create appender
    //configure the appender
    String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN)); 
    console.setThreshold(Level.TRACE);
    console.activateOptions();
    //add appender to any Logger (here is root)
    Logger.getRootLogger().addAppender(console);

    System.out.println("testing");
    Logger.getLogger(CreateTable.class).debug("Did this work?");

    createTable(tableName, conf);
    System.out.println("Created");
  }

  public static void createTable(TableName tableName, Configuration conf) throws IOException {
    Connection connection = null;
    Admin admin = null;

    try {
      DebugUtil.printSystemProperties();
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
//      if (!admin.isTableAvailable(tableName)) {
      // wait for the table to be created
      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      tableDescriptor.addFamily(new HColumnDescriptor(CF));
      admin.createTable(tableDescriptor);
      System.err.println("Woohoo!  Could create table: " + tableName);
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      // ignore
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Yikes!  Could not create table: " + tableName);
    } finally {
      try {
        if (admin != null) admin.close();
      } catch(Exception e) {
        e.printStackTrace();
      }
      if (connection != null) connection.close();
    }
  }
}
