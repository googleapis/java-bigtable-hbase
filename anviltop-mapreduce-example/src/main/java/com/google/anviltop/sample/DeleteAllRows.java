package com.google.anviltop.sample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class DeleteAllRows {
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 1) {
      System.err.println("Usage: delete-rows <table-name>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "delete rows");
    TableMapReduceUtil.initTableMapperJob(args[0], new Scan(),
      null, null, null, job);
  }
}
