package com.google.cloud.bigtable.hbase.util;

import org.apache.hadoop.hbase.HConstants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class TestTimestampConverter {

  @Test
  public void testNow(){
    long nowMs = System.currentTimeMillis();
    long nowMicros = TimeUnit.MILLISECONDS.toMicros(nowMs);

    Assert.assertEquals(nowMs, TimestampConverter.bigtable2hbase(nowMicros));
    Assert.assertEquals(nowMicros, TimestampConverter.hbase2bigtable(nowMs));
  }

  @Test
  public void testLarge(){
    Assert.assertEquals(HConstants.LATEST_TIMESTAMP,
        TimestampConverter.bigtable2hbase(Long.MAX_VALUE));
    Assert.assertEquals(TimestampConverter.BIGTABLE_MAX_TIMESTAMP,
        TimestampConverter.hbase2bigtable(Long.MAX_VALUE));
  }
}
