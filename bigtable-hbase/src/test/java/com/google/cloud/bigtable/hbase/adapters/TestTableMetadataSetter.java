package com.google.cloud.bigtable.hbase.adapters;

import org.apache.hadoop.hbase.TableName;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTableMetadataSetter {

  @Test
  public void testGoodHBaseName() {
    ClusterMetadataSetter clusterMetadataSetter =
        new ClusterMetadataSetter("some-project", "some-zone", "some-cluster");

    String bigtableTbleName =
        TableMetadataSetter.getName("some-project", "some-zone", "some-cluster",
          TableName.valueOf("some-table"));

    String hbaseName = clusterMetadataSetter.toHBaseTableName(bigtableTbleName);

    Assert.assertEquals("some-table", hbaseName);
  }
}
