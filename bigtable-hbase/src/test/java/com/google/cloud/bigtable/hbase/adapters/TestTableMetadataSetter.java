package com.google.cloud.bigtable.hbase.adapters;

import org.apache.hadoop.hbase.TableName;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTableMetadataSetter {

  ClusterMetadataSetter clusterMetadataSetter =
      new ClusterMetadataSetter("some-project", "some-zone", "some-cluster");

  String tableName = "some-table";
  TableMetadataSetter tableMetadataSetter =
      new TableMetadataSetter(TableName.valueOf(tableName), "some-project", "some-zone",
          "some-cluster");

  @Test
  public void testGoodHBaseName() {
    String hbaseName =
        clusterMetadataSetter.toHBaseTableName(tableMetadataSetter.getFormattedV1TableName());

    Assert.assertEquals(tableName, hbaseName);
  }
}
