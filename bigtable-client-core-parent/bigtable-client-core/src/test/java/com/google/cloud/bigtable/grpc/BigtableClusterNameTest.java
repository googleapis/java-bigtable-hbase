package com.google.cloud.bigtable.grpc;

import org.junit.Assert;
import org.junit.Test;

public class BigtableClusterNameTest {

  @Test
  public void getInstanceId() throws Exception {
    String clusterName = "projects/proj/instances/inst/clusters/cluster";
    Assert.assertEquals("inst", new BigtableClusterName(clusterName).getInstanceId());
  }

}