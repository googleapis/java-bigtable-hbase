package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestFirstKeyOnlyFilterAdapter {

  FirstKeyOnlyFilterAdapter adapter = new FirstKeyOnlyFilterAdapter();

  @Test
  public void onlyTheFirstKeyFromEachRowIsEmitted() throws IOException {
    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(new Scan()), new FirstKeyOnlyFilter());
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setCellsPerRowLimitFilter(1)
            .build(),
        adaptedFilter);
  }
}
