package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestKeyOnlyFilterAdapter {

  KeyOnlyFilterAdapter filterAdapter = new KeyOnlyFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan);

  @Test
  public void stripValuesIsApplied() throws IOException {
    KeyOnlyFilter filter = new KeyOnlyFilter();
    RowFilter rowFilter = filterAdapter.adapt(emptyScanContext, filter);
    Assert.assertTrue(rowFilter.getStripValueTransformer());
  }

  @Test
  public void lengthAsValIsNotSupported() {
    KeyOnlyFilter filter = new KeyOnlyFilter(true);
    Assert.assertFalse(
        filterAdapter.isFilterSupported(emptyScanContext, filter).isSupported());
  }
}
