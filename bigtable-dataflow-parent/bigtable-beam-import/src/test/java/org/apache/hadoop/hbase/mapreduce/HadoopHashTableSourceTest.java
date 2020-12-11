package org.apache.hadoop.hbase.mapreduce;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.List;
import junit.framework.TestCase;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.KeyBasedHashTableSource;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.RangeHash;
import org.mockito.Mockito;

public class HadoopHashTableSourceTest extends TestCase {

  HadoopHashTableSource source;
  BigtableTableHashAccessor mockBigtableTableHashAccessor;
  SerializableConfiguration conf;

  public static final String HASH_TABLE_OUTPUT_PATH_DIR = "gs://my-bucket/outputDir";
  private static final ImmutableBytesWritable START_ROW = new ImmutableBytesWritable(
      "a".getBytes());
  private static final ImmutableBytesWritable STOP_ROW = new ImmutableBytesWritable("z".getBytes());
  private static final ImmutableBytesWritable PARTITION1 = new ImmutableBytesWritable(
      "d".getBytes());
  private static final ImmutableBytesWritable PARTITION2 = new ImmutableBytesWritable(
      "g".getBytes());
  private static final ImmutableBytesWritable PARTITION3 = new ImmutableBytesWritable(
      "k".getBytes());


  public void setUp() throws Exception {
    super.setUp();
    conf = new SerializableConfiguration();
    source = new HadoopHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR);
    mockBigtableTableHashAccessor = Mockito.mock(BigtableTableHashAccessor.class);
    // Mock the supplier to inject a MockTableHashHelper.
    source.tableHashHelperSupplier = () -> {
      return mockBigtableTableHashAccessor;
    };
  }

  public void tearDown() throws Exception {

  }

  public void testSplitHappyCase() {
    // Row range [a-z) with splits on {d,g}. The data files will be for {[a,d), [d,g), [g,z)}.
    when(mockBigtableTableHashAccessor.getNumHashFiles()).thenReturn(3);
    when(mockBigtableTableHashAccessor.getPartitions())
        .thenReturn(ImmutableList.of(PARTITION1, PARTITION2));
    when(mockBigtableTableHashAccessor.getStartRow()).thenReturn(START_ROW);
    when(mockBigtableTableHashAccessor.getStopRow()).thenReturn(STOP_ROW);

    List<? extends BoundedSource<RangeHash>> actual = source.split(1, null);

    List<BoundedSource<RangeHash>> expected = ImmutableList.of(
        new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, PARTITION1),
        new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
        new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, STOP_ROW));
    assertTrue(Iterables.elementsEqual(expected, actual));
    assertEquals(expected, actual);
    verify(mockBigtableTableHashAccessor);
  }

  public void testCreateReader() {
  }
}