package org.apache.hadoop.hbase.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Helper class to access package private state from HashTable.TableHash and increase testability of
 * classes using it.
 */
// TODO Move it to bigtable package, have an accessor with static methods only.
public class BigtableTableHashAccessor {

  private TableHash hash;

  private BigtableTableHashAccessor(TableHash hash) {
    this.hash = hash;
  }

  // TODO: For  mocking, check if we can live without it.
  public BigtableTableHashAccessor(){}

  public static BigtableTableHashAccessor create(SerializableConfiguration conf, String hashTableOutputDir) {
    BigtableTableHashAccessor tableHashAccessor;
    try {
      // TODO add more validations from SyncTable.
      tableHashAccessor = new BigtableTableHashAccessor(TableHash.read(conf.get(), new Path(hashTableOutputDir)));
    } catch (IOException e) {
      throw new RuntimeException("Failed to read HashTable's output via TableHash.read", e);
    }
    Preconditions.checkArgument(
        tableHashAccessor.hash.numHashFiles == (tableHashAccessor.hash.partitions.size() + 1),
        String.format(
            "Corrupt hashtable output. %d hash files for %d partitions. Expected %d files.",
            tableHashAccessor.hash.numHashFiles, tableHashAccessor.hash.partitions.size(), tableHashAccessor.hash.partitions.size() + 1));
    return tableHashAccessor;
  }

  public int getNumHashFiles(){
    return hash.numHashFiles;
  }

  public ImmutableList<ImmutableBytesWritable> getPartitions(){
    return ImmutableList.copyOf(hash.partitions);
  }

  public ImmutableBytesWritable getStartRow(){
    return new ImmutableBytesWritable(hash.startRow);
  }

  public ImmutableBytesWritable getStopRow(){
    return new ImmutableBytesWritable(hash.stopRow);
  }

  public HashTable.TableHash.Reader createReader(Configuration conf, ImmutableBytesWritable startRow){
    try {
      return hash.newReader(conf, startRow);
    } catch (IOException e) {
      throw new RuntimeException("Failed to open reader at " + Bytes.toStringBinary(startRow.copyBytes()), e);
    }
  }
}
