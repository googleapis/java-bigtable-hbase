package com.google.cloud.bigtable.beam.hbasesnapshots.conf;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.beam.hbasesnapshots.coders.RegionConfigCoder;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;

/**
 * A {@link AutoValue} class representing the region configuration enclosing {@link SnapshotConfig},
 * hbase region info and hbase table descriptor.
 */
@DefaultCoder(RegionConfigCoder.class)
@AutoValue
public abstract class RegionConfig {
  public static Builder builder() {
    return new AutoValue_RegionConfig.Builder();
  }

  @Nullable
  public abstract String getName();

  public abstract SnapshotConfig getSnapshotConfig();

  public abstract RegionInfo getRegionInfo();

  public abstract TableDescriptor getTableDescriptor();

  public abstract Long getRegionSize();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setName(String value);

    public abstract Builder setSnapshotConfig(SnapshotConfig value);

    public abstract Builder setRegionInfo(RegionInfo value);

    public abstract Builder setTableDescriptor(TableDescriptor value);

    public abstract Builder setRegionSize(Long value);

    public abstract RegionConfig build();
  }
}
