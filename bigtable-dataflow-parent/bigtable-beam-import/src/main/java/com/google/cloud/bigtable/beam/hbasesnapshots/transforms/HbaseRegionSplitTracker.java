package com.google.cloud.bigtable.beam.hbasesnapshots.transforms;

import com.google.api.core.InternalApi;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RestrictionTracker} wrapping the {@link ByteKeyRangeTracker} for controlled execution of
 * dynamic splitting.
 */
@InternalApi("For internal usage only")
public class HbaseRegionSplitTracker extends RestrictionTracker<ByteKeyRange, ByteKey>
    implements RestrictionTracker.HasProgress {

  private static final Logger LOG = LoggerFactory.getLogger(HbaseRegionSplitTracker.class);

  private final String snapshotName;

  private final String regionName;
  private final ByteKeyRangeTracker byteKeyRangeTracker;

  private final boolean enableDynamicSplitting;

  public HbaseRegionSplitTracker(
      String snapshotName, String regionName, ByteKeyRange range, boolean enableDynamicSplitting) {
    this.snapshotName = snapshotName;
    this.regionName = regionName;
    this.byteKeyRangeTracker = ByteKeyRangeTracker.of(range);
    this.enableDynamicSplitting = enableDynamicSplitting;
  }

  public ByteKeyRange currentRestriction() {
    return this.byteKeyRangeTracker.currentRestriction();
  }

  public SplitResult<ByteKeyRange> trySplit(double fractionOfRemainder) {
    LOG.info(
        "Splitting restriction for region:{} in snapshot:{}", this.regionName, this.snapshotName);

    return enableDynamicSplitting ? this.byteKeyRangeTracker.trySplit(fractionOfRemainder) : null;
  }

  public boolean tryClaim(ByteKey key) {
    return this.byteKeyRangeTracker.tryClaim(key);
  }

  public void checkDone() throws IllegalStateException {
    this.byteKeyRangeTracker.checkDone();
  }

  public RestrictionTracker.IsBounded isBounded() {
    return this.byteKeyRangeTracker.isBounded();
  }

  public String toString() {
    return this.byteKeyRangeTracker.toString();
  }

  @Override
  public Progress getProgress() {
    return this.byteKeyRangeTracker.getProgress();
  }
}
