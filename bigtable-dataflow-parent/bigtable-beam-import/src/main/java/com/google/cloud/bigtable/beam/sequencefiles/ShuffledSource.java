package com.google.cloud.bigtable.beam.sequencefiles;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;

/**
 * Wrapper that will randomize the ordering of initial splits.
 *
 * This useful when reading sorted files to import into Bigtable. It will allow Bigtable
 * to create initial split quicker.
 */
class ShuffledSource<T> extends BoundedSource<T> {
  private final BoundedSource<T> delegate;

  /**
   * Constructs the wrapper.
   *
   * @param delegate The actual source, whose splits to randomize
   */
  ShuffledSource(BoundedSource<T> delegate) {
    this.delegate = delegate;
  }

  /**
   * Shuffles the delegate source's splits.
   */
  @Override
  public List<? extends BoundedSource<T>> split(long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {

    List<? extends BoundedSource<T>> splits = delegate.split(desiredBundleSizeBytes, options);
    Collections.shuffle(splits);
    return splits;
  }

  /** {@inheritDoc} */
  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return delegate.getEstimatedSizeBytes(options);
  }

  /** {@inheritDoc} */
  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    return delegate.createReader(options);
  }

  /** {@inheritDoc} */
  @Override
  public void validate() {
    delegate.validate();
  }

  /** {@inheritDoc} */
  @Override
  public Coder<T> getDefaultOutputCoder() {
    return delegate.getDefaultOutputCoder();
  }

  /** {@inheritDoc} */
  @Override
  public void populateDisplayData(Builder builder) {
    delegate.populateDisplayData(builder);
  }
}
