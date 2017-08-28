/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
