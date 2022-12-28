/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.beam;

import java.io.Serializable;
import java.nio.charset.CharacterCodingException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ParseFilter;

public class SerializableScan extends Scan implements Serializable {

  private final ValueProvider<String> start;
  private final ValueProvider<String> stop;
  private final ValueProvider<Integer> maxVersion;
  private final ValueProvider<String> filter;

  SerializableScan(
      ValueProvider<String> start,
      ValueProvider<String> stop,
      ValueProvider<Integer> maxVersion,
      ValueProvider<String> filter) {

    this.start = start;
    this.stop = stop;
    this.maxVersion = maxVersion;
    this.filter = filter;
  }

  public Scan toScan() {
    Scan scan = new Scan();
    // default scan version is 1. reset it to max
    scan.setMaxVersions(Integer.MAX_VALUE);
    if (start.get() != null && !start.get().isEmpty()) {
      scan.withStartRow(start.get().getBytes());
    }
    if (stop.get() != null && !stop.get().isEmpty()) {
      scan.withStopRow(stop.get().getBytes());
    }
    if (maxVersion.get() != null) {
      scan.setMaxVersions(maxVersion.get());
    }
    if (filter.get() != null && !filter.get().isEmpty()) {
      try {
        scan.setFilter(new ParseFilter().parseFilterString(filter.get()));
      } catch (CharacterCodingException e) {
        throw new RuntimeException(e);
      }
    }
    return scan;
  }
}
