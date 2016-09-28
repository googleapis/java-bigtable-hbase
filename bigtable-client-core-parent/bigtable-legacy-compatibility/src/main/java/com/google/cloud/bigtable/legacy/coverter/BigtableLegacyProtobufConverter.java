/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.legacy.coverter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Converts protobuf objects between V1 and V2 instances, primarily for the sake of Dataflow. It
 * converts:
 * <ol>
 *   <li> V1 {@link com.google.bigtable.v1.Mutation} to V2 {@link com.google.bigtable.v2.Mutation}</li>
 *   <li> V2 {@link com.google.bigtable.v2.Row} to V1 {@link com.google.bigtable.v1.Row}</li>
 *   <li> V1 {@link com.google.bigtable.v1.RowFilter} to V2 {@link com.google.bigtable.v2.RowFilter} (TODO)</li>
 * </ol>
 * @author sduskis
 */
public class BigtableLegacyProtobufConverter {

  /**
   * Convert between v1 {@link com.google.bigtable.v1.Mutation} and v2 {@link com.google.bigtable.v2.Mutation}.
   * The conversion is pretty straight forward. The v1 and v2 Mutation objects have the same
   * structure, just different packages. Call {@link com.google.bigtable.v1.Mutation#toByteString()}
   * for v1, and {@link com.google.bigtable.v2.Mutation#parseFrom(ByteString)} for v2.
   *
   * @param v1Mutation the {@link com.google.bigtable.v1.Mutation} to convert.
   * @return @{link com.google.bigtable.v2.Mutation} of equivalent content to the v1Mutation
   * @throws InvalidProtocolBufferException
   */
  public static com.google.bigtable.v2.Mutation convert(com.google.bigtable.v1.Mutation v1Mutation)
      throws InvalidProtocolBufferException {
    return com.google.bigtable.v2.Mutation.parseFrom(v1Mutation.toByteString());
  }

  /**
   * Convert between v2 {@link com.google.bigtable.v2.Row} and v1 {@link com.google.bigtable.v1.Row}.
   * The conversion is pretty straight forward. The v1 and v2 Mutation objects have the same
   * structure, just different packages. Call {@link com.google.bigtable.v2.Row#toByteString()}
   * for v2, and {@link com.google.bigtable.v1.Row#parseFrom(ByteString)} for v1.
   *
   * @param v2Row the {@link com.google.bigtable.v2.Row} to convert.
   * @return @{link com.google.bigtable.v1.Row} of equivalent content to the v2Row.
   * @throws InvalidProtocolBufferException
   */
  public static com.google.bigtable.v1.Row convert(com.google.bigtable.v2.Row v2Row)
      throws InvalidProtocolBufferException {
    return com.google.bigtable.v1.Row.parseFrom(v2Row.toByteString());
  }

  /**
   * Convert between v1 {@link com.google.bigtable.v1.RowFilter} and v2
   * {@link com.google.bigtable.v2.RowFilter}. The conversion is a bit more complicated than the Row
   * and Mutation counterparts, since RowFilter is a more complicaed structure. Please raise a issue
   * if this is a problem.
   *
   * @param v1RowFilter the {@link com.google.bigtable.v1.RowFilter} to convert.
   * @return @{link com.google.bigtable.v2.RowFilter} of equivalent content to the v1RowFilter
   * @throws InvalidProtocolBufferException
   */
  public static com.google.bigtable.v2.RowFilter
      convert(com.google.bigtable.v1.RowFilter v1RowFilter) throws InvalidProtocolBufferException {
    return com.google.bigtable.v2.RowFilter.parseFrom(v1RowFilter.toByteString());
  }
}
