/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol;

import com.google.api.core.InternalApi;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.ClassSize;

@InternalApi("For internal usage only")
public class RequestResourcesDescription {
  public final int numberOfResults;
  public final long sizeInBytes;

  private static final long getObjectSize = ClassSize.estimateBase(Get.class, false);
  private static final long rowMutationObjectSize =
      ClassSize.estimateBase(RowMutations.class, false);

  protected RequestResourcesDescription(int numberOfResults, long sizeInBytes) {
    this.numberOfResults = numberOfResults;
    this.sizeInBytes = sizeInBytes;
  }

  public RequestResourcesDescription(boolean bool) {
    this(1, calculateSize(bool));
  }

  public RequestResourcesDescription(boolean[] array) {
    this(array.length, calculateSize(array));
  }

  public RequestResourcesDescription(Result result) {
    this(1, calculateSize(result));
  }

  public RequestResourcesDescription(Result[] array) {
    this(array.length, calculateSize(array));
  }

  public RequestResourcesDescription(Mutation operation) {
    this(1, calculateSize(operation));
  }

  public RequestResourcesDescription(RowMutations operation) {
    this(1, calculateSize(operation));
  }

  public RequestResourcesDescription(List<? extends Mutation> operation) {
    this(operation.size(), calculateSizeMutations(operation));
  }

  public RequestResourcesDescription(
      List<? extends Row> allSuccessfulOperations, Result[] successfulReadsResults) {
    this(
        allSuccessfulOperations.size(),
        calculateSize(successfulReadsResults) + calculateSize(allSuccessfulOperations));
  }

  private static long calculateSize(Result[] array) {
    long totalSize = 0;
    for (Result result : array) {
      if (result != null) {
        totalSize += Result.getTotalSizeOfCells(result);
      }
    }
    return totalSize;
  }

  private static long calculateSize(Result result) {
    if (result == null) {
      return 0;
    }
    return Result.getTotalSizeOfCells(result);
  }

  private static long calculateSize(boolean value) {
    // Not sure about this value - single boolean might be stored as a byte or as an integer,
    // however it won't be used extensively (only in Table#exists(Get)) and thus shouldn't cause any
    // problems.
    return ClassSize.align(1);
  }

  private static long calculateSize(boolean[] array) {
    // In Oracle’s Java Virtual Machine implementation, boolean arrays in the Java
    // programming language are encoded as Java Virtual Machine byte arrays, using 8 bits per
    // boolean element.
    // ~ The Java® Virtual Machine Specification, Java SE 7 Edition

    // Might be different in other VMs, but that would be surprising.
    return ClassSize.align(array.length);
  }

  private static long calculateSize(Mutation mutation) {
    return mutation.heapSize();
  }

  private static long calculateSize(Get operation) {
    // We have to count
    // - Object overhead
    // - All fields size and overhead
    // - familyTree internal structures overhead
    //     - for each entry:
    //        - key reference
    //        - key size
    //            - array overhead
    //            - array contents
    //        - value reference
    //        - navigable set of values
    //            - navigable set overhead
    //            - for each entry in set
    //                - entry reference
    //                - array overhead
    //                - array contents
    // We are doing our best to make this estimation close to reality, but still it is only a guess
    // and might be smaller than real size. We hope that family names and qualifier names dominate
    // the total size.
    long totalSize = 0;
    // getObjectSize counts size of Get fields
    // - Object overhead
    // - total size of primitives
    // - ClassSize.ARRAY for every array field
    // - ClassSize.REFERENCE for every reference field.
    totalSize += getObjectSize;
    // Length of row contents, array overhead was already counted in `getObjectSize`.
    totalSize += ClassSize.align(operation.getRow().length);
    // Overhead of map entries, reference overhead was already counted in `getObjectSize`.
    totalSize += ClassSize.align(operation.getFamilyMap().size() * ClassSize.MAP_ENTRY);
    // Overhead of internal map structures was not yet counted.
    totalSize += ClassSize.TREEMAP; // Implementation detail - in 1.x familyMap is a TreeMap.

    for (Entry<byte[], NavigableSet<byte[]>> entry : operation.getFamilyMap().entrySet()) {
      // key:
      // Assuming that reference size was counted in `MAP_ENTRY`.
      totalSize += ClassSize.align(ClassSize.ARRAY + entry.getKey().length);

      // values:
      // Assuming that size of reference to the set was counted in `MAP_ENTRY`.
      // Implementation detail - the NavigableSet is an interface over a TreeMap.
      totalSize += ClassSize.TREEMAP;
      // NavigableSet internally is a TreeMap, thus there are keys and values in it.
      // The values are references to the same sentinel object.
      // References to this object are counted in MAP_ENTRY, but the objects itself does not.
      // Thus we are adding a single REFERENCE for each Set.
      totalSize += ClassSize.REFERENCE;

      // For each qualifier, we keep a entry in the set.
      totalSize += ClassSize.align(ClassSize.MAP_ENTRY * entry.getValue().size());
      for (byte[] qualifier : entry.getValue()) {
        // And each qualifier is a separate array in memory.
        totalSize += ClassSize.align(ClassSize.ARRAY + qualifier.length);
      }
    }

    // Overhead of keeping Get reference in memory.
    totalSize += ClassSize.REFERENCE;
    return totalSize;
  }

  private static long calculateSize(RowMutations operation) {
    return rowMutationObjectSize // Object overhead, row array overhead, operations list reference.
        + ClassSize.align(operation.getRow().length) // Row contents.
        + ClassSize.ARRAYLIST // Operation list internal overhead.
        + calculateSizeMutations(operation.getMutations()) // Total size of operation elements.
        + ClassSize.REFERENCE; // Keeping this object in memory.
  }

  // Not using overload because it'd clash with calculateSize(List<? extends Row>) after erasure.
  private static long calculateSizeMutations(List<? extends Mutation> operations) {
    long totalSize = 0;
    for (Mutation mutation : operations) {
      totalSize += mutation.heapSize();
    }
    return totalSize;
  }

  private static long calculateSize(List<? extends Row> operation) {
    long totalSize = 0;
    for (Row row : operation) {
      if (row instanceof Mutation) {
        totalSize += calculateSize((Mutation) row);
      } else if (row instanceof RowMutations) {
        totalSize += calculateSize((RowMutations) row);
      } else if (row instanceof Get) {
        totalSize += calculateSize((Get) row);
      } else {
        throw new IllegalArgumentException(
            String.format("calculateSize expected Mutation, RowMutations or Get, not %s.", row));
      }
    }
    return totalSize;
  }
}
