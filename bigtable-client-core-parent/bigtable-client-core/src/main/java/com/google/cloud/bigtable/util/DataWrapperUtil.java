/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.util;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

/**
 * This class has utility methods for model conversion for
 * {@link com.google.cloud.bigtable.core.IBigtableDataClient}.
 */
public class DataWrapperUtil {

  /**
   * This method is referred from CheckAndMutateUtil#wasMutationApplied
   */
  public static boolean wasMutationApplied(CheckAndMutateRowRequest request,
      CheckAndMutateRowResponse response) {

    // If we have true mods, we want the predicate to have matched.
    // If we have false mods, we did not want the predicate to have matched.
    return (request.getTrueMutationsCount() > 0
        && response.getPredicateMatched()) || (
        request.getFalseMutationsCount() > 0
            && !response.getPredicateMatched());
  }

  /**
   * This method converts instances of {@link com.google.bigtable.v2.Row} to {@link Row}.
   *
   * @param row an instance of {@link com.google.bigtable.v2.Row} type.
   * @return {@link Row} an instance of {@link Row}.
   */
  public static Row convert(com.google.bigtable.v2.Row row) {
    ImmutableList.Builder<RowCell> rowCells  = new ImmutableList.Builder<>();
    for (Family family : row.getFamiliesList()) {
      String familyName = family.getName();
      for (Column column : family.getColumnsList()) {
        ByteString qualifier = column.getQualifier();
        for (Cell cell : column.getCellsList()) {
          rowCells.add(RowCell.create(familyName, qualifier, cell.getTimestampMicros(),
              cell.getLabelsList(), cell.getValue()));
        }
      }
    }
    return Row.create(row.getKey(), rowCells.build());
  }
}
