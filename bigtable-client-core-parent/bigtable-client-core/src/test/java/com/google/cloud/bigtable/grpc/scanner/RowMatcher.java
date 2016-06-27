/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.scanner;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.protobuf.ByteString;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code RowMatcher} provides a convenient Matcher-based language for evaluating
 * {@link Row} test conditions.  Example usage:
 *
 *   import static com.google.cloud.bigtable.grpc.scanner.RowMatcher.matchesRow;
 *
 *   assertThat(row,
 *       matchesRow("row key").withFamily("Family").withColumn("column").withAnyCell());
 */
public class RowMatcher extends TypeSafeMatcher<Row> {
  /**
   * Creates a new row matcher using the provided matcher to evaluate the
   * row key.
   *
   * @param rowKey Matcher to use for the row key.
   * @return A new {@code RowMatcher} object.
   */
  public static RowMatcher matchesRow(Matcher<ByteString> rowKey) {
    return new RowMatcher(rowKey);
  }

  /**
   * A convenience method for constructing a new row matcher with key equal
   * to the UTF-8 bytes for the given string.
   *
   * @see #matchesRow(Matcher)
   */
  public static RowMatcher matchesRow(String rowKey) {
    return new RowMatcher(CoreMatchers.equalTo(ByteString.copyFromUtf8(rowKey)));
  }

  /**
   * Adds a new column family with the given name to the expected value for
   * this row.  Subsequent {@link #withColumn(Matcher)} calls will be added
   * to the most recently added family.
   *
   * Usage:
   *   {@code matchesRow("key").withFamily("family")}
   */
  public RowMatcher withFamily(String familyName) {
    lastFamily = new FamilyMatcher(familyName);
    families.put(familyName, lastFamily);
    return this;
  }

  /**
   * Adds a column with qualifier matching the given matcher to the current
   * column family.  A column family <em>must</em> have already been added
   * to the expected row with {@link #withFamily(String)} prior to calling
   * {@code withColumn}.
   *
   * Example:
   *   {@code withFamily("family").withColumn(any(ByteString.class))}
   */
  public RowMatcher withColumn(Matcher<ByteString> columnName) {
    lastColumn = new ColumnMatcher(columnName);
    lastFamily.addColumn(lastColumn);
    return this;
  }

  /**
   * A convenience method for column names equal to the UTF-8 bytes for the
   * given string.
   *
   * @see #withColumn(Matcher)
   */
  public RowMatcher withColumn(String columnName) {
    return this.withColumn(CoreMatchers.equalTo(ByteString.copyFromUtf8(columnName)));
  }

  /**
   * Adds an expectation that the most recently added column will contain an
   * arbitrary cell.  This may be repeated multiple times if multiple cells
   * are expected in the column.
   *
   * It is not legal to combine {@code withAnyCell} and
   * {@link #atTimestamp(long)}.  To match a cell with arbitrary contents but
   * a specific timestamp, use
   * {@code withCellValue(any(ByteString.class)).atTimestamp(time)} instead.
   *
   * A family and column must have already been added to the expected row
   * prior to calling this method.
   */
  public RowMatcher withAnyCell() {
    lastColumn.addCell(CoreMatchers.any(Cell.class));
    // withAnyCell().atTimestamp() is illegal.  Cause a NPE if somebody tries.
    // Use withCellValue(any(ByteString.class)).atTimestamp() instead.
    lastCell = null;
    return this;
  }

  /**
   * Adds an expectation that the most recently added column will contain a
   * cell matching the provided matcher and have timestamp 0 (or no timestamp
   * set).  The expected timestamp can be changed by chaining this method
   * with {@link #atTimestamp(long)}.
   *
   * A family and column must have already been added to the expected row
   * prior to calling this method.
   */
  public RowMatcher withCellValue(Matcher<ByteString> value) {
    lastCell = new CellMatcher(value, CoreMatchers.equalTo(0L));
    lastColumn.addCell(lastCell);
    return this;
  }

  /**
   * A convenience method for cell values equal to the UTF-8 bytes of the
   * given string.
   *
   * @see #withCellValue(Matcher)
   */
  public RowMatcher withCellValue(String value) {
    return this.withCellValue(CoreMatchers.equalTo(ByteString.copyFromUtf8(value)));
  }

  /**
   * Sets the expected timestamp for the most recently added cell.  This
   * should immediately follow {@code withCellValue()}.  It is not legal to
   * combine this method with {@link #withAnyCell()}.
   */
  public RowMatcher atTimestamp(long timestamp) {
    lastCell.setTimestamp(timestamp);
    return this;
  }

  /**
   * A matcher for the Bigtable {@code Cell} protocol buffer.
   */
  static class CellMatcher extends TypeSafeMatcher<Cell> {
    /** Creates a matcher for a cell with value and timestamp matching the given matchers. */
    public CellMatcher(Matcher<ByteString> value, Matcher<Long> timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }

    /** Sets the expected cell timestamp to be equal to {@code timestamp}. */
    public void setTimestamp(long timestamp) {
      this.timestamp = CoreMatchers.equalTo(timestamp);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("timestamp ").appendDescriptionOf(timestamp);
      description.appendText(" value ").appendDescriptionOf(value);
    }

    @Override
    protected boolean matchesSafely(Cell item) {
      return this.timestamp.matches(item.getTimestampMicros())
          && this.value.matches(item.getValue());
    }

    private final Matcher<ByteString> value;
    private Matcher<Long> timestamp;
  }

  /**
   * A matcher for the Bigtable {@code Column} protocol buffer.
   */
  static class ColumnMatcher extends TypeSafeMatcher<Column> {
    /**
     * Creates a matcher expecting a column name that will match {@code columnName}.
     * Initially the matcher expects no cells in the column.  Cells should be added
     * in the order they are expected to occur in the Column protocol message with
     * {@link #addCell(Matcher)}.
     */
    public ColumnMatcher(Matcher<ByteString> columnName) {
      this.qualifier = columnName;
      this.cells = new ArrayList<>();
    }

    /** Adds an expected cell value. */
    public void addCell(Matcher<Cell> cell) {
      this.cells.add(cell);
    }

    @Override
    public void describeTo(Description description) {
      description.appendValue("column qualifier ").appendDescriptionOf(qualifier);
      description.appendText(" with ").appendValue(cells.size()).appendText(" cells");
      description.appendText(" matching: ");
      String sep = "";
      for (Matcher<Cell> m : cells) {
        description.appendText(sep).appendDescriptionOf(m);
        sep = ", ";
      }
    }

    @Override
    protected boolean matchesSafely(Column item) {
      if (!qualifier.matches(item.getQualifier())) {
        return false;
      }
      if (item.getCellsCount() != cells.size()) {
        return false;
      }
      for (int i = 0; i < cells.size(); ++i) {
        if (!cells.get(i).matches(item.getCells(i))) {
          return false;
        }
      }
      return true;
    }

    private final Matcher<ByteString> qualifier;
    private final List<Matcher<Cell>> cells;
  }

  /**
   * A matcher for the Bigtable {@code Family} protocol buffer.
   */
  static class FamilyMatcher extends TypeSafeMatcher<Family> {
    /**
     * Creates a new matcher for a family with the given {@code name}.
     * Initially the family expects no columns.  Columns should be added
     * in the order they will occur with {@link #addColumn(Matcher)}.
     *
     * @param name The expected family name.
     */
    public FamilyMatcher(String name) {
      this.name = name;
      this.columns = new ArrayList<>();
    }

    public String getFamilyName() { return name; }

    /**
     * Add an expected column to this column family.  Columns should be
     * added in the order they will occur.
     */
    public void addColumn(Matcher<Column> c) {
      columns.add(c);
    }

    @Override
    public boolean matchesSafely(Family family) {
      if (!this.name.equals(family.getName())) {
        return false;
      }
      if (columns.size() != family.getColumnsCount()) {
        return false;
      }
      for (int i = 0; i < columns.size(); ++i) {
        if (!columns.get(i).matches(family.getColumns(i))) {
          return false;
        }
      }
      return true;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("family name: ").appendValue(name);
      String sep = "";
      for (Matcher<Column> column : columns) {
        description.appendText(sep).appendDescriptionOf(column);
        sep = ", ";
      }
    }

    private final String name;
    private final List<Matcher<Column>> columns;
  }

  /** Creates a matcher for a row with a key matching the given {@code rowKey}. */
  RowMatcher(Matcher<ByteString> rowKey) {
    this.rowKey = rowKey;
    this.families = new HashMap<>();
  }

  @Override
  public boolean matchesSafely(Row row) {
    if (!rowKey.matches(row.getKey())) {
      return false;
    }
    if (row.getFamiliesCount() != families.size()) {
      return false;
    }
    try {
      List<Family> f = row.getFamiliesList();
      for (Family fam : f) {
        RowMatcher.FamilyMatcher matcher = families.get(fam.getName());
        if (matcher == null || !matcher.matchesSafely(fam)) {
          return false;
        }
      }
    } catch (NullPointerException npe) {
      // Don't know if this is really necessary, but it seems prudent.
      return false;
    }
    // Everything matched.
    return true;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("row with key matching ").appendDescriptionOf(rowKey);
    if (families.isEmpty()) {
      description.appendText(" and no column families.");
    } else {
      description.appendText(" and column families: ");
      String sep = "";
      for (RowMatcher.FamilyMatcher family : families.values()) {
        description.appendText(sep).appendDescriptionOf(family);
        sep = ", ";
      }
    }
  }

  private final Matcher<ByteString> rowKey;
  private final Map<String, RowMatcher.FamilyMatcher> families;
  private FamilyMatcher lastFamily;
  private ColumnMatcher lastColumn;
  private CellMatcher lastCell;
}
