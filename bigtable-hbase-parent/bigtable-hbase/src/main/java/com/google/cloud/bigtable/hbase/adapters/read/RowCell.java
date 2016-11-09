/** Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters.read;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This implementation of {@link org.apache.hadoop.hbase.Cell} is more efficient for Bigtable scanning than {@link org.apache.hadoop.hbase.KeyValue}
 * . RowCell is pretty straight forward. Each *Array() method returns the array passed in in the
 * constructor. Each *Offset() method returns 0. Each *Length() returns the length of the array.
 * This implementation is a few microseconds quicker thank KeyValue, which makes a big performance
 * difference for large scans.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class RowCell implements Cell {

  private final byte[] rowArray;
  private final byte[] familyArray;
  private final byte[] qualifierArray;
  private final long timestamp;
  private final byte[] valueArray;

  /**
   * <p>Constructor for RowCell.</p>
   *
   * @param rowArray an array of byte.
   * @param familyArray an array of byte.
   * @param qualifierArray an array of byte.
   * @param timestamp a long.
   * @param valueArray an array of byte.
   */
  public RowCell(byte[] rowArray, byte[] familyArray, byte[] qualifierArray,
      long timestamp, byte[] valueArray) {
    this.rowArray = rowArray;
    this.familyArray = familyArray;
    this.qualifierArray = qualifierArray;
    this.timestamp = timestamp;
    this.valueArray = valueArray;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getRowArray() {
    return this.rowArray;
  }

  /** {@inheritDoc} */
  @Override
  public int getRowOffset() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public short getRowLength() {
    return (short) this.rowArray.length;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getFamilyArray() {
    return this.familyArray;
  }

  /** {@inheritDoc} */
  @Override
  public int getFamilyOffset() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public byte getFamilyLength() {
    return (byte) this.familyArray.length;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getQualifierArray() {
    return this.qualifierArray;
  }

  /** {@inheritDoc} */
  @Override
  public int getQualifierOffset() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public int getQualifierLength() {
    return this.qualifierArray.length;
  }

  /** {@inheritDoc} */
  @Override
  public long getTimestamp() {
    return timestamp;
  }

  /** {@inheritDoc} */
  @Override
  public byte getTypeByte() {
    return Type.Put.getCode();
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public long getMvccVersion() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public long getSequenceId() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValueArray() {
    return this.valueArray;
  }

  /** {@inheritDoc} */
  @Override
  public int getValueOffset() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public int getValueLength() {
    return this.valueArray.length;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getTagsArray() {
    return HConstants.EMPTY_BYTE_ARRAY;
  }

  /** {@inheritDoc} */
  @Override
  public int getTagsOffset() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public int getTagsLength() {
    return 0;
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public byte[] getValue() {
    return Bytes.copy(this.valueArray);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public byte[] getFamily() {
    return Bytes.copy(this.familyArray);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public byte[] getQualifier() {
    return Bytes.copy(this.qualifierArray);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public byte[] getRow() {
    return Bytes.copy(this.rowArray);
  }

  /**
   * Needed doing 'contains' on List.  Only compares the key portion, not the value.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Cell)) {
      return false;
    }
    return CellComparator.equals(this, (Cell)other);
  }

  /**
   * In line with {@link #equals(Object)}, only uses the key portion, not the value.
   */
  @Override
  public int hashCode() {
    return CellComparator.hashCodeIgnoreMvcc(this);
  }


  //---------------------------------------------------------------------------
  //
  //  String representation
  //
  //---------------------------------------------------------------------------

  @Override
  public String toString() {
    if (this.rowArray == null || this.rowArray.length == 0) {
      return "empty";
    }
    return KeyValue.keyToString(this.rowArray) + "/vlen=" + getValueLength();
  }
}

