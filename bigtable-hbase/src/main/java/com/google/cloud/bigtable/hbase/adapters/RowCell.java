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
package com.google.cloud.bigtable.hbase.adapters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This implementation of {@link Cell} is more efficient for Bigtable scanning than {@link KeyValue}
 * . RowCell is pretty straight forward. Each *Array() method returns the array passed in in the
 * constructor. Each *Offset() method returns 0. Each *Length() returns the length of the array.
 * This implementation is a few microseconds quicker thank KeyValue, which makes a big performance
 * difference for large scans.
 */
public class RowCell implements Cell {

  private final byte[] rowArray;
  private final byte[] familyArray;
  private final byte[] qualifierArray;
  private final long timestamp;
  private final byte[] valueArray;

  public RowCell(byte[] rowArray, byte[] familyArray, byte[] qualifierArray,
      long timestamp, byte[] valueArray) {
    this.rowArray = rowArray;
    this.familyArray = familyArray;
    this.qualifierArray = qualifierArray;
    this.timestamp = timestamp;
    this.valueArray = valueArray;
  }

  @Override
  public byte[] getRowArray() {
    return this.rowArray;
  }

  @Override
  public int getRowOffset() {
    return 0;
  }

  @Override
  public short getRowLength() {
    return (short) this.rowArray.length;
  }

  @Override
  public byte[] getFamilyArray() {
    return this.familyArray;
  }

  @Override
  public int getFamilyOffset() {
    return 0;
  }

  @Override
  public byte getFamilyLength() {
    return (byte) this.familyArray.length;
  }

  @Override
  public byte[] getQualifierArray() {
    return this.qualifierArray;
  }

  @Override
  public int getQualifierOffset() {
    return 0;
  }

  @Override
  public int getQualifierLength() {
    return this.qualifierArray.length;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public byte getTypeByte() {
    return Type.Put.getCode();
  }

  @Deprecated
  @Override
  public long getMvccVersion() {
    return 0;
  }

  @Override
  public long getSequenceId() {
    return 0;
  }

  @Override
  public byte[] getValueArray() {
    return this.valueArray;
  }

  @Override
  public int getValueOffset() {
    return 0;
  }

  @Override
  public int getValueLength() {
    return this.valueArray.length;
  }

  @Override
  public byte[] getTagsArray() {
    return HConstants.EMPTY_BYTE_ARRAY;
  }

  @Override
  public int getTagsOffset() {
    return 0;
  }

  @Override
  public int getTagsLength() {
    return 0;
  }

  @Deprecated
  @Override
  public byte[] getValue() {
    return Bytes.copy(this.valueArray);
  }

  @Deprecated
  @Override
  public byte[] getFamily() {
    return Bytes.copy(this.familyArray);
  }

  @Deprecated
  @Override
  public byte[] getQualifier() {
    return Bytes.copy(this.qualifierArray);
  }

  @Deprecated
  @Override
  public byte[] getRow() {
    return Bytes.copy(this.rowArray);
  }
}
