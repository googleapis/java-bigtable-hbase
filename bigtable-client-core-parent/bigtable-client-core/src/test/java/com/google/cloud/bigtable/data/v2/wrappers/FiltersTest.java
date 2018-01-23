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
package com.google.cloud.bigtable.data.v2.wrappers;

import static com.google.cloud.bigtable.data.v2.wrappers.Filters.F;

import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Condition;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.bigtable.v2.ValueRange;
import com.google.protobuf.ByteString;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FiltersTest {
  @Test
  public void chainTest() {
    RowFilter actualProto =
        F.chain()
            .filter(F.key().regex(".*"))
            .filter(F.key().sample(0.5))
            .filter(F.chain().filter(F.family().regex("hi$")))
            .toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setChain(
                Chain.newBuilder()
                    .addFilters(
                        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(".*")))
                    .addFilters(RowFilter.newBuilder().setRowSampleFilter(0.5))
                    .addFilters(
                        RowFilter.newBuilder()
                            .setChain(
                                Chain.newBuilder()
                                    .addFilters(
                                        RowFilter.newBuilder().setFamilyNameRegexFilter("hi$")))))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void interleaveTest() {
    RowFilter actualProto =
        F.interleave()
            .filter(F.key().regex(".*"))
            .filter(F.key().sample(0.5))
            .filter(F.interleave().filter(F.family().regex("hi$")))
            .toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setInterleave(
                Interleave.newBuilder()
                    .addFilters(
                        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(".*")))
                    .addFilters(RowFilter.newBuilder().setRowSampleFilter(0.5))
                    .addFilters(
                        RowFilter.newBuilder()
                            .setInterleave(
                                Interleave.newBuilder()
                                    .addFilters(
                                        RowFilter.newBuilder().setFamilyNameRegexFilter("hi$")))))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void conditionTest() {
    RowFilter actualProto =
        F.condition(F.key().regex(".*"))
            .then(F.label("true"))
            .otherwise(F.label("false"))
            .toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setPredicateFilter(
                        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(".*")))
                    .setTrueFilter(RowFilter.newBuilder().setApplyLabelTransformer("true"))
                    .setFalseFilter(RowFilter.newBuilder().setApplyLabelTransformer("false")))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void passTest() {
    RowFilter actualProto = F.pass().toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setPassAllFilter(true).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void blockTest() {
    RowFilter actualProto = F.block().toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setBlockAllFilter(true).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void sinkTest() {
    RowFilter actualProto = F.sink().toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setSink(true).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void labelTest() {
    RowFilter actualProto = F.label("my-label").toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setApplyLabelTransformer("my-label").build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void keyRegexTest() {
    RowFilter actualProto = F.key().regex(".*").toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(".*")).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void keyExactMatchTest() {
    RowFilter actualProto = F.key().exactMatch(ByteString.copyFromUtf8(".*")).toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8("\\.\\*")).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void keySampleTest() {
    RowFilter actualProto = F.key().sample(0.3).toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setRowSampleFilter(0.3).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void familyRegexTest() {
    RowFilter actualProto = F.family().regex("^hi").toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setFamilyNameRegexFilter("^hi").build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void familyExactMatchTest() {
    RowFilter actualProto = F.family().exactMatch("^hi").toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setFamilyNameRegexFilter("\\^hi").build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void qualifierRegexTest() {
    RowFilter actualProto = F.qualifier().regex("^hi").toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(ByteString.copyFromUtf8("^hi"))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void qualifierExactMatchTest() {
    RowFilter actualProto = F.qualifier().exactMatch(ByteString.copyFromUtf8("^hi")).toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(ByteString.copyFromUtf8("\\^hi"))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void qualifierRangeOpenClosed() {
    String family = "family";
    ByteString begin = ByteString.copyFromUtf8("begin");
    ByteString end = ByteString.copyFromUtf8("end");
    RowFilter actualProto = F.qualifier().range()
        .family(family)
        .startOpen(begin)
        .endClosed(end)
        .build()
        .toProto();
    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setColumnRangeFilter(
                ColumnRange.newBuilder()
                    .setFamilyName(family)
                    .setStartQualifierOpen(begin)
                    .setEndQualifierClosed(end))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void qualifierRangeClosedOpen() {
    String family = "family";
    ByteString begin = ByteString.copyFromUtf8("begin");
    ByteString end = ByteString.copyFromUtf8("end");
    RowFilter actualProto = F.qualifier().range()
        .family(family)
        .startClosed(begin)
        .endOpen(end)
        .build()
        .toProto();
    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setColumnRangeFilter(
                ColumnRange.newBuilder()
                    .setFamilyName(family)
                    .setStartQualifierClosed(begin)
                    .setEndQualifierOpen(end))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void valueRegex() {
    RowFilter actualProto = F.value().regex("some[0-9]regex").toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setValueRegexFilter(ByteString.copyFromUtf8("some[0-9]regex"))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void valueExactMatch() {
    RowFilter actualProto =
        F.value().exactMatch(ByteString.copyFromUtf8("some[0-9]regex")).toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setValueRegexFilter(ByteString.copyFromUtf8("some\\[0\\-9\\]regex"))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void valueRangeClosedOpen() {
    ByteString begin = ByteString.copyFromUtf8("begin");
    ByteString end = ByteString.copyFromUtf8("end");

    RowFilter actualProto = F.value().range()
        .startOpen(begin)
        .endClosed(end)
        .build()
        .toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setValueRangeFilter(
                ValueRange.newBuilder()
                    .setStartValueOpen(begin)
                    .setEndValueClosed(end))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void valueRangeOpenClosed() {
    ByteString begin = ByteString.copyFromUtf8("begin");
    ByteString end = ByteString.copyFromUtf8("end");

    RowFilter actualProto = F.value().range()
        .startClosed(begin)
        .endOpen(end)
        .build()
        .toProto();

    RowFilter expectedFilter =
        RowFilter.newBuilder()
            .setValueRangeFilter(
                ValueRange.newBuilder()
                    .setStartValueClosed(begin)
                    .setEndValueOpen(end))
            .build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void valueStripTest() {
    RowFilter actualProto = F.value().strip().toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setStripValueTransformer(true).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void offsetCellsPerRowTest() {
    RowFilter actualProto = F.offset().cellsPerRow(10).toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setCellsPerRowOffsetFilter(10).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void limitCellsPerRowTest() {
    RowFilter actualProto = F.limit().cellsPerRow(10).toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setCellsPerRowLimitFilter(10).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }

  @Test
  public void limitCellsPerColumnTest() {
    RowFilter actualProto = F.limit().cellsPerColumn(10).toProto();

    RowFilter expectedFilter = RowFilter.newBuilder().setCellsPerColumnLimitFilter(10).build();

    Assert.assertEquals(expectedFilter, actualProto);
  }
}
