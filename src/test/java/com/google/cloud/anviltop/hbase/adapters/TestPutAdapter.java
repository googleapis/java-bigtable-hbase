package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData.RowMutation;
import com.google.bigtable.anviltop.AnviltopData.RowMutation.Mod;
import com.google.bigtable.anviltop.AnviltopData.RowMutation.Mod.SetCell;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class TestPutAdapter {
  protected byte[] randomData(String prefix) {
    return Bytes.toBytes(prefix + RandomStringUtils.randomAlphanumeric(8));
  }

  private final PutAdapter adapter = new PutAdapter();
  private final byte[] familySeparator;

  public  TestPutAdapter() {
    familySeparator = StandardCharsets.UTF_8.encode(":").array();
  }

  protected byte[] makeFullQualifier(byte[] family, byte[] cellQualifier) {
    byte[] result = new byte[family.length + familySeparator.length + cellQualifier.length];
    ByteBuffer wrapper = ByteBuffer.wrap(result);
    wrapper.put(family);
    wrapper.put(familySeparator);
    wrapper.put(cellQualifier);
    return result;
  }

  @Test
  public void testSingleCellIsConverted() {
    byte[] row = randomData("rk-");
    byte[] family = randomData("f");
    byte[] qualifier = randomData("qual");
    byte[] value = randomData("v1");
    long timestamp = 2L;

    Put hbasePut = new Put(row);
    hbasePut.add(family, qualifier, timestamp, value);

    RowMutation.Builder rowMutationBuilder = adapter.adapt(hbasePut);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(1, rowMutationBuilder.getModsCount());
    Mod mod = rowMutationBuilder.getMods(0);

    Assert.assertTrue(mod.hasSetCell());
    SetCell setCell = mod.getSetCell();

    byte[] fullCellQualifier = makeFullQualifier(family, qualifier);
    Assert.assertArrayEquals(fullCellQualifier, setCell.getColumnName().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp),
        setCell.getCell().getTimestampMicros());
    Assert.assertArrayEquals(value, setCell.getCell().getValue().toByteArray());
  }

  @Test
  public void testMultipleCellsInOneFamilyAreConverted() {
    byte[] row = randomData("rk-");
    byte[] family = randomData("f1");
    byte[] qualifier1 = randomData("qual1");
    byte[] qualifier2 = randomData("qual2");
    byte[] value1 = randomData("v1");
    byte[] value2 = randomData("v2");
    long timestamp1 = 1L;
    long timestamp2 = 2L;

    byte[] fullCellQualifier1 = makeFullQualifier(family, qualifier1);
    byte[] fullCellQualifier2 = makeFullQualifier(family, qualifier2);

    Put hbasePut = new Put(row);
    hbasePut.add(family, qualifier1, timestamp1, value1);
    hbasePut.add(family, qualifier2, timestamp2, value2);

    RowMutation.Builder rowMutationBuilder = adapter.adapt(hbasePut);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(2, rowMutationBuilder.getModsCount());
    Mod mod = rowMutationBuilder.getMods(0);

    Assert.assertTrue(mod.hasSetCell());
    SetCell setCell = mod.getSetCell();
    Assert.assertArrayEquals(fullCellQualifier1, setCell.getColumnName().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp1),
        setCell.getCell().getTimestampMicros());
    Assert.assertArrayEquals(value1, setCell.getCell().getValue().toByteArray());


    Mod mod2 = rowMutationBuilder.getMods(1);
    SetCell setCell2 = mod2.getSetCell();
    Assert.assertArrayEquals(fullCellQualifier2, setCell2.getColumnName().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp2),
        setCell2.getCell().getTimestampMicros());
    Assert.assertArrayEquals(value2, setCell2.getCell().getValue().toByteArray());
  }

  @Test
  public void testMultipleCellsInMultipleFamiliesAreConverted() {
    byte[] row = randomData("rk-");
    byte[] family1 = randomData("f1");
    byte[] family2 = randomData("f2");
    byte[] qualifier1 = randomData("qual1");
    byte[] qualifier2 = randomData("qual2");
    byte[] value1 = randomData("v1");
    byte[] value2 = randomData("v1");
    long timestamp1 = 1L;
    long timestamp2 = 2L;

    byte[] fullCellQualifier1 = makeFullQualifier(family1, qualifier1);
    byte[] fullCellQualifier2 = makeFullQualifier(family2, qualifier2);

    Put hbasePut = new Put(row);
    hbasePut.add(family1, qualifier1, timestamp1, value1);
    hbasePut.add(family2, qualifier2, timestamp2, value2);

    RowMutation.Builder rowMutationBuilder = adapter.adapt(hbasePut);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(2, rowMutationBuilder.getModsCount());
    Mod mod = rowMutationBuilder.getMods(0);

    Assert.assertTrue(mod.hasSetCell());
    SetCell setCell = mod.getSetCell();
    Assert.assertArrayEquals(fullCellQualifier1, setCell.getColumnName().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp1),
        setCell.getCell().getTimestampMicros());
    Assert.assertArrayEquals(value1, setCell.getCell().getValue().toByteArray());

    Mod mod2 = rowMutationBuilder.getMods(1);
    SetCell setCell2 = mod2.getSetCell();
    Assert.assertArrayEquals(fullCellQualifier2, setCell2.getColumnName().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp2),
        setCell2.getCell().getTimestampMicros());
    Assert.assertArrayEquals(value2, setCell2.getCell().getValue().toByteArray());
  }

  @Test
  public void testImplicitTimestampsAreUnset() {
    byte[] row = randomData("rk-");
    byte[] family1 = randomData("f1");
    byte[] qualifier1 = randomData("qual1");
    byte[] value1 = randomData("v1");

    Put hbasePut = new Put(row);
    hbasePut.add(family1, qualifier1, value1);

    RowMutation.Builder rowMutationBuilder = adapter.adapt(hbasePut);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(1, rowMutationBuilder.getModsCount());
    Mod mod = rowMutationBuilder.getMods(0);

    Assert.assertTrue(mod.hasSetCell());
    SetCell setCell = mod.getSetCell();

    byte[] fullCellQualifier = makeFullQualifier(family1, qualifier1);
    Assert.assertArrayEquals(fullCellQualifier, setCell.getColumnName().toByteArray());
    Assert.assertFalse(setCell.getCell().hasTimestampMicros());
    Assert.assertEquals(0L, setCell.getCell().getTimestampMicros());
    Assert.assertArrayEquals(value1, setCell.getCell().getValue().toByteArray());
  }

}
