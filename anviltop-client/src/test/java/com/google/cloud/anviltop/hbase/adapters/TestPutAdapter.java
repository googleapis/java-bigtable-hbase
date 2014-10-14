package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData.RowMutation;
import com.google.bigtable.anviltop.AnviltopData.RowMutation.Mod;
import com.google.bigtable.anviltop.AnviltopData.RowMutation.Mod.SetCell;
import com.google.cloud.anviltop.hbase.DataGenerationHelper;

import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class TestPutAdapter {

  protected final PutAdapter adapter = new PutAdapter();
  protected final DataGenerationHelper dataHelper = new DataGenerationHelper();
  protected final QualifierTestHelper qualifierHelper = new QualifierTestHelper();

  @Test
  public void testSingleCellIsConverted() {
    byte[] row = dataHelper.randomData("rk-");
    byte[] family = dataHelper.randomData("f");
    byte[] qualifier = dataHelper.randomData("qual");
    byte[] value = dataHelper.randomData("v1");
    long timestamp = 2L;

    Put hbasePut = new Put(row);
    hbasePut.add(family, qualifier, timestamp, value);

    RowMutation.Builder rowMutationBuilder = adapter.adapt(hbasePut);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(1, rowMutationBuilder.getModsCount());
    Mod mod = rowMutationBuilder.getMods(0);

    Assert.assertTrue(mod.hasSetCell());
    SetCell setCell = mod.getSetCell();

    byte[] fullCellQualifier = qualifierHelper.makeFullQualifier(family, qualifier);
    Assert.assertArrayEquals(fullCellQualifier, setCell.getColumnName().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp),
        setCell.getCell().getTimestampMicros());
    Assert.assertArrayEquals(value, setCell.getCell().getValue().toByteArray());
  }

  @Test
  public void testMultipleCellsInOneFamilyAreConverted() {
    byte[] row = dataHelper.randomData("rk-");
    byte[] family = dataHelper.randomData("f1");
    byte[] qualifier1 = dataHelper.randomData("qual1");
    byte[] qualifier2 = dataHelper.randomData("qual2");
    byte[] value1 = dataHelper.randomData("v1");
    byte[] value2 = dataHelper.randomData("v2");
    long timestamp1 = 1L;
    long timestamp2 = 2L;

    byte[] fullCellQualifier1 = qualifierHelper.makeFullQualifier(family, qualifier1);
    byte[] fullCellQualifier2 = qualifierHelper.makeFullQualifier(family, qualifier2);

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
    byte[] row = dataHelper.randomData("rk-");
    byte[] family1 = dataHelper.randomData("f1");
    byte[] family2 = dataHelper.randomData("f2");
    byte[] qualifier1 = dataHelper.randomData("qual1");
    byte[] qualifier2 = dataHelper.randomData("qual2");
    byte[] value1 = dataHelper.randomData("v1");
    byte[] value2 = dataHelper.randomData("v1");
    long timestamp1 = 1L;
    long timestamp2 = 2L;

    byte[] fullCellQualifier1 = qualifierHelper.makeFullQualifier(family1, qualifier1);
    byte[] fullCellQualifier2 = qualifierHelper.makeFullQualifier(family2, qualifier2);

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
    byte[] row = dataHelper.randomData("rk-");
    byte[] family1 = dataHelper.randomData("f1");
    byte[] qualifier1 = dataHelper.randomData("qual1");
    byte[] value1 = dataHelper.randomData("v1");

    Put hbasePut = new Put(row);
    hbasePut.add(family1, qualifier1, value1);

    RowMutation.Builder rowMutationBuilder = adapter.adapt(hbasePut);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(1, rowMutationBuilder.getModsCount());
    Mod mod = rowMutationBuilder.getMods(0);

    Assert.assertTrue(mod.hasSetCell());
    SetCell setCell = mod.getSetCell();

    byte[] fullCellQualifier = qualifierHelper.makeFullQualifier(family1, qualifier1);
    Assert.assertArrayEquals(fullCellQualifier, setCell.getColumnName().toByteArray());
    Assert.assertFalse(setCell.getCell().hasTimestampMicros());
    Assert.assertEquals(0L, setCell.getCell().getTimestampMicros());
    Assert.assertArrayEquals(value1, setCell.getCell().getValue().toByteArray());
  }
}
