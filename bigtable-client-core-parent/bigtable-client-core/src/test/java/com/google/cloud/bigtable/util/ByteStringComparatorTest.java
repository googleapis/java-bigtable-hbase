package com.google.cloud.bigtable.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.protobuf.ByteString;

import java.util.Comparator;

import org.junit.Assert;

@RunWith(JUnit4.class)
public class ByteStringComparatorTest {

  Comparator<ByteString> underTest = ByteStringComparator.INSTANCE;

  @Test
  public void testSimple(){
    compare("a", "c");
    compare("aa", "cc");
    compare("aa", "c");
    compare("a", "cc");
    compare("aa", "aac");
  }

  @Test
  public void testUnsigned() {
    compare(ByteString.copyFrom(new byte[] { 0x7f }),
      ByteString.copyFrom(new byte[] { (byte) 0x80 }));
  }

  protected void compare(String a, String b) {
    compare(ByteString.copyFromUtf8(a), ByteString.copyFromUtf8(b));
  }

  protected void compare(ByteString a, ByteString b) {
    Assert.assertTrue(underTest.compare(a, b) < 0);
    Assert.assertTrue(underTest.compare(b, a) > 0);
    Assert.assertEquals(0, underTest.compare(b, b));
  }
}
