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
    compare(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("b"));
    compare(ByteString.copyFromUtf8("aa"), ByteString.copyFromUtf8("bb"));
    compare(ByteString.copyFromUtf8("aa"), ByteString.copyFromUtf8("b"));
    compare(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("bb"));
    compare(ByteString.copyFromUtf8("aa"), ByteString.copyFromUtf8("aab"));
  }

  public void testUnsigned() {
    compare(ByteString.copyFrom(new byte[] { 0x7f }),
      ByteString.copyFrom(new byte[] { (byte) 0x80 }));
  }

  protected void compare(ByteString a, ByteString b) {
    Assert.assertEquals(-1, underTest.compare(a, b));
    Assert.assertEquals(1, underTest.compare(b, a));
    Assert.assertEquals(0, underTest.compare(b, b));
  }
}
