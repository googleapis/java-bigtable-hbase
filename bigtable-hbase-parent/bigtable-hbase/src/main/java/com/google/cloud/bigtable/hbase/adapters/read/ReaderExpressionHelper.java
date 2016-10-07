/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Methods and constants to help build a bigtable reader expression
 * // TODO(AngusDavis): Move more ScanAdapter and FilterAdapter writing logic to here.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ReaderExpressionHelper {
  /** Constant <code>ANY_BYTE="\\C"</code> */
  public static final String ANY_BYTE = "\\C";
  /** Constant <code>ANY_BYTE_BYTES=Bytes.toBytes(ANY_BYTE)</code> */
  public static final byte[] ANY_BYTE_BYTES = Bytes.toBytes(ANY_BYTE);
  /** Constant <code>ALL_QUALIFIERS="\\C*"</code> */
  public static final String ALL_QUALIFIERS = "\\C*";
  /** Constant <code>ALL_QUALIFIERS_BYTES=Bytes.toBytes(ALL_QUALIFIERS)</code> */
  public static final byte[] ALL_QUALIFIERS_BYTES = Bytes.toBytes(ALL_QUALIFIERS);
  /** Constant <code>ALL_FAMILIES=".*"</code> */
  public static final String ALL_FAMILIES = ".*";
  /** Constant <code>ALL_FAMILIES_BYTES=Bytes.toBytes(ALL_FAMILIES)</code> */
  public static final byte[] ALL_FAMILIES_BYTES = Bytes.toBytes(ALL_FAMILIES);
  /** Constant <code>ALL_VERSIONS="all"</code> */
  public static final String ALL_VERSIONS = "all";
  /** Constant <code>ALL_VERSIONS_BYTES=Bytes.toBytes(ALL_VERSIONS)</code> */
  public static final byte[] ALL_VERSIONS_BYTES = Bytes.toBytes(ALL_VERSIONS);
  /** Constant <code>LATEST_VERSION="latest"</code> */
  public static final String LATEST_VERSION = "latest";
  /** Constant <code>INTERLEAVE_CHARACTERS=Bytes.toBytes(" + ")</code> */
  public static final byte[] INTERLEAVE_CHARACTERS = Bytes.toBytes(" + ");
  /** Constant <code>PIPE_CHARACTER_BYTES=Bytes.toBytes(" | ")</code> */
  public static final byte[] PIPE_CHARACTER_BYTES = Bytes.toBytes(" | ");
  private final static byte[] NULL_CHARACTER_BYTES = Bytes.toBytes("\\x00");

  /**
   * Write unquoted to the OutputStream applying just Bigtable reader
   * expression quoting. Used when the supplied value is NOT a regular expression.
   *
   * @param unquoted A byte-array, possibly containing bytes outside of the ASCII
   * @param outputStream A stream to write quoted output to
   * @throws java.io.IOException if any.
   */
  public static void writeFilterQuotedExpression(byte[] unquoted, OutputStream outputStream)
      throws IOException{
    QuoteFilterExpressionStream quoteFilterExpressionStream =
        new QuoteFilterExpressionStream(outputStream);
    quoteFilterExpressionStream.write(unquoted);
    quoteFilterExpressionStream.close();
  }

  /**
   * Write unquoted to the OutputStream applying both RE2:QuoteMeta and Bigtable reader
   * expression quoting.
   *
   * @param outputStream A stream to write quoted output to
   * @param unquoted A byte-array, possibly containing bytes outside of the ASCII
   * @throws java.io.IOException if any.
   */
  public static void writeQuotedExpression(OutputStream outputStream, byte[] unquoted)
      throws  IOException {
    writeQuotedRegularExpression(new QuoteFilterExpressionStream(outputStream), unquoted);
  }

  /**
   * <p>quoteRegularExpression.</p>
   *
   * @param unquoted an array of byte.
   * @return an array of byte.
   * @throws java.io.IOException if any.
   */
  public static byte[] quoteRegularExpression(byte[] unquoted) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(unquoted.length * 2);
    writeQuotedRegularExpression(baos, unquoted);
    return baos.toByteArray();
  }

  /**
   * Write unquoted to the OutputStream applying RE2:QuoteMeta quoting.
   *
   * @param outputStream a {@link java.io.OutputStream} object.
   * @param unquoted an array of byte.
   * @throws java.io.IOException if any.
   */
  public static void writeQuotedRegularExpression(OutputStream outputStream, byte[] unquoted)
      throws IOException {
    QuoteMetaOutputStream quoteMetaOutputStream = new QuoteMetaOutputStream(outputStream);
    quoteMetaOutputStream.write(unquoted);
    quoteMetaOutputStream.close();
  }

  /**
   * An OutputStream that performs RE2:QuoteMeta as bytes are written.
   */
  public static class QuoteMetaOutputStream extends OutputStream {
    protected final OutputStream delegate;

    public QuoteMetaOutputStream(OutputStream delegate) {
      this.delegate = delegate;
    }

    public void writeNullCharacterBytes() throws IOException {
      for (byte b : NULL_CHARACTER_BYTES) {
        delegate.write(b);
      }
    }

    @Override
    public void write(int unquoted) throws IOException {
      if (unquoted == 0) { // Special handling for null chars.
        // Note that this special handling is not strictly required for RE2,
        // but this quoting is required for other regexp libraries such as
        // PCRE.
        // Can't use "\\0" since the next character might be a digit.
        writeNullCharacterBytes();
        return;
      }
      if ((unquoted < 'a' || unquoted > 'z')
          && (unquoted < 'A' || unquoted > 'Z')
          && (unquoted < '0' || unquoted > '9')
          && (unquoted != '_')
          // If this is the part of a UTF8 or Latin1 character, we need
          // to copy this byte without escaping.  Experimentally this is
          // what works correctly with the regexp library.
          && (unquoted >= 0)) {
        delegate.write('\\');
      }
      delegate.write(unquoted);
    }
  }

  /**
   * An OutputStream that performs bigtable reader filter expression language quoting of
   * '@', '{', and '}' by pre-pending a '@' to each.
   */
  public static class QuoteFilterExpressionStream extends OutputStream {
    protected final OutputStream delegate;

    public QuoteFilterExpressionStream(OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(int unquoted) throws IOException {
      switch(unquoted) {
      case '@':
      case '{':
      case '}':
        delegate.write('@');
      default:
        // fall through
        delegate.write(unquoted);
      }
    }
  }
}
