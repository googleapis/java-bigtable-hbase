package com.google.cloud.anviltop.hbase.adapters;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Methods and constants to help build a bigtable reader expression
 * // TODO(AngusDavis): Move more ScanAdapter and FilterAdapter writing logic to here.
 */
public class ReaderExpressionHelper {

  public static final String ALL_QUALIFIERS = "\\C*";
  public static final String ALL_FAMILIES = ".*";
  public static final String ALL_VERSIONS = "ALL";
  public static final byte[] INTERLEAVE_CHARACTERS = Bytes.toBytes(" + ");
  private final static byte[] NULL_CHARACTER_BYTES = Bytes.toBytes("\\x00");

  /**
   * Write unquoted to the OutputStream applying both RE2:QuoteMeta and Bigtable reader
   * expression quoting.
   * @param unquoted A byte-array, possibly containing bytes outside of the ASCII
   * @param outputStream A stream to write quoted output to
   */
  public void writeQuotedExpression(byte[] unquoted, OutputStream outputStream)
      throws  IOException {
    QuoteFilterExpressionStream quoteFilterExpressionStream =
        new QuoteFilterExpressionStream(outputStream);
    QuoteMetaOutputStream quoteMetaOutputStream =
        new QuoteMetaOutputStream(quoteFilterExpressionStream);

    quoteMetaOutputStream.write(unquoted);
  }

  /**
   * An OutputStream that performs RE2:QuoteMeta as bytes are written.
   */
  protected static class QuoteMetaOutputStream extends OutputStream {
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
  protected static class QuoteFilterExpressionStream extends OutputStream {
    protected final OutputStream delegate;

    public QuoteFilterExpressionStream(OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(int unquoted) throws IOException {
      if (unquoted == '@' || unquoted == '{' || unquoted == '}') {
        delegate.write('@');
      }
      delegate.write(unquoted);
    }
  }
}
