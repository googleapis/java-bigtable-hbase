// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/v2/data.proto

package com.google.bigtable.v2;

/**
 * Protobuf type {@code google.bigtable.v2.RowRange}
 *
 * <pre>
 * Specifies a contiguous range of rows.
 * </pre>
 */
public  final class RowRange extends
    com.google.protobuf.GeneratedMessage implements
    // @@protoc_insertion_point(message_implements:google.bigtable.v2.RowRange)
    RowRangeOrBuilder {
  // Use RowRange.newBuilder() to construct.
  private RowRange(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
    super(builder);
  }
  private RowRange() {
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private RowRange(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry) {
    this();
    int mutable_bitField0_ = 0;
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!input.skipField(tag)) {
              done = true;
            }
            break;
          }
          case 10: {
            startKeyCase_ = 1;
            startKey_ = input.readBytes();
            break;
          }
          case 18: {
            startKeyCase_ = 2;
            startKey_ = input.readBytes();
            break;
          }
          case 26: {
            endKeyCase_ = 3;
            endKey_ = input.readBytes();
            break;
          }
          case 34: {
            endKeyCase_ = 4;
            endKey_ = input.readBytes();
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e.setUnfinishedMessage(this));
    } catch (java.io.IOException e) {
      throw new RuntimeException(
          new com.google.protobuf.InvalidProtocolBufferException(
              e.getMessage()).setUnfinishedMessage(this));
    } finally {
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_RowRange_descriptor;
  }

  protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_RowRange_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.google.bigtable.v2.RowRange.class, com.google.bigtable.v2.RowRange.Builder.class);
  }

  private int startKeyCase_ = 0;
  private java.lang.Object startKey_;
  public enum StartKeyCase
      implements com.google.protobuf.Internal.EnumLite {
    START_KEY_CLOSED(1),
    START_KEY_OPEN(2),
    STARTKEY_NOT_SET(0);
    private int value = 0;
    private StartKeyCase(int value) {
      this.value = value;
    }
    public static StartKeyCase valueOf(int value) {
      switch (value) {
        case 1: return START_KEY_CLOSED;
        case 2: return START_KEY_OPEN;
        case 0: return STARTKEY_NOT_SET;
        default: throw new java.lang.IllegalArgumentException(
          "Value is undefined for this oneof enum.");
      }
    }
    public int getNumber() {
      return this.value;
    }
  };

  public StartKeyCase
  getStartKeyCase() {
    return StartKeyCase.valueOf(
        startKeyCase_);
  }

  private int endKeyCase_ = 0;
  private java.lang.Object endKey_;
  public enum EndKeyCase
      implements com.google.protobuf.Internal.EnumLite {
    END_KEY_OPEN(3),
    END_KEY_CLOSED(4),
    ENDKEY_NOT_SET(0);
    private int value = 0;
    private EndKeyCase(int value) {
      this.value = value;
    }
    public static EndKeyCase valueOf(int value) {
      switch (value) {
        case 3: return END_KEY_OPEN;
        case 4: return END_KEY_CLOSED;
        case 0: return ENDKEY_NOT_SET;
        default: throw new java.lang.IllegalArgumentException(
          "Value is undefined for this oneof enum.");
      }
    }
    public int getNumber() {
      return this.value;
    }
  };

  public EndKeyCase
  getEndKeyCase() {
    return EndKeyCase.valueOf(
        endKeyCase_);
  }

  public static final int START_KEY_CLOSED_FIELD_NUMBER = 1;
  /**
   * <code>optional bytes start_key_closed = 1;</code>
   *
   * <pre>
   * Used when giving an inclusive lower bound for the range.
   * </pre>
   */
  public com.google.protobuf.ByteString getStartKeyClosed() {
    if (startKeyCase_ == 1) {
      return (com.google.protobuf.ByteString) startKey_;
    }
    return com.google.protobuf.ByteString.EMPTY;
  }

  public static final int START_KEY_OPEN_FIELD_NUMBER = 2;
  /**
   * <code>optional bytes start_key_open = 2;</code>
   *
   * <pre>
   * Used when giving an exclusive lower bound for the range.
   * </pre>
   */
  public com.google.protobuf.ByteString getStartKeyOpen() {
    if (startKeyCase_ == 2) {
      return (com.google.protobuf.ByteString) startKey_;
    }
    return com.google.protobuf.ByteString.EMPTY;
  }

  public static final int END_KEY_OPEN_FIELD_NUMBER = 3;
  /**
   * <code>optional bytes end_key_open = 3;</code>
   *
   * <pre>
   * Used when giving an exclusive upper bound for the range.
   * </pre>
   */
  public com.google.protobuf.ByteString getEndKeyOpen() {
    if (endKeyCase_ == 3) {
      return (com.google.protobuf.ByteString) endKey_;
    }
    return com.google.protobuf.ByteString.EMPTY;
  }

  public static final int END_KEY_CLOSED_FIELD_NUMBER = 4;
  /**
   * <code>optional bytes end_key_closed = 4;</code>
   *
   * <pre>
   * Used when giving an inclusive upper bound for the range.
   * </pre>
   */
  public com.google.protobuf.ByteString getEndKeyClosed() {
    if (endKeyCase_ == 4) {
      return (com.google.protobuf.ByteString) endKey_;
    }
    return com.google.protobuf.ByteString.EMPTY;
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (startKeyCase_ == 1) {
      output.writeBytes(
          1, (com.google.protobuf.ByteString)((com.google.protobuf.ByteString) startKey_));
    }
    if (startKeyCase_ == 2) {
      output.writeBytes(
          2, (com.google.protobuf.ByteString)((com.google.protobuf.ByteString) startKey_));
    }
    if (endKeyCase_ == 3) {
      output.writeBytes(
          3, (com.google.protobuf.ByteString)((com.google.protobuf.ByteString) endKey_));
    }
    if (endKeyCase_ == 4) {
      output.writeBytes(
          4, (com.google.protobuf.ByteString)((com.google.protobuf.ByteString) endKey_));
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (startKeyCase_ == 1) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(
            1, (com.google.protobuf.ByteString)((com.google.protobuf.ByteString) startKey_));
    }
    if (startKeyCase_ == 2) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(
            2, (com.google.protobuf.ByteString)((com.google.protobuf.ByteString) startKey_));
    }
    if (endKeyCase_ == 3) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(
            3, (com.google.protobuf.ByteString)((com.google.protobuf.ByteString) endKey_));
    }
    if (endKeyCase_ == 4) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(
            4, (com.google.protobuf.ByteString)((com.google.protobuf.ByteString) endKey_));
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  public static com.google.bigtable.v2.RowRange parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.bigtable.v2.RowRange parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.bigtable.v2.RowRange parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.bigtable.v2.RowRange parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.bigtable.v2.RowRange parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return PARSER.parseFrom(input);
  }
  public static com.google.bigtable.v2.RowRange parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseFrom(input, extensionRegistry);
  }
  public static com.google.bigtable.v2.RowRange parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return PARSER.parseDelimitedFrom(input);
  }
  public static com.google.bigtable.v2.RowRange parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseDelimitedFrom(input, extensionRegistry);
  }
  public static com.google.bigtable.v2.RowRange parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return PARSER.parseFrom(input);
  }
  public static com.google.bigtable.v2.RowRange parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseFrom(input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.google.bigtable.v2.RowRange prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessage.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code google.bigtable.v2.RowRange}
   *
   * <pre>
   * Specifies a contiguous range of rows.
   * </pre>
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessage.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:google.bigtable.v2.RowRange)
      com.google.bigtable.v2.RowRangeOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_RowRange_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_RowRange_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.google.bigtable.v2.RowRange.class, com.google.bigtable.v2.RowRange.Builder.class);
    }

    // Construct using com.google.bigtable.v2.RowRange.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
      }
    }
    public Builder clear() {
      super.clear();
      startKeyCase_ = 0;
      startKey_ = null;
      endKeyCase_ = 0;
      endKey_ = null;
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_RowRange_descriptor;
    }

    public com.google.bigtable.v2.RowRange getDefaultInstanceForType() {
      return com.google.bigtable.v2.RowRange.getDefaultInstance();
    }

    public com.google.bigtable.v2.RowRange build() {
      com.google.bigtable.v2.RowRange result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public com.google.bigtable.v2.RowRange buildPartial() {
      com.google.bigtable.v2.RowRange result = new com.google.bigtable.v2.RowRange(this);
      if (startKeyCase_ == 1) {
        result.startKey_ = startKey_;
      }
      if (startKeyCase_ == 2) {
        result.startKey_ = startKey_;
      }
      if (endKeyCase_ == 3) {
        result.endKey_ = endKey_;
      }
      if (endKeyCase_ == 4) {
        result.endKey_ = endKey_;
      }
      result.startKeyCase_ = startKeyCase_;
      result.endKeyCase_ = endKeyCase_;
      onBuilt();
      return result;
    }

    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.google.bigtable.v2.RowRange) {
        return mergeFrom((com.google.bigtable.v2.RowRange)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.google.bigtable.v2.RowRange other) {
      if (other == com.google.bigtable.v2.RowRange.getDefaultInstance()) return this;
      switch (other.getStartKeyCase()) {
        case START_KEY_CLOSED: {
          setStartKeyClosed(other.getStartKeyClosed());
          break;
        }
        case START_KEY_OPEN: {
          setStartKeyOpen(other.getStartKeyOpen());
          break;
        }
        case STARTKEY_NOT_SET: {
          break;
        }
      }
      switch (other.getEndKeyCase()) {
        case END_KEY_OPEN: {
          setEndKeyOpen(other.getEndKeyOpen());
          break;
        }
        case END_KEY_CLOSED: {
          setEndKeyClosed(other.getEndKeyClosed());
          break;
        }
        case ENDKEY_NOT_SET: {
          break;
        }
      }
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      com.google.bigtable.v2.RowRange parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.google.bigtable.v2.RowRange) e.getUnfinishedMessage();
        throw e;
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int startKeyCase_ = 0;
    private java.lang.Object startKey_;
    public StartKeyCase
        getStartKeyCase() {
      return StartKeyCase.valueOf(
          startKeyCase_);
    }

    public Builder clearStartKey() {
      startKeyCase_ = 0;
      startKey_ = null;
      onChanged();
      return this;
    }

    private int endKeyCase_ = 0;
    private java.lang.Object endKey_;
    public EndKeyCase
        getEndKeyCase() {
      return EndKeyCase.valueOf(
          endKeyCase_);
    }

    public Builder clearEndKey() {
      endKeyCase_ = 0;
      endKey_ = null;
      onChanged();
      return this;
    }


    /**
     * <code>optional bytes start_key_closed = 1;</code>
     *
     * <pre>
     * Used when giving an inclusive lower bound for the range.
     * </pre>
     */
    public com.google.protobuf.ByteString getStartKeyClosed() {
      if (startKeyCase_ == 1) {
        return (com.google.protobuf.ByteString) startKey_;
      }
      return com.google.protobuf.ByteString.EMPTY;
    }
    /**
     * <code>optional bytes start_key_closed = 1;</code>
     *
     * <pre>
     * Used when giving an inclusive lower bound for the range.
     * </pre>
     */
    public Builder setStartKeyClosed(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  startKeyCase_ = 1;
      startKey_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes start_key_closed = 1;</code>
     *
     * <pre>
     * Used when giving an inclusive lower bound for the range.
     * </pre>
     */
    public Builder clearStartKeyClosed() {
      if (startKeyCase_ == 1) {
        startKeyCase_ = 0;
        startKey_ = null;
        onChanged();
      }
      return this;
    }

    /**
     * <code>optional bytes start_key_open = 2;</code>
     *
     * <pre>
     * Used when giving an exclusive lower bound for the range.
     * </pre>
     */
    public com.google.protobuf.ByteString getStartKeyOpen() {
      if (startKeyCase_ == 2) {
        return (com.google.protobuf.ByteString) startKey_;
      }
      return com.google.protobuf.ByteString.EMPTY;
    }
    /**
     * <code>optional bytes start_key_open = 2;</code>
     *
     * <pre>
     * Used when giving an exclusive lower bound for the range.
     * </pre>
     */
    public Builder setStartKeyOpen(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  startKeyCase_ = 2;
      startKey_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes start_key_open = 2;</code>
     *
     * <pre>
     * Used when giving an exclusive lower bound for the range.
     * </pre>
     */
    public Builder clearStartKeyOpen() {
      if (startKeyCase_ == 2) {
        startKeyCase_ = 0;
        startKey_ = null;
        onChanged();
      }
      return this;
    }

    /**
     * <code>optional bytes end_key_open = 3;</code>
     *
     * <pre>
     * Used when giving an exclusive upper bound for the range.
     * </pre>
     */
    public com.google.protobuf.ByteString getEndKeyOpen() {
      if (endKeyCase_ == 3) {
        return (com.google.protobuf.ByteString) endKey_;
      }
      return com.google.protobuf.ByteString.EMPTY;
    }
    /**
     * <code>optional bytes end_key_open = 3;</code>
     *
     * <pre>
     * Used when giving an exclusive upper bound for the range.
     * </pre>
     */
    public Builder setEndKeyOpen(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  endKeyCase_ = 3;
      endKey_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes end_key_open = 3;</code>
     *
     * <pre>
     * Used when giving an exclusive upper bound for the range.
     * </pre>
     */
    public Builder clearEndKeyOpen() {
      if (endKeyCase_ == 3) {
        endKeyCase_ = 0;
        endKey_ = null;
        onChanged();
      }
      return this;
    }

    /**
     * <code>optional bytes end_key_closed = 4;</code>
     *
     * <pre>
     * Used when giving an inclusive upper bound for the range.
     * </pre>
     */
    public com.google.protobuf.ByteString getEndKeyClosed() {
      if (endKeyCase_ == 4) {
        return (com.google.protobuf.ByteString) endKey_;
      }
      return com.google.protobuf.ByteString.EMPTY;
    }
    /**
     * <code>optional bytes end_key_closed = 4;</code>
     *
     * <pre>
     * Used when giving an inclusive upper bound for the range.
     * </pre>
     */
    public Builder setEndKeyClosed(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  endKeyCase_ = 4;
      endKey_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes end_key_closed = 4;</code>
     *
     * <pre>
     * Used when giving an inclusive upper bound for the range.
     * </pre>
     */
    public Builder clearEndKeyClosed() {
      if (endKeyCase_ == 4) {
        endKeyCase_ = 0;
        endKey_ = null;
        onChanged();
      }
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:google.bigtable.v2.RowRange)
  }

  // @@protoc_insertion_point(class_scope:google.bigtable.v2.RowRange)
  private static final com.google.bigtable.v2.RowRange DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.google.bigtable.v2.RowRange();
  }

  public static com.google.bigtable.v2.RowRange getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<RowRange>
      PARSER = new com.google.protobuf.AbstractParser<RowRange>() {
    public RowRange parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      try {
        return new RowRange(input, extensionRegistry);
      } catch (RuntimeException e) {
        if (e.getCause() instanceof
            com.google.protobuf.InvalidProtocolBufferException) {
          throw (com.google.protobuf.InvalidProtocolBufferException)
              e.getCause();
        }
        throw e;
      }
    }
  };

  public static com.google.protobuf.Parser<RowRange> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<RowRange> getParserForType() {
    return PARSER;
  }

  public com.google.bigtable.v2.RowRange getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

