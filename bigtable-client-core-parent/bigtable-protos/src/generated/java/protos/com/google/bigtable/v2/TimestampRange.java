// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/v2/data.proto

package com.google.bigtable.v2;

/**
 * Protobuf type {@code google.bigtable.v2.TimestampRange}
 *
 * <pre>
 * Specified a contiguous range of microsecond timestamps.
 * </pre>
 */
public  final class TimestampRange extends
    com.google.protobuf.GeneratedMessage implements
    // @@protoc_insertion_point(message_implements:google.bigtable.v2.TimestampRange)
    TimestampRangeOrBuilder {
  // Use TimestampRange.newBuilder() to construct.
  private TimestampRange(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
    super(builder);
  }
  private TimestampRange() {
    startTimestampMicros_ = 0L;
    endTimestampMicros_ = 0L;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private TimestampRange(
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
          case 8: {

            startTimestampMicros_ = input.readInt64();
            break;
          }
          case 16: {

            endTimestampMicros_ = input.readInt64();
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
    return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_TimestampRange_descriptor;
  }

  protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_TimestampRange_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.google.bigtable.v2.TimestampRange.class, com.google.bigtable.v2.TimestampRange.Builder.class);
  }

  public static final int START_TIMESTAMP_MICROS_FIELD_NUMBER = 1;
  private long startTimestampMicros_;
  /**
   * <code>optional int64 start_timestamp_micros = 1;</code>
   *
   * <pre>
   * Inclusive lower bound. If left empty, interpreted as 0.
   * </pre>
   */
  public long getStartTimestampMicros() {
    return startTimestampMicros_;
  }

  public static final int END_TIMESTAMP_MICROS_FIELD_NUMBER = 2;
  private long endTimestampMicros_;
  /**
   * <code>optional int64 end_timestamp_micros = 2;</code>
   *
   * <pre>
   * Exclusive upper bound. If left empty, interpreted as infinity.
   * </pre>
   */
  public long getEndTimestampMicros() {
    return endTimestampMicros_;
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
    if (startTimestampMicros_ != 0L) {
      output.writeInt64(1, startTimestampMicros_);
    }
    if (endTimestampMicros_ != 0L) {
      output.writeInt64(2, endTimestampMicros_);
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (startTimestampMicros_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(1, startTimestampMicros_);
    }
    if (endTimestampMicros_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(2, endTimestampMicros_);
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  public static com.google.bigtable.v2.TimestampRange parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.bigtable.v2.TimestampRange parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.bigtable.v2.TimestampRange parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.bigtable.v2.TimestampRange parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.bigtable.v2.TimestampRange parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return PARSER.parseFrom(input);
  }
  public static com.google.bigtable.v2.TimestampRange parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseFrom(input, extensionRegistry);
  }
  public static com.google.bigtable.v2.TimestampRange parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return PARSER.parseDelimitedFrom(input);
  }
  public static com.google.bigtable.v2.TimestampRange parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseDelimitedFrom(input, extensionRegistry);
  }
  public static com.google.bigtable.v2.TimestampRange parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return PARSER.parseFrom(input);
  }
  public static com.google.bigtable.v2.TimestampRange parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseFrom(input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.google.bigtable.v2.TimestampRange prototype) {
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
   * Protobuf type {@code google.bigtable.v2.TimestampRange}
   *
   * <pre>
   * Specified a contiguous range of microsecond timestamps.
   * </pre>
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessage.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:google.bigtable.v2.TimestampRange)
      com.google.bigtable.v2.TimestampRangeOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_TimestampRange_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_TimestampRange_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.google.bigtable.v2.TimestampRange.class, com.google.bigtable.v2.TimestampRange.Builder.class);
    }

    // Construct using com.google.bigtable.v2.TimestampRange.newBuilder()
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
      startTimestampMicros_ = 0L;

      endTimestampMicros_ = 0L;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.google.bigtable.v2.DataProto.internal_static_google_bigtable_v2_TimestampRange_descriptor;
    }

    public com.google.bigtable.v2.TimestampRange getDefaultInstanceForType() {
      return com.google.bigtable.v2.TimestampRange.getDefaultInstance();
    }

    public com.google.bigtable.v2.TimestampRange build() {
      com.google.bigtable.v2.TimestampRange result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public com.google.bigtable.v2.TimestampRange buildPartial() {
      com.google.bigtable.v2.TimestampRange result = new com.google.bigtable.v2.TimestampRange(this);
      result.startTimestampMicros_ = startTimestampMicros_;
      result.endTimestampMicros_ = endTimestampMicros_;
      onBuilt();
      return result;
    }

    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.google.bigtable.v2.TimestampRange) {
        return mergeFrom((com.google.bigtable.v2.TimestampRange)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.google.bigtable.v2.TimestampRange other) {
      if (other == com.google.bigtable.v2.TimestampRange.getDefaultInstance()) return this;
      if (other.getStartTimestampMicros() != 0L) {
        setStartTimestampMicros(other.getStartTimestampMicros());
      }
      if (other.getEndTimestampMicros() != 0L) {
        setEndTimestampMicros(other.getEndTimestampMicros());
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
      com.google.bigtable.v2.TimestampRange parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.google.bigtable.v2.TimestampRange) e.getUnfinishedMessage();
        throw e;
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private long startTimestampMicros_ ;
    /**
     * <code>optional int64 start_timestamp_micros = 1;</code>
     *
     * <pre>
     * Inclusive lower bound. If left empty, interpreted as 0.
     * </pre>
     */
    public long getStartTimestampMicros() {
      return startTimestampMicros_;
    }
    /**
     * <code>optional int64 start_timestamp_micros = 1;</code>
     *
     * <pre>
     * Inclusive lower bound. If left empty, interpreted as 0.
     * </pre>
     */
    public Builder setStartTimestampMicros(long value) {
      
      startTimestampMicros_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 start_timestamp_micros = 1;</code>
     *
     * <pre>
     * Inclusive lower bound. If left empty, interpreted as 0.
     * </pre>
     */
    public Builder clearStartTimestampMicros() {
      
      startTimestampMicros_ = 0L;
      onChanged();
      return this;
    }

    private long endTimestampMicros_ ;
    /**
     * <code>optional int64 end_timestamp_micros = 2;</code>
     *
     * <pre>
     * Exclusive upper bound. If left empty, interpreted as infinity.
     * </pre>
     */
    public long getEndTimestampMicros() {
      return endTimestampMicros_;
    }
    /**
     * <code>optional int64 end_timestamp_micros = 2;</code>
     *
     * <pre>
     * Exclusive upper bound. If left empty, interpreted as infinity.
     * </pre>
     */
    public Builder setEndTimestampMicros(long value) {
      
      endTimestampMicros_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 end_timestamp_micros = 2;</code>
     *
     * <pre>
     * Exclusive upper bound. If left empty, interpreted as infinity.
     * </pre>
     */
    public Builder clearEndTimestampMicros() {
      
      endTimestampMicros_ = 0L;
      onChanged();
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


    // @@protoc_insertion_point(builder_scope:google.bigtable.v2.TimestampRange)
  }

  // @@protoc_insertion_point(class_scope:google.bigtable.v2.TimestampRange)
  private static final com.google.bigtable.v2.TimestampRange DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.google.bigtable.v2.TimestampRange();
  }

  public static com.google.bigtable.v2.TimestampRange getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<TimestampRange>
      PARSER = new com.google.protobuf.AbstractParser<TimestampRange>() {
    public TimestampRange parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      try {
        return new TimestampRange(input, extensionRegistry);
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

  public static com.google.protobuf.Parser<TimestampRange> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<TimestampRange> getParserForType() {
    return PARSER;
  }

  public com.google.bigtable.v2.TimestampRange getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

