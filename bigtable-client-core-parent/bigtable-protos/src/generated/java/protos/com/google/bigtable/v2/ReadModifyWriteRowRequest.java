// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/v2/bigtable.proto

package com.google.bigtable.v2;

/**
 * <pre>
 * Request message for Bigtable.ReadModifyWriteRow.
 * </pre>
 *
 * Protobuf type {@code google.bigtable.v2.ReadModifyWriteRowRequest}
 */
public  final class ReadModifyWriteRowRequest extends
    com.google.protobuf.GeneratedMessage implements
    // @@protoc_insertion_point(message_implements:google.bigtable.v2.ReadModifyWriteRowRequest)
    ReadModifyWriteRowRequestOrBuilder {
  // Use ReadModifyWriteRowRequest.newBuilder() to construct.
  private ReadModifyWriteRowRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
    super(builder);
  }
  private ReadModifyWriteRowRequest() {
    tableName_ = "";
    rowKey_ = com.google.protobuf.ByteString.EMPTY;
    rules_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private ReadModifyWriteRowRequest(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
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
            java.lang.String s = input.readStringRequireUtf8();

            tableName_ = s;
            break;
          }
          case 18: {

            rowKey_ = input.readBytes();
            break;
          }
          case 26: {
            if (!((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
              rules_ = new java.util.ArrayList<com.google.bigtable.v2.ReadModifyWriteRule>();
              mutable_bitField0_ |= 0x00000004;
            }
            rules_.add(input.readMessage(com.google.bigtable.v2.ReadModifyWriteRule.parser(), extensionRegistry));
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      if (((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
        rules_ = java.util.Collections.unmodifiableList(rules_);
      }
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.google.bigtable.v2.BigtableProto.internal_static_google_bigtable_v2_ReadModifyWriteRowRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.google.bigtable.v2.BigtableProto.internal_static_google_bigtable_v2_ReadModifyWriteRowRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.google.bigtable.v2.ReadModifyWriteRowRequest.class, com.google.bigtable.v2.ReadModifyWriteRowRequest.Builder.class);
  }

  private int bitField0_;
  public static final int TABLE_NAME_FIELD_NUMBER = 1;
  private volatile java.lang.Object tableName_;
  /**
   * <pre>
   * The unique name of the table to which the read/modify/write rules should be
   * applied.
   * Values are of the form
   * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
   * </pre>
   *
   * <code>optional string table_name = 1;</code>
   */
  public java.lang.String getTableName() {
    java.lang.Object ref = tableName_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      tableName_ = s;
      return s;
    }
  }
  /**
   * <pre>
   * The unique name of the table to which the read/modify/write rules should be
   * applied.
   * Values are of the form
   * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
   * </pre>
   *
   * <code>optional string table_name = 1;</code>
   */
  public com.google.protobuf.ByteString
      getTableNameBytes() {
    java.lang.Object ref = tableName_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      tableName_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int ROW_KEY_FIELD_NUMBER = 2;
  private com.google.protobuf.ByteString rowKey_;
  /**
   * <pre>
   * The key of the row to which the read/modify/write rules should be applied.
   * </pre>
   *
   * <code>optional bytes row_key = 2;</code>
   */
  public com.google.protobuf.ByteString getRowKey() {
    return rowKey_;
  }

  public static final int RULES_FIELD_NUMBER = 3;
  private java.util.List<com.google.bigtable.v2.ReadModifyWriteRule> rules_;
  /**
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   *
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   */
  public java.util.List<com.google.bigtable.v2.ReadModifyWriteRule> getRulesList() {
    return rules_;
  }
  /**
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   *
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   */
  public java.util.List<? extends com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder> 
      getRulesOrBuilderList() {
    return rules_;
  }
  /**
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   *
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   */
  public int getRulesCount() {
    return rules_.size();
  }
  /**
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   *
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   */
  public com.google.bigtable.v2.ReadModifyWriteRule getRules(int index) {
    return rules_.get(index);
  }
  /**
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   *
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   */
  public com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder getRulesOrBuilder(
      int index) {
    return rules_.get(index);
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
    if (!getTableNameBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessage.writeString(output, 1, tableName_);
    }
    if (!rowKey_.isEmpty()) {
      output.writeBytes(2, rowKey_);
    }
    for (int i = 0; i < rules_.size(); i++) {
      output.writeMessage(3, rules_.get(i));
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getTableNameBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessage.computeStringSize(1, tableName_);
    }
    if (!rowKey_.isEmpty()) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(2, rowKey_);
    }
    for (int i = 0; i < rules_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, rules_.get(i));
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseWithIOException(PARSER, input);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseWithIOException(PARSER, input);
  }
  public static com.google.bigtable.v2.ReadModifyWriteRowRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.google.bigtable.v2.ReadModifyWriteRowRequest prototype) {
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
   * <pre>
   * Request message for Bigtable.ReadModifyWriteRow.
   * </pre>
   *
   * Protobuf type {@code google.bigtable.v2.ReadModifyWriteRowRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessage.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:google.bigtable.v2.ReadModifyWriteRowRequest)
      com.google.bigtable.v2.ReadModifyWriteRowRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.google.bigtable.v2.BigtableProto.internal_static_google_bigtable_v2_ReadModifyWriteRowRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.google.bigtable.v2.BigtableProto.internal_static_google_bigtable_v2_ReadModifyWriteRowRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.google.bigtable.v2.ReadModifyWriteRowRequest.class, com.google.bigtable.v2.ReadModifyWriteRowRequest.Builder.class);
    }

    // Construct using com.google.bigtable.v2.ReadModifyWriteRowRequest.newBuilder()
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
        getRulesFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      tableName_ = "";

      rowKey_ = com.google.protobuf.ByteString.EMPTY;

      if (rulesBuilder_ == null) {
        rules_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
      } else {
        rulesBuilder_.clear();
      }
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.google.bigtable.v2.BigtableProto.internal_static_google_bigtable_v2_ReadModifyWriteRowRequest_descriptor;
    }

    public com.google.bigtable.v2.ReadModifyWriteRowRequest getDefaultInstanceForType() {
      return com.google.bigtable.v2.ReadModifyWriteRowRequest.getDefaultInstance();
    }

    public com.google.bigtable.v2.ReadModifyWriteRowRequest build() {
      com.google.bigtable.v2.ReadModifyWriteRowRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public com.google.bigtable.v2.ReadModifyWriteRowRequest buildPartial() {
      com.google.bigtable.v2.ReadModifyWriteRowRequest result = new com.google.bigtable.v2.ReadModifyWriteRowRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      result.tableName_ = tableName_;
      result.rowKey_ = rowKey_;
      if (rulesBuilder_ == null) {
        if (((bitField0_ & 0x00000004) == 0x00000004)) {
          rules_ = java.util.Collections.unmodifiableList(rules_);
          bitField0_ = (bitField0_ & ~0x00000004);
        }
        result.rules_ = rules_;
      } else {
        result.rules_ = rulesBuilder_.build();
      }
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.google.bigtable.v2.ReadModifyWriteRowRequest) {
        return mergeFrom((com.google.bigtable.v2.ReadModifyWriteRowRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.google.bigtable.v2.ReadModifyWriteRowRequest other) {
      if (other == com.google.bigtable.v2.ReadModifyWriteRowRequest.getDefaultInstance()) return this;
      if (!other.getTableName().isEmpty()) {
        tableName_ = other.tableName_;
        onChanged();
      }
      if (other.getRowKey() != com.google.protobuf.ByteString.EMPTY) {
        setRowKey(other.getRowKey());
      }
      if (rulesBuilder_ == null) {
        if (!other.rules_.isEmpty()) {
          if (rules_.isEmpty()) {
            rules_ = other.rules_;
            bitField0_ = (bitField0_ & ~0x00000004);
          } else {
            ensureRulesIsMutable();
            rules_.addAll(other.rules_);
          }
          onChanged();
        }
      } else {
        if (!other.rules_.isEmpty()) {
          if (rulesBuilder_.isEmpty()) {
            rulesBuilder_.dispose();
            rulesBuilder_ = null;
            rules_ = other.rules_;
            bitField0_ = (bitField0_ & ~0x00000004);
            rulesBuilder_ = 
              com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders ?
                 getRulesFieldBuilder() : null;
          } else {
            rulesBuilder_.addAllMessages(other.rules_);
          }
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
      com.google.bigtable.v2.ReadModifyWriteRowRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.google.bigtable.v2.ReadModifyWriteRowRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object tableName_ = "";
    /**
     * <pre>
     * The unique name of the table to which the read/modify/write rules should be
     * applied.
     * Values are of the form
     * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
     * </pre>
     *
     * <code>optional string table_name = 1;</code>
     */
    public java.lang.String getTableName() {
      java.lang.Object ref = tableName_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        tableName_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <pre>
     * The unique name of the table to which the read/modify/write rules should be
     * applied.
     * Values are of the form
     * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
     * </pre>
     *
     * <code>optional string table_name = 1;</code>
     */
    public com.google.protobuf.ByteString
        getTableNameBytes() {
      java.lang.Object ref = tableName_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        tableName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     * The unique name of the table to which the read/modify/write rules should be
     * applied.
     * Values are of the form
     * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
     * </pre>
     *
     * <code>optional string table_name = 1;</code>
     */
    public Builder setTableName(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      tableName_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * The unique name of the table to which the read/modify/write rules should be
     * applied.
     * Values are of the form
     * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
     * </pre>
     *
     * <code>optional string table_name = 1;</code>
     */
    public Builder clearTableName() {
      
      tableName_ = getDefaultInstance().getTableName();
      onChanged();
      return this;
    }
    /**
     * <pre>
     * The unique name of the table to which the read/modify/write rules should be
     * applied.
     * Values are of the form
     * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
     * </pre>
     *
     * <code>optional string table_name = 1;</code>
     */
    public Builder setTableNameBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      tableName_ = value;
      onChanged();
      return this;
    }

    private com.google.protobuf.ByteString rowKey_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <pre>
     * The key of the row to which the read/modify/write rules should be applied.
     * </pre>
     *
     * <code>optional bytes row_key = 2;</code>
     */
    public com.google.protobuf.ByteString getRowKey() {
      return rowKey_;
    }
    /**
     * <pre>
     * The key of the row to which the read/modify/write rules should be applied.
     * </pre>
     *
     * <code>optional bytes row_key = 2;</code>
     */
    public Builder setRowKey(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      rowKey_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * The key of the row to which the read/modify/write rules should be applied.
     * </pre>
     *
     * <code>optional bytes row_key = 2;</code>
     */
    public Builder clearRowKey() {
      
      rowKey_ = getDefaultInstance().getRowKey();
      onChanged();
      return this;
    }

    private java.util.List<com.google.bigtable.v2.ReadModifyWriteRule> rules_ =
      java.util.Collections.emptyList();
    private void ensureRulesIsMutable() {
      if (!((bitField0_ & 0x00000004) == 0x00000004)) {
        rules_ = new java.util.ArrayList<com.google.bigtable.v2.ReadModifyWriteRule>(rules_);
        bitField0_ |= 0x00000004;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilder<
        com.google.bigtable.v2.ReadModifyWriteRule, com.google.bigtable.v2.ReadModifyWriteRule.Builder, com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder> rulesBuilder_;

    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public java.util.List<com.google.bigtable.v2.ReadModifyWriteRule> getRulesList() {
      if (rulesBuilder_ == null) {
        return java.util.Collections.unmodifiableList(rules_);
      } else {
        return rulesBuilder_.getMessageList();
      }
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public int getRulesCount() {
      if (rulesBuilder_ == null) {
        return rules_.size();
      } else {
        return rulesBuilder_.getCount();
      }
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public com.google.bigtable.v2.ReadModifyWriteRule getRules(int index) {
      if (rulesBuilder_ == null) {
        return rules_.get(index);
      } else {
        return rulesBuilder_.getMessage(index);
      }
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder setRules(
        int index, com.google.bigtable.v2.ReadModifyWriteRule value) {
      if (rulesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureRulesIsMutable();
        rules_.set(index, value);
        onChanged();
      } else {
        rulesBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder setRules(
        int index, com.google.bigtable.v2.ReadModifyWriteRule.Builder builderForValue) {
      if (rulesBuilder_ == null) {
        ensureRulesIsMutable();
        rules_.set(index, builderForValue.build());
        onChanged();
      } else {
        rulesBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder addRules(com.google.bigtable.v2.ReadModifyWriteRule value) {
      if (rulesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureRulesIsMutable();
        rules_.add(value);
        onChanged();
      } else {
        rulesBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder addRules(
        int index, com.google.bigtable.v2.ReadModifyWriteRule value) {
      if (rulesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureRulesIsMutable();
        rules_.add(index, value);
        onChanged();
      } else {
        rulesBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder addRules(
        com.google.bigtable.v2.ReadModifyWriteRule.Builder builderForValue) {
      if (rulesBuilder_ == null) {
        ensureRulesIsMutable();
        rules_.add(builderForValue.build());
        onChanged();
      } else {
        rulesBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder addRules(
        int index, com.google.bigtable.v2.ReadModifyWriteRule.Builder builderForValue) {
      if (rulesBuilder_ == null) {
        ensureRulesIsMutable();
        rules_.add(index, builderForValue.build());
        onChanged();
      } else {
        rulesBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder addAllRules(
        java.lang.Iterable<? extends com.google.bigtable.v2.ReadModifyWriteRule> values) {
      if (rulesBuilder_ == null) {
        ensureRulesIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, rules_);
        onChanged();
      } else {
        rulesBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder clearRules() {
      if (rulesBuilder_ == null) {
        rules_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
      } else {
        rulesBuilder_.clear();
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public Builder removeRules(int index) {
      if (rulesBuilder_ == null) {
        ensureRulesIsMutable();
        rules_.remove(index);
        onChanged();
      } else {
        rulesBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public com.google.bigtable.v2.ReadModifyWriteRule.Builder getRulesBuilder(
        int index) {
      return getRulesFieldBuilder().getBuilder(index);
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder getRulesOrBuilder(
        int index) {
      if (rulesBuilder_ == null) {
        return rules_.get(index);  } else {
        return rulesBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public java.util.List<? extends com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder> 
         getRulesOrBuilderList() {
      if (rulesBuilder_ != null) {
        return rulesBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(rules_);
      }
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public com.google.bigtable.v2.ReadModifyWriteRule.Builder addRulesBuilder() {
      return getRulesFieldBuilder().addBuilder(
          com.google.bigtable.v2.ReadModifyWriteRule.getDefaultInstance());
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public com.google.bigtable.v2.ReadModifyWriteRule.Builder addRulesBuilder(
        int index) {
      return getRulesFieldBuilder().addBuilder(
          index, com.google.bigtable.v2.ReadModifyWriteRule.getDefaultInstance());
    }
    /**
     * <pre>
     * Rules specifying how the specified row's contents are to be transformed
     * into writes. Entries are applied in order, meaning that earlier rules will
     * affect the results of later ones.
     * </pre>
     *
     * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
     */
    public java.util.List<com.google.bigtable.v2.ReadModifyWriteRule.Builder> 
         getRulesBuilderList() {
      return getRulesFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilder<
        com.google.bigtable.v2.ReadModifyWriteRule, com.google.bigtable.v2.ReadModifyWriteRule.Builder, com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder> 
        getRulesFieldBuilder() {
      if (rulesBuilder_ == null) {
        rulesBuilder_ = new com.google.protobuf.RepeatedFieldBuilder<
            com.google.bigtable.v2.ReadModifyWriteRule, com.google.bigtable.v2.ReadModifyWriteRule.Builder, com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder>(
                rules_,
                ((bitField0_ & 0x00000004) == 0x00000004),
                getParentForChildren(),
                isClean());
        rules_ = null;
      }
      return rulesBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:google.bigtable.v2.ReadModifyWriteRowRequest)
  }

  // @@protoc_insertion_point(class_scope:google.bigtable.v2.ReadModifyWriteRowRequest)
  private static final com.google.bigtable.v2.ReadModifyWriteRowRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.google.bigtable.v2.ReadModifyWriteRowRequest();
  }

  public static com.google.bigtable.v2.ReadModifyWriteRowRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<ReadModifyWriteRowRequest>
      PARSER = new com.google.protobuf.AbstractParser<ReadModifyWriteRowRequest>() {
    public ReadModifyWriteRowRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new ReadModifyWriteRowRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<ReadModifyWriteRowRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<ReadModifyWriteRowRequest> getParserForType() {
    return PARSER;
  }

  public com.google.bigtable.v2.ReadModifyWriteRowRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

