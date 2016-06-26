// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/admin/v2/bigtable_table_admin.proto

package com.google.bigtable.admin.v2;

/**
 * Protobuf type {@code google.bigtable.admin.v2.ListTablesRequest}
 *
 * <pre>
 * Request message for [google.bigtable.admin.v2.BigtableTableAdmin.ListTables][google.bigtable.admin.v2.BigtableTableAdmin.ListTables]
 * </pre>
 */
public  final class ListTablesRequest extends
    com.google.protobuf.GeneratedMessage implements
    // @@protoc_insertion_point(message_implements:google.bigtable.admin.v2.ListTablesRequest)
    ListTablesRequestOrBuilder {
  // Use ListTablesRequest.newBuilder() to construct.
  private ListTablesRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
    super(builder);
  }
  private ListTablesRequest() {
    parent_ = "";
    view_ = 0;
    pageToken_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private ListTablesRequest(
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
            String s = input.readStringRequireUtf8();

            parent_ = s;
            break;
          }
          case 16: {
            int rawValue = input.readEnum();

            view_ = rawValue;
            break;
          }
          case 26: {
            String s = input.readStringRequireUtf8();

            pageToken_ = s;
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
    return com.google.bigtable.admin.v2.BigtableTableAdminProto.internal_static_google_bigtable_admin_v2_ListTablesRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.google.bigtable.admin.v2.BigtableTableAdminProto.internal_static_google_bigtable_admin_v2_ListTablesRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.google.bigtable.admin.v2.ListTablesRequest.class, com.google.bigtable.admin.v2.ListTablesRequest.Builder.class);
  }

  public static final int PARENT_FIELD_NUMBER = 1;
  private volatile java.lang.Object parent_;
  /**
   * <code>optional string parent = 1;</code>
   *
   * <pre>
   * The unique name of the instance for which tables should be listed.
   * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
   * </pre>
   */
  public java.lang.String getParent() {
    java.lang.Object ref = parent_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      parent_ = s;
      return s;
    }
  }
  /**
   * <code>optional string parent = 1;</code>
   *
   * <pre>
   * The unique name of the instance for which tables should be listed.
   * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
   * </pre>
   */
  public com.google.protobuf.ByteString
      getParentBytes() {
    java.lang.Object ref = parent_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      parent_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int VIEW_FIELD_NUMBER = 2;
  private int view_;
  /**
   * <code>optional .google.bigtable.admin.v2.Table.View view = 2;</code>
   *
   * <pre>
   * The view to be applied to the returned tables' fields.
   * Defaults to NAME_ONLY if unspecified (no others are currently supported).
   * </pre>
   */
  public int getViewValue() {
    return view_;
  }
  /**
   * <code>optional .google.bigtable.admin.v2.Table.View view = 2;</code>
   *
   * <pre>
   * The view to be applied to the returned tables' fields.
   * Defaults to NAME_ONLY if unspecified (no others are currently supported).
   * </pre>
   */
  public com.google.bigtable.admin.v2.Table.View getView() {
    com.google.bigtable.admin.v2.Table.View result = com.google.bigtable.admin.v2.Table.View.valueOf(view_);
    return result == null ? com.google.bigtable.admin.v2.Table.View.UNRECOGNIZED : result;
  }

  public static final int PAGE_TOKEN_FIELD_NUMBER = 3;
  private volatile java.lang.Object pageToken_;
  /**
   * <code>optional string page_token = 3;</code>
   *
   * <pre>
   * The value of `next_page_token` returned by a previous call.
   * </pre>
   */
  public java.lang.String getPageToken() {
    java.lang.Object ref = pageToken_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      pageToken_ = s;
      return s;
    }
  }
  /**
   * <code>optional string page_token = 3;</code>
   *
   * <pre>
   * The value of `next_page_token` returned by a previous call.
   * </pre>
   */
  public com.google.protobuf.ByteString
      getPageTokenBytes() {
    java.lang.Object ref = pageToken_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      pageToken_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
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
    if (!getParentBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessage.writeString(output, 1, parent_);
    }
    if (view_ != com.google.bigtable.admin.v2.Table.View.VIEW_UNSPECIFIED.getNumber()) {
      output.writeEnum(2, view_);
    }
    if (!getPageTokenBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessage.writeString(output, 3, pageToken_);
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getParentBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessage.computeStringSize(1, parent_);
    }
    if (view_ != com.google.bigtable.admin.v2.Table.View.VIEW_UNSPECIFIED.getNumber()) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(2, view_);
    }
    if (!getPageTokenBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessage.computeStringSize(3, pageToken_);
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  public static com.google.bigtable.admin.v2.ListTablesRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return PARSER.parseFrom(input);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseFrom(input, extensionRegistry);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return PARSER.parseDelimitedFrom(input);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseDelimitedFrom(input, extensionRegistry);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return PARSER.parseFrom(input);
  }
  public static com.google.bigtable.admin.v2.ListTablesRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return PARSER.parseFrom(input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.google.bigtable.admin.v2.ListTablesRequest prototype) {
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
   * Protobuf type {@code google.bigtable.admin.v2.ListTablesRequest}
   *
   * <pre>
   * Request message for [google.bigtable.admin.v2.BigtableTableAdmin.ListTables][google.bigtable.admin.v2.BigtableTableAdmin.ListTables]
   * </pre>
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessage.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:google.bigtable.admin.v2.ListTablesRequest)
      com.google.bigtable.admin.v2.ListTablesRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.google.bigtable.admin.v2.BigtableTableAdminProto.internal_static_google_bigtable_admin_v2_ListTablesRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.google.bigtable.admin.v2.BigtableTableAdminProto.internal_static_google_bigtable_admin_v2_ListTablesRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.google.bigtable.admin.v2.ListTablesRequest.class, com.google.bigtable.admin.v2.ListTablesRequest.Builder.class);
    }

    // Construct using com.google.bigtable.admin.v2.ListTablesRequest.newBuilder()
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
      parent_ = "";

      view_ = 0;

      pageToken_ = "";

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.google.bigtable.admin.v2.BigtableTableAdminProto.internal_static_google_bigtable_admin_v2_ListTablesRequest_descriptor;
    }

    public com.google.bigtable.admin.v2.ListTablesRequest getDefaultInstanceForType() {
      return com.google.bigtable.admin.v2.ListTablesRequest.getDefaultInstance();
    }

    public com.google.bigtable.admin.v2.ListTablesRequest build() {
      com.google.bigtable.admin.v2.ListTablesRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public com.google.bigtable.admin.v2.ListTablesRequest buildPartial() {
      com.google.bigtable.admin.v2.ListTablesRequest result = new com.google.bigtable.admin.v2.ListTablesRequest(this);
      result.parent_ = parent_;
      result.view_ = view_;
      result.pageToken_ = pageToken_;
      onBuilt();
      return result;
    }

    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.google.bigtable.admin.v2.ListTablesRequest) {
        return mergeFrom((com.google.bigtable.admin.v2.ListTablesRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.google.bigtable.admin.v2.ListTablesRequest other) {
      if (other == com.google.bigtable.admin.v2.ListTablesRequest.getDefaultInstance()) return this;
      if (!other.getParent().isEmpty()) {
        parent_ = other.parent_;
        onChanged();
      }
      if (other.view_ != 0) {
        setViewValue(other.getViewValue());
      }
      if (!other.getPageToken().isEmpty()) {
        pageToken_ = other.pageToken_;
        onChanged();
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
      com.google.bigtable.admin.v2.ListTablesRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.google.bigtable.admin.v2.ListTablesRequest) e.getUnfinishedMessage();
        throw e;
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private java.lang.Object parent_ = "";
    /**
     * <code>optional string parent = 1;</code>
     *
     * <pre>
     * The unique name of the instance for which tables should be listed.
     * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
     * </pre>
     */
    public java.lang.String getParent() {
      java.lang.Object ref = parent_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        parent_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string parent = 1;</code>
     *
     * <pre>
     * The unique name of the instance for which tables should be listed.
     * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
     * </pre>
     */
    public com.google.protobuf.ByteString
        getParentBytes() {
      java.lang.Object ref = parent_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        parent_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string parent = 1;</code>
     *
     * <pre>
     * The unique name of the instance for which tables should be listed.
     * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
     * </pre>
     */
    public Builder setParent(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      parent_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string parent = 1;</code>
     *
     * <pre>
     * The unique name of the instance for which tables should be listed.
     * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
     * </pre>
     */
    public Builder clearParent() {
      
      parent_ = getDefaultInstance().getParent();
      onChanged();
      return this;
    }
    /**
     * <code>optional string parent = 1;</code>
     *
     * <pre>
     * The unique name of the instance for which tables should be listed.
     * Values are of the form projects/&lt;project&gt;/instances/&lt;instance&gt;
     * </pre>
     */
    public Builder setParentBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      parent_ = value;
      onChanged();
      return this;
    }

    private int view_ = 0;
    /**
     * <code>optional .google.bigtable.admin.v2.Table.View view = 2;</code>
     *
     * <pre>
     * The view to be applied to the returned tables' fields.
     * Defaults to NAME_ONLY if unspecified (no others are currently supported).
     * </pre>
     */
    public int getViewValue() {
      return view_;
    }
    /**
     * <code>optional .google.bigtable.admin.v2.Table.View view = 2;</code>
     *
     * <pre>
     * The view to be applied to the returned tables' fields.
     * Defaults to NAME_ONLY if unspecified (no others are currently supported).
     * </pre>
     */
    public Builder setViewValue(int value) {
      view_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional .google.bigtable.admin.v2.Table.View view = 2;</code>
     *
     * <pre>
     * The view to be applied to the returned tables' fields.
     * Defaults to NAME_ONLY if unspecified (no others are currently supported).
     * </pre>
     */
    public com.google.bigtable.admin.v2.Table.View getView() {
      com.google.bigtable.admin.v2.Table.View result = com.google.bigtable.admin.v2.Table.View.valueOf(view_);
      return result == null ? com.google.bigtable.admin.v2.Table.View.UNRECOGNIZED : result;
    }
    /**
     * <code>optional .google.bigtable.admin.v2.Table.View view = 2;</code>
     *
     * <pre>
     * The view to be applied to the returned tables' fields.
     * Defaults to NAME_ONLY if unspecified (no others are currently supported).
     * </pre>
     */
    public Builder setView(com.google.bigtable.admin.v2.Table.View value) {
      if (value == null) {
        throw new NullPointerException();
      }
      
      view_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .google.bigtable.admin.v2.Table.View view = 2;</code>
     *
     * <pre>
     * The view to be applied to the returned tables' fields.
     * Defaults to NAME_ONLY if unspecified (no others are currently supported).
     * </pre>
     */
    public Builder clearView() {
      
      view_ = 0;
      onChanged();
      return this;
    }

    private java.lang.Object pageToken_ = "";
    /**
     * <code>optional string page_token = 3;</code>
     *
     * <pre>
     * The value of `next_page_token` returned by a previous call.
     * </pre>
     */
    public java.lang.String getPageToken() {
      java.lang.Object ref = pageToken_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        pageToken_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string page_token = 3;</code>
     *
     * <pre>
     * The value of `next_page_token` returned by a previous call.
     * </pre>
     */
    public com.google.protobuf.ByteString
        getPageTokenBytes() {
      java.lang.Object ref = pageToken_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        pageToken_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string page_token = 3;</code>
     *
     * <pre>
     * The value of `next_page_token` returned by a previous call.
     * </pre>
     */
    public Builder setPageToken(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      pageToken_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string page_token = 3;</code>
     *
     * <pre>
     * The value of `next_page_token` returned by a previous call.
     * </pre>
     */
    public Builder clearPageToken() {
      
      pageToken_ = getDefaultInstance().getPageToken();
      onChanged();
      return this;
    }
    /**
     * <code>optional string page_token = 3;</code>
     *
     * <pre>
     * The value of `next_page_token` returned by a previous call.
     * </pre>
     */
    public Builder setPageTokenBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      pageToken_ = value;
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


    // @@protoc_insertion_point(builder_scope:google.bigtable.admin.v2.ListTablesRequest)
  }

  // @@protoc_insertion_point(class_scope:google.bigtable.admin.v2.ListTablesRequest)
  private static final com.google.bigtable.admin.v2.ListTablesRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.google.bigtable.admin.v2.ListTablesRequest();
  }

  public static com.google.bigtable.admin.v2.ListTablesRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<ListTablesRequest>
      PARSER = new com.google.protobuf.AbstractParser<ListTablesRequest>() {
    public ListTablesRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      try {
        return new ListTablesRequest(input, extensionRegistry);
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

  public static com.google.protobuf.Parser<ListTablesRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<ListTablesRequest> getParserForType() {
    return PARSER;
  }

  public com.google.bigtable.admin.v2.ListTablesRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

