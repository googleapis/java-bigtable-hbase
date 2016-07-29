// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/rpc/status.proto

package com.google.rpc;

/**
 * <pre>
 * The `Status` type defines a logical error model that is suitable for different
 * programming environments, including REST APIs and RPC APIs. It is used by
 * [gRPC](https://github.com/grpc). The error model is designed to be:
 * - Simple to use and understand for most users
 * - Flexible enough to meet unexpected needs
 * # Overview
 * The `Status` message contains three pieces of data: error code, error message,
 * and error details. The error code should be an enum value of
 * [google.rpc.Code][google.rpc.Code], but it may accept additional error codes if needed.  The
 * error message should be a developer-facing English message that helps
 * developers *understand* and *resolve* the error. If a localized user-facing
 * error message is needed, put the localized message in the error details or
 * localize it in the client. The optional error details may contain arbitrary
 * information about the error. There is a predefined set of error detail types
 * in the package `google.rpc` which can be used for common error conditions.
 * # Language mapping
 * The `Status` message is the logical representation of the error model, but it
 * is not necessarily the actual wire format. When the `Status` message is
 * exposed in different client libraries and different wire protocols, it can be
 * mapped differently. For example, it will likely be mapped to some exceptions
 * in Java, but more likely mapped to some error codes in C.
 * # Other uses
 * The error model and the `Status` message can be used in a variety of
 * environments, either with or without APIs, to provide a
 * consistent developer experience across different environments.
 * Example uses of this error model include:
 * - Partial errors. If a service needs to return partial errors to the client,
 *     it may embed the `Status` in the normal response to indicate the partial
 *     errors.
 * - Workflow errors. A typical workflow has multiple steps. Each step may
 *     have a `Status` message for error reporting purpose.
 * - Batch operations. If a client uses batch request and batch response, the
 *     `Status` message should be used directly inside batch response, one for
 *     each error sub-response.
 * - Asynchronous operations. If an API call embeds asynchronous operation
 *     results in its response, the status of those operations should be
 *     represented directly using the `Status` message.
 * - Logging. If some API errors are stored in logs, the message `Status` could
 *     be used directly after any stripping needed for security/privacy reasons.
 * </pre>
 *
 * Protobuf type {@code google.rpc.Status}
 */
public  final class Status extends
    com.google.protobuf.GeneratedMessage implements
    // @@protoc_insertion_point(message_implements:google.rpc.Status)
    StatusOrBuilder {
  // Use Status.newBuilder() to construct.
  private Status(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
    super(builder);
  }
  private Status() {
    code_ = 0;
    message_ = "";
    details_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private Status(
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
          case 8: {

            code_ = input.readInt32();
            break;
          }
          case 18: {
            java.lang.String s = input.readStringRequireUtf8();

            message_ = s;
            break;
          }
          case 26: {
            if (!((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
              details_ = new java.util.ArrayList<com.google.protobuf.Any>();
              mutable_bitField0_ |= 0x00000004;
            }
            details_.add(input.readMessage(com.google.protobuf.Any.parser(), extensionRegistry));
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
        details_ = java.util.Collections.unmodifiableList(details_);
      }
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.google.rpc.StatusProto.internal_static_google_rpc_Status_descriptor;
  }

  protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.google.rpc.StatusProto.internal_static_google_rpc_Status_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.google.rpc.Status.class, com.google.rpc.Status.Builder.class);
  }

  private int bitField0_;
  public static final int CODE_FIELD_NUMBER = 1;
  private int code_;
  /**
   * <pre>
   * The status code, which should be an enum value of [google.rpc.Code][google.rpc.Code].
   * </pre>
   *
   * <code>optional int32 code = 1;</code>
   */
  public int getCode() {
    return code_;
  }

  public static final int MESSAGE_FIELD_NUMBER = 2;
  private volatile java.lang.Object message_;
  /**
   * <pre>
   * A developer-facing error message, which should be in English. Any
   * user-facing error message should be localized and sent in the
   * [google.rpc.Status.details][google.rpc.Status.details] field, or localized by the client.
   * </pre>
   *
   * <code>optional string message = 2;</code>
   */
  public java.lang.String getMessage() {
    java.lang.Object ref = message_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      message_ = s;
      return s;
    }
  }
  /**
   * <pre>
   * A developer-facing error message, which should be in English. Any
   * user-facing error message should be localized and sent in the
   * [google.rpc.Status.details][google.rpc.Status.details] field, or localized by the client.
   * </pre>
   *
   * <code>optional string message = 2;</code>
   */
  public com.google.protobuf.ByteString
      getMessageBytes() {
    java.lang.Object ref = message_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      message_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int DETAILS_FIELD_NUMBER = 3;
  private java.util.List<com.google.protobuf.Any> details_;
  /**
   * <pre>
   * A list of messages that carry the error details.  There will be a
   * common set of message types for APIs to use.
   * </pre>
   *
   * <code>repeated .google.protobuf.Any details = 3;</code>
   */
  public java.util.List<com.google.protobuf.Any> getDetailsList() {
    return details_;
  }
  /**
   * <pre>
   * A list of messages that carry the error details.  There will be a
   * common set of message types for APIs to use.
   * </pre>
   *
   * <code>repeated .google.protobuf.Any details = 3;</code>
   */
  public java.util.List<? extends com.google.protobuf.AnyOrBuilder> 
      getDetailsOrBuilderList() {
    return details_;
  }
  /**
   * <pre>
   * A list of messages that carry the error details.  There will be a
   * common set of message types for APIs to use.
   * </pre>
   *
   * <code>repeated .google.protobuf.Any details = 3;</code>
   */
  public int getDetailsCount() {
    return details_.size();
  }
  /**
   * <pre>
   * A list of messages that carry the error details.  There will be a
   * common set of message types for APIs to use.
   * </pre>
   *
   * <code>repeated .google.protobuf.Any details = 3;</code>
   */
  public com.google.protobuf.Any getDetails(int index) {
    return details_.get(index);
  }
  /**
   * <pre>
   * A list of messages that carry the error details.  There will be a
   * common set of message types for APIs to use.
   * </pre>
   *
   * <code>repeated .google.protobuf.Any details = 3;</code>
   */
  public com.google.protobuf.AnyOrBuilder getDetailsOrBuilder(
      int index) {
    return details_.get(index);
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
    if (code_ != 0) {
      output.writeInt32(1, code_);
    }
    if (!getMessageBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessage.writeString(output, 2, message_);
    }
    for (int i = 0; i < details_.size(); i++) {
      output.writeMessage(3, details_.get(i));
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (code_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(1, code_);
    }
    if (!getMessageBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessage.computeStringSize(2, message_);
    }
    for (int i = 0; i < details_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, details_.get(i));
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  public static com.google.rpc.Status parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.rpc.Status parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.rpc.Status parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.google.rpc.Status parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.google.rpc.Status parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseWithIOException(PARSER, input);
  }
  public static com.google.rpc.Status parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.google.rpc.Status parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static com.google.rpc.Status parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.google.rpc.Status parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessage
        .parseWithIOException(PARSER, input);
  }
  public static com.google.rpc.Status parseFrom(
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
  public static Builder newBuilder(com.google.rpc.Status prototype) {
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
   * The `Status` type defines a logical error model that is suitable for different
   * programming environments, including REST APIs and RPC APIs. It is used by
   * [gRPC](https://github.com/grpc). The error model is designed to be:
   * - Simple to use and understand for most users
   * - Flexible enough to meet unexpected needs
   * # Overview
   * The `Status` message contains three pieces of data: error code, error message,
   * and error details. The error code should be an enum value of
   * [google.rpc.Code][google.rpc.Code], but it may accept additional error codes if needed.  The
   * error message should be a developer-facing English message that helps
   * developers *understand* and *resolve* the error. If a localized user-facing
   * error message is needed, put the localized message in the error details or
   * localize it in the client. The optional error details may contain arbitrary
   * information about the error. There is a predefined set of error detail types
   * in the package `google.rpc` which can be used for common error conditions.
   * # Language mapping
   * The `Status` message is the logical representation of the error model, but it
   * is not necessarily the actual wire format. When the `Status` message is
   * exposed in different client libraries and different wire protocols, it can be
   * mapped differently. For example, it will likely be mapped to some exceptions
   * in Java, but more likely mapped to some error codes in C.
   * # Other uses
   * The error model and the `Status` message can be used in a variety of
   * environments, either with or without APIs, to provide a
   * consistent developer experience across different environments.
   * Example uses of this error model include:
   * - Partial errors. If a service needs to return partial errors to the client,
   *     it may embed the `Status` in the normal response to indicate the partial
   *     errors.
   * - Workflow errors. A typical workflow has multiple steps. Each step may
   *     have a `Status` message for error reporting purpose.
   * - Batch operations. If a client uses batch request and batch response, the
   *     `Status` message should be used directly inside batch response, one for
   *     each error sub-response.
   * - Asynchronous operations. If an API call embeds asynchronous operation
   *     results in its response, the status of those operations should be
   *     represented directly using the `Status` message.
   * - Logging. If some API errors are stored in logs, the message `Status` could
   *     be used directly after any stripping needed for security/privacy reasons.
   * </pre>
   *
   * Protobuf type {@code google.rpc.Status}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessage.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:google.rpc.Status)
      com.google.rpc.StatusOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.google.rpc.StatusProto.internal_static_google_rpc_Status_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.google.rpc.StatusProto.internal_static_google_rpc_Status_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.google.rpc.Status.class, com.google.rpc.Status.Builder.class);
    }

    // Construct using com.google.rpc.Status.newBuilder()
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
        getDetailsFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      code_ = 0;

      message_ = "";

      if (detailsBuilder_ == null) {
        details_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
      } else {
        detailsBuilder_.clear();
      }
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.google.rpc.StatusProto.internal_static_google_rpc_Status_descriptor;
    }

    public com.google.rpc.Status getDefaultInstanceForType() {
      return com.google.rpc.Status.getDefaultInstance();
    }

    public com.google.rpc.Status build() {
      com.google.rpc.Status result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public com.google.rpc.Status buildPartial() {
      com.google.rpc.Status result = new com.google.rpc.Status(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      result.code_ = code_;
      result.message_ = message_;
      if (detailsBuilder_ == null) {
        if (((bitField0_ & 0x00000004) == 0x00000004)) {
          details_ = java.util.Collections.unmodifiableList(details_);
          bitField0_ = (bitField0_ & ~0x00000004);
        }
        result.details_ = details_;
      } else {
        result.details_ = detailsBuilder_.build();
      }
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.google.rpc.Status) {
        return mergeFrom((com.google.rpc.Status)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.google.rpc.Status other) {
      if (other == com.google.rpc.Status.getDefaultInstance()) return this;
      if (other.getCode() != 0) {
        setCode(other.getCode());
      }
      if (!other.getMessage().isEmpty()) {
        message_ = other.message_;
        onChanged();
      }
      if (detailsBuilder_ == null) {
        if (!other.details_.isEmpty()) {
          if (details_.isEmpty()) {
            details_ = other.details_;
            bitField0_ = (bitField0_ & ~0x00000004);
          } else {
            ensureDetailsIsMutable();
            details_.addAll(other.details_);
          }
          onChanged();
        }
      } else {
        if (!other.details_.isEmpty()) {
          if (detailsBuilder_.isEmpty()) {
            detailsBuilder_.dispose();
            detailsBuilder_ = null;
            details_ = other.details_;
            bitField0_ = (bitField0_ & ~0x00000004);
            detailsBuilder_ = 
              com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders ?
                 getDetailsFieldBuilder() : null;
          } else {
            detailsBuilder_.addAllMessages(other.details_);
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
      com.google.rpc.Status parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.google.rpc.Status) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private int code_ ;
    /**
     * <pre>
     * The status code, which should be an enum value of [google.rpc.Code][google.rpc.Code].
     * </pre>
     *
     * <code>optional int32 code = 1;</code>
     */
    public int getCode() {
      return code_;
    }
    /**
     * <pre>
     * The status code, which should be an enum value of [google.rpc.Code][google.rpc.Code].
     * </pre>
     *
     * <code>optional int32 code = 1;</code>
     */
    public Builder setCode(int value) {
      
      code_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * The status code, which should be an enum value of [google.rpc.Code][google.rpc.Code].
     * </pre>
     *
     * <code>optional int32 code = 1;</code>
     */
    public Builder clearCode() {
      
      code_ = 0;
      onChanged();
      return this;
    }

    private java.lang.Object message_ = "";
    /**
     * <pre>
     * A developer-facing error message, which should be in English. Any
     * user-facing error message should be localized and sent in the
     * [google.rpc.Status.details][google.rpc.Status.details] field, or localized by the client.
     * </pre>
     *
     * <code>optional string message = 2;</code>
     */
    public java.lang.String getMessage() {
      java.lang.Object ref = message_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        message_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <pre>
     * A developer-facing error message, which should be in English. Any
     * user-facing error message should be localized and sent in the
     * [google.rpc.Status.details][google.rpc.Status.details] field, or localized by the client.
     * </pre>
     *
     * <code>optional string message = 2;</code>
     */
    public com.google.protobuf.ByteString
        getMessageBytes() {
      java.lang.Object ref = message_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        message_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     * A developer-facing error message, which should be in English. Any
     * user-facing error message should be localized and sent in the
     * [google.rpc.Status.details][google.rpc.Status.details] field, or localized by the client.
     * </pre>
     *
     * <code>optional string message = 2;</code>
     */
    public Builder setMessage(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      message_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * A developer-facing error message, which should be in English. Any
     * user-facing error message should be localized and sent in the
     * [google.rpc.Status.details][google.rpc.Status.details] field, or localized by the client.
     * </pre>
     *
     * <code>optional string message = 2;</code>
     */
    public Builder clearMessage() {
      
      message_ = getDefaultInstance().getMessage();
      onChanged();
      return this;
    }
    /**
     * <pre>
     * A developer-facing error message, which should be in English. Any
     * user-facing error message should be localized and sent in the
     * [google.rpc.Status.details][google.rpc.Status.details] field, or localized by the client.
     * </pre>
     *
     * <code>optional string message = 2;</code>
     */
    public Builder setMessageBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      message_ = value;
      onChanged();
      return this;
    }

    private java.util.List<com.google.protobuf.Any> details_ =
      java.util.Collections.emptyList();
    private void ensureDetailsIsMutable() {
      if (!((bitField0_ & 0x00000004) == 0x00000004)) {
        details_ = new java.util.ArrayList<com.google.protobuf.Any>(details_);
        bitField0_ |= 0x00000004;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilder<
        com.google.protobuf.Any, com.google.protobuf.Any.Builder, com.google.protobuf.AnyOrBuilder> detailsBuilder_;

    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public java.util.List<com.google.protobuf.Any> getDetailsList() {
      if (detailsBuilder_ == null) {
        return java.util.Collections.unmodifiableList(details_);
      } else {
        return detailsBuilder_.getMessageList();
      }
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public int getDetailsCount() {
      if (detailsBuilder_ == null) {
        return details_.size();
      } else {
        return detailsBuilder_.getCount();
      }
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public com.google.protobuf.Any getDetails(int index) {
      if (detailsBuilder_ == null) {
        return details_.get(index);
      } else {
        return detailsBuilder_.getMessage(index);
      }
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder setDetails(
        int index, com.google.protobuf.Any value) {
      if (detailsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureDetailsIsMutable();
        details_.set(index, value);
        onChanged();
      } else {
        detailsBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder setDetails(
        int index, com.google.protobuf.Any.Builder builderForValue) {
      if (detailsBuilder_ == null) {
        ensureDetailsIsMutable();
        details_.set(index, builderForValue.build());
        onChanged();
      } else {
        detailsBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder addDetails(com.google.protobuf.Any value) {
      if (detailsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureDetailsIsMutable();
        details_.add(value);
        onChanged();
      } else {
        detailsBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder addDetails(
        int index, com.google.protobuf.Any value) {
      if (detailsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureDetailsIsMutable();
        details_.add(index, value);
        onChanged();
      } else {
        detailsBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder addDetails(
        com.google.protobuf.Any.Builder builderForValue) {
      if (detailsBuilder_ == null) {
        ensureDetailsIsMutable();
        details_.add(builderForValue.build());
        onChanged();
      } else {
        detailsBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder addDetails(
        int index, com.google.protobuf.Any.Builder builderForValue) {
      if (detailsBuilder_ == null) {
        ensureDetailsIsMutable();
        details_.add(index, builderForValue.build());
        onChanged();
      } else {
        detailsBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder addAllDetails(
        java.lang.Iterable<? extends com.google.protobuf.Any> values) {
      if (detailsBuilder_ == null) {
        ensureDetailsIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, details_);
        onChanged();
      } else {
        detailsBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder clearDetails() {
      if (detailsBuilder_ == null) {
        details_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
      } else {
        detailsBuilder_.clear();
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public Builder removeDetails(int index) {
      if (detailsBuilder_ == null) {
        ensureDetailsIsMutable();
        details_.remove(index);
        onChanged();
      } else {
        detailsBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public com.google.protobuf.Any.Builder getDetailsBuilder(
        int index) {
      return getDetailsFieldBuilder().getBuilder(index);
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public com.google.protobuf.AnyOrBuilder getDetailsOrBuilder(
        int index) {
      if (detailsBuilder_ == null) {
        return details_.get(index);  } else {
        return detailsBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public java.util.List<? extends com.google.protobuf.AnyOrBuilder> 
         getDetailsOrBuilderList() {
      if (detailsBuilder_ != null) {
        return detailsBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(details_);
      }
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public com.google.protobuf.Any.Builder addDetailsBuilder() {
      return getDetailsFieldBuilder().addBuilder(
          com.google.protobuf.Any.getDefaultInstance());
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public com.google.protobuf.Any.Builder addDetailsBuilder(
        int index) {
      return getDetailsFieldBuilder().addBuilder(
          index, com.google.protobuf.Any.getDefaultInstance());
    }
    /**
     * <pre>
     * A list of messages that carry the error details.  There will be a
     * common set of message types for APIs to use.
     * </pre>
     *
     * <code>repeated .google.protobuf.Any details = 3;</code>
     */
    public java.util.List<com.google.protobuf.Any.Builder> 
         getDetailsBuilderList() {
      return getDetailsFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilder<
        com.google.protobuf.Any, com.google.protobuf.Any.Builder, com.google.protobuf.AnyOrBuilder> 
        getDetailsFieldBuilder() {
      if (detailsBuilder_ == null) {
        detailsBuilder_ = new com.google.protobuf.RepeatedFieldBuilder<
            com.google.protobuf.Any, com.google.protobuf.Any.Builder, com.google.protobuf.AnyOrBuilder>(
                details_,
                ((bitField0_ & 0x00000004) == 0x00000004),
                getParentForChildren(),
                isClean());
        details_ = null;
      }
      return detailsBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:google.rpc.Status)
  }

  // @@protoc_insertion_point(class_scope:google.rpc.Status)
  private static final com.google.rpc.Status DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.google.rpc.Status();
  }

  public static com.google.rpc.Status getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<Status>
      PARSER = new com.google.protobuf.AbstractParser<Status>() {
    public Status parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new Status(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<Status> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<Status> getParserForType() {
    return PARSER;
  }

  public com.google.rpc.Status getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

