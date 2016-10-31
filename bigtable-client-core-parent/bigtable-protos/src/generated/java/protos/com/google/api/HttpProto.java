// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/api/http.proto

package com.google.api;

public final class HttpProto {
  private HttpProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_google_api_HttpRule_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_api_HttpRule_fieldAccessorTable;
  static com.google.protobuf.Descriptors.Descriptor
    internal_static_google_api_CustomHttpPattern_descriptor;
  static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_api_CustomHttpPattern_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\025google/api/http.proto\022\ngoogle.api\"\330\001\n\010" +
      "HttpRule\022\r\n\003get\030\002 \001(\tH\000\022\r\n\003put\030\003 \001(\tH\000\022\016" +
      "\n\004post\030\004 \001(\tH\000\022\020\n\006delete\030\005 \001(\tH\000\022\017\n\005patc" +
      "h\030\006 \001(\tH\000\022/\n\006custom\030\010 \001(\0132\035.google.api.C" +
      "ustomHttpPatternH\000\022\014\n\004body\030\007 \001(\t\0221\n\023addi" +
      "tional_bindings\030\013 \003(\0132\024.google.api.HttpR" +
      "uleB\t\n\007pattern\"/\n\021CustomHttpPattern\022\014\n\004k" +
      "ind\030\001 \001(\t\022\014\n\004path\030\002 \001(\tB\035\n\016com.google.ap" +
      "iB\tHttpProtoP\001b\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_google_api_HttpRule_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_google_api_HttpRule_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_api_HttpRule_descriptor,
        new java.lang.String[] { "Get", "Put", "Post", "Delete", "Patch", "Custom", "Body", "AdditionalBindings", "Pattern", });
    internal_static_google_api_CustomHttpPattern_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_google_api_CustomHttpPattern_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_api_CustomHttpPattern_descriptor,
        new java.lang.String[] { "Kind", "Path", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
