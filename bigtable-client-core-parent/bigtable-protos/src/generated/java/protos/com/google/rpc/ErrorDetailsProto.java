// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/rpc/error_details.proto

package com.google.rpc;

public final class ErrorDetailsProto {
  private ErrorDetailsProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_RetryInfo_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_RetryInfo_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_DebugInfo_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_DebugInfo_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_QuotaFailure_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_QuotaFailure_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_QuotaFailure_Violation_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_QuotaFailure_Violation_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_BadRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_BadRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_BadRequest_FieldViolation_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_BadRequest_FieldViolation_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_RequestInfo_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_RequestInfo_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_ResourceInfo_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_ResourceInfo_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_Help_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_Help_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_google_rpc_Help_Link_descriptor;
  static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_google_rpc_Help_Link_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\036google/rpc/error_details.proto\022\ngoogle" +
      ".rpc\032\036google/protobuf/duration.proto\";\n\t" +
      "RetryInfo\022.\n\013retry_delay\030\001 \001(\0132\031.google." +
      "protobuf.Duration\"2\n\tDebugInfo\022\025\n\rstack_" +
      "entries\030\001 \003(\t\022\016\n\006detail\030\002 \001(\t\"y\n\014QuotaFa" +
      "ilure\0226\n\nviolations\030\001 \003(\0132\".google.rpc.Q" +
      "uotaFailure.Violation\0321\n\tViolation\022\017\n\007su" +
      "bject\030\001 \001(\t\022\023\n\013description\030\002 \001(\t\"\203\001\n\nBad" +
      "Request\022?\n\020field_violations\030\001 \003(\0132%.goog" +
      "le.rpc.BadRequest.FieldViolation\0324\n\016Fiel",
      "dViolation\022\r\n\005field\030\001 \001(\t\022\023\n\013description" +
      "\030\002 \001(\t\"7\n\013RequestInfo\022\022\n\nrequest_id\030\001 \001(" +
      "\t\022\024\n\014serving_data\030\002 \001(\t\"`\n\014ResourceInfo\022" +
      "\025\n\rresource_type\030\001 \001(\t\022\025\n\rresource_name\030" +
      "\002 \001(\t\022\r\n\005owner\030\003 \001(\t\022\023\n\013description\030\004 \001(" +
      "\t\"V\n\004Help\022$\n\005links\030\001 \003(\0132\025.google.rpc.He" +
      "lp.Link\032(\n\004Link\022\023\n\013description\030\001 \001(\t\022\013\n\003" +
      "url\030\002 \001(\tB%\n\016com.google.rpcB\021ErrorDetail" +
      "sProtoP\001b\006proto3"
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
          com.google.protobuf.DurationProto.getDescriptor(),
        }, assigner);
    internal_static_google_rpc_RetryInfo_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_google_rpc_RetryInfo_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_RetryInfo_descriptor,
        new java.lang.String[] { "RetryDelay", });
    internal_static_google_rpc_DebugInfo_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_google_rpc_DebugInfo_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_DebugInfo_descriptor,
        new java.lang.String[] { "StackEntries", "Detail", });
    internal_static_google_rpc_QuotaFailure_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_google_rpc_QuotaFailure_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_QuotaFailure_descriptor,
        new java.lang.String[] { "Violations", });
    internal_static_google_rpc_QuotaFailure_Violation_descriptor =
      internal_static_google_rpc_QuotaFailure_descriptor.getNestedTypes().get(0);
    internal_static_google_rpc_QuotaFailure_Violation_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_QuotaFailure_Violation_descriptor,
        new java.lang.String[] { "Subject", "Description", });
    internal_static_google_rpc_BadRequest_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_google_rpc_BadRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_BadRequest_descriptor,
        new java.lang.String[] { "FieldViolations", });
    internal_static_google_rpc_BadRequest_FieldViolation_descriptor =
      internal_static_google_rpc_BadRequest_descriptor.getNestedTypes().get(0);
    internal_static_google_rpc_BadRequest_FieldViolation_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_BadRequest_FieldViolation_descriptor,
        new java.lang.String[] { "Field", "Description", });
    internal_static_google_rpc_RequestInfo_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_google_rpc_RequestInfo_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_RequestInfo_descriptor,
        new java.lang.String[] { "RequestId", "ServingData", });
    internal_static_google_rpc_ResourceInfo_descriptor =
      getDescriptor().getMessageTypes().get(5);
    internal_static_google_rpc_ResourceInfo_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_ResourceInfo_descriptor,
        new java.lang.String[] { "ResourceType", "ResourceName", "Owner", "Description", });
    internal_static_google_rpc_Help_descriptor =
      getDescriptor().getMessageTypes().get(6);
    internal_static_google_rpc_Help_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_Help_descriptor,
        new java.lang.String[] { "Links", });
    internal_static_google_rpc_Help_Link_descriptor =
      internal_static_google_rpc_Help_descriptor.getNestedTypes().get(0);
    internal_static_google_rpc_Help_Link_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_google_rpc_Help_Link_descriptor,
        new java.lang.String[] { "Description", "Url", });
    com.google.protobuf.DurationProto.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
