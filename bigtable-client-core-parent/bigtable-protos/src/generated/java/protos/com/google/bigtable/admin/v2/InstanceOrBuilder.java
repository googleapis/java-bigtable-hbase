// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/admin/v2/instance.proto

package com.google.bigtable.admin.v2;

public interface InstanceOrBuilder extends
    // @@protoc_insertion_point(interface_extends:google.bigtable.admin.v2.Instance)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string name = 1;</code>
   *
   * <pre>
   * &#64;OutputOnly
   * The unique name of the instance. Values are of the form
   * projects/&lt;project&gt;/instances/[a-z][a-z0-9&#92;&#92;-]+[a-z0-9]
   * </pre>
   */
  java.lang.String getName();
  /**
   * <code>optional string name = 1;</code>
   *
   * <pre>
   * &#64;OutputOnly
   * The unique name of the instance. Values are of the form
   * projects/&lt;project&gt;/instances/[a-z][a-z0-9&#92;&#92;-]+[a-z0-9]
   * </pre>
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
   * <code>optional string display_name = 2;</code>
   *
   * <pre>
   * The descriptive name for this instance as it appears in UIs.
   * Can be changed at any time, but should be kept globally unique
   * to avoid confusion.
   * </pre>
   */
  java.lang.String getDisplayName();
  /**
   * <code>optional string display_name = 2;</code>
   *
   * <pre>
   * The descriptive name for this instance as it appears in UIs.
   * Can be changed at any time, but should be kept globally unique
   * to avoid confusion.
   * </pre>
   */
  com.google.protobuf.ByteString
      getDisplayNameBytes();

  /**
   * <code>optional .google.bigtable.admin.v2.Instance.State state = 3;</code>
   *
   * <pre>
   * The current state of the instance.
   * </pre>
   */
  int getStateValue();
  /**
   * <code>optional .google.bigtable.admin.v2.Instance.State state = 3;</code>
   *
   * <pre>
   * The current state of the instance.
   * </pre>
   */
  com.google.bigtable.admin.v2.Instance.State getState();
}
