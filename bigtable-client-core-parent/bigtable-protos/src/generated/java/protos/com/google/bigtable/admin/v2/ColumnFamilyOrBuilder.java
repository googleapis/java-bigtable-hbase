// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/admin/v2/table.proto

package com.google.bigtable.admin.v2;

public interface ColumnFamilyOrBuilder extends
    // @@protoc_insertion_point(interface_extends:google.bigtable.admin.v2.ColumnFamily)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .google.bigtable.admin.v2.GcRule gc_rule = 1;</code>
   *
   * <pre>
   * Garbage collection rule specified as a protobuf.
   * Must serialize to at most 500 bytes.
   * NOTE: Garbage collection executes opportunistically in the background, and
   * so it's possible for reads to return a cell even if it matches the active
   * GC expression for its family.
   * </pre>
   */
  boolean hasGcRule();
  /**
   * <code>optional .google.bigtable.admin.v2.GcRule gc_rule = 1;</code>
   *
   * <pre>
   * Garbage collection rule specified as a protobuf.
   * Must serialize to at most 500 bytes.
   * NOTE: Garbage collection executes opportunistically in the background, and
   * so it's possible for reads to return a cell even if it matches the active
   * GC expression for its family.
   * </pre>
   */
  com.google.bigtable.admin.v2.GcRule getGcRule();
  /**
   * <code>optional .google.bigtable.admin.v2.GcRule gc_rule = 1;</code>
   *
   * <pre>
   * Garbage collection rule specified as a protobuf.
   * Must serialize to at most 500 bytes.
   * NOTE: Garbage collection executes opportunistically in the background, and
   * so it's possible for reads to return a cell even if it matches the active
   * GC expression for its family.
   * </pre>
   */
  com.google.bigtable.admin.v2.GcRuleOrBuilder getGcRuleOrBuilder();
}
