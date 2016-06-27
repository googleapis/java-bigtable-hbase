// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/v2/bigtable.proto

package com.google.bigtable.v2;

public interface ReadModifyWriteRowRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:google.bigtable.v2.ReadModifyWriteRowRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string table_name = 1;</code>
   *
   * <pre>
   * The unique name of the table to which the read/modify/write rules should be
   * applied.
   * Values are of the form
   * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
   * </pre>
   */
  java.lang.String getTableName();
  /**
   * <code>optional string table_name = 1;</code>
   *
   * <pre>
   * The unique name of the table to which the read/modify/write rules should be
   * applied.
   * Values are of the form
   * projects/&amp;lt;project&amp;gt;/instances/&amp;lt;instance&amp;gt;/tables/&amp;lt;table&amp;gt;
   * </pre>
   */
  com.google.protobuf.ByteString
      getTableNameBytes();

  /**
   * <code>optional bytes row_key = 2;</code>
   *
   * <pre>
   * The key of the row to which the read/modify/write rules should be applied.
   * </pre>
   */
  com.google.protobuf.ByteString getRowKey();

  /**
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   *
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   */
  java.util.List<com.google.bigtable.v2.ReadModifyWriteRule> 
      getRulesList();
  /**
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   *
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   */
  com.google.bigtable.v2.ReadModifyWriteRule getRules(int index);
  /**
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   *
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   */
  int getRulesCount();
  /**
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   *
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   */
  java.util.List<? extends com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder> 
      getRulesOrBuilderList();
  /**
   * <code>repeated .google.bigtable.v2.ReadModifyWriteRule rules = 3;</code>
   *
   * <pre>
   * Rules specifying how the specified row's contents are to be transformed
   * into writes. Entries are applied in order, meaning that earlier rules will
   * affect the results of later ones.
   * </pre>
   */
  com.google.bigtable.v2.ReadModifyWriteRuleOrBuilder getRulesOrBuilder(
      int index);
}
