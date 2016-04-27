// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/v2/bigtable.proto

package com.google.bigtable.v2;

public interface MutateRowsResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:google.bigtable.v2.MutateRowsResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .google.rpc.Status statuses = 1;</code>
   *
   * <pre>
   * The results for each Entry from the request, presented in the order
   * in which the entries were originally given.
   * Depending on how requests are batched during execution, it is possible
   * for one Entry to fail due to an error with another Entry. In the event
   * that this occurs, the same error will be reported for both entries.
   * </pre>
   */
  java.util.List<com.google.rpc.Status> 
      getStatusesList();
  /**
   * <code>repeated .google.rpc.Status statuses = 1;</code>
   *
   * <pre>
   * The results for each Entry from the request, presented in the order
   * in which the entries were originally given.
   * Depending on how requests are batched during execution, it is possible
   * for one Entry to fail due to an error with another Entry. In the event
   * that this occurs, the same error will be reported for both entries.
   * </pre>
   */
  com.google.rpc.Status getStatuses(int index);
  /**
   * <code>repeated .google.rpc.Status statuses = 1;</code>
   *
   * <pre>
   * The results for each Entry from the request, presented in the order
   * in which the entries were originally given.
   * Depending on how requests are batched during execution, it is possible
   * for one Entry to fail due to an error with another Entry. In the event
   * that this occurs, the same error will be reported for both entries.
   * </pre>
   */
  int getStatusesCount();
  /**
   * <code>repeated .google.rpc.Status statuses = 1;</code>
   *
   * <pre>
   * The results for each Entry from the request, presented in the order
   * in which the entries were originally given.
   * Depending on how requests are batched during execution, it is possible
   * for one Entry to fail due to an error with another Entry. In the event
   * that this occurs, the same error will be reported for both entries.
   * </pre>
   */
  java.util.List<? extends com.google.rpc.StatusOrBuilder> 
      getStatusesOrBuilderList();
  /**
   * <code>repeated .google.rpc.Status statuses = 1;</code>
   *
   * <pre>
   * The results for each Entry from the request, presented in the order
   * in which the entries were originally given.
   * Depending on how requests are batched during execution, it is possible
   * for one Entry to fail due to an error with another Entry. In the event
   * that this occurs, the same error will be reported for both entries.
   * </pre>
   */
  com.google.rpc.StatusOrBuilder getStatusesOrBuilder(
      int index);
}
