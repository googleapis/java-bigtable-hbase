// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/admin/v2/bigtable_instance_admin.proto

package com.google.bigtable.admin.v2;

public interface ListClustersResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:google.bigtable.admin.v2.ListClustersResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * The list of requested clusters.
   * </pre>
   *
   * <code>repeated .google.bigtable.admin.v2.Cluster clusters = 1;</code>
   */
  java.util.List<com.google.bigtable.admin.v2.Cluster> 
      getClustersList();
  /**
   * <pre>
   * The list of requested clusters.
   * </pre>
   *
   * <code>repeated .google.bigtable.admin.v2.Cluster clusters = 1;</code>
   */
  com.google.bigtable.admin.v2.Cluster getClusters(int index);
  /**
   * <pre>
   * The list of requested clusters.
   * </pre>
   *
   * <code>repeated .google.bigtable.admin.v2.Cluster clusters = 1;</code>
   */
  int getClustersCount();
  /**
   * <pre>
   * The list of requested clusters.
   * </pre>
   *
   * <code>repeated .google.bigtable.admin.v2.Cluster clusters = 1;</code>
   */
  java.util.List<? extends com.google.bigtable.admin.v2.ClusterOrBuilder> 
      getClustersOrBuilderList();
  /**
   * <pre>
   * The list of requested clusters.
   * </pre>
   *
   * <code>repeated .google.bigtable.admin.v2.Cluster clusters = 1;</code>
   */
  com.google.bigtable.admin.v2.ClusterOrBuilder getClustersOrBuilder(
      int index);

  /**
   * <pre>
   * Locations from which Cluster information could not be retrieved,
   * due to an outage or some other transient condition.
   * Clusters from these locations may be missing from 'clusters',
   * or may only have partial information returned.
   * </pre>
   *
   * <code>repeated string failed_locations = 2;</code>
   */
  com.google.protobuf.ProtocolStringList
      getFailedLocationsList();
  /**
   * <pre>
   * Locations from which Cluster information could not be retrieved,
   * due to an outage or some other transient condition.
   * Clusters from these locations may be missing from 'clusters',
   * or may only have partial information returned.
   * </pre>
   *
   * <code>repeated string failed_locations = 2;</code>
   */
  int getFailedLocationsCount();
  /**
   * <pre>
   * Locations from which Cluster information could not be retrieved,
   * due to an outage or some other transient condition.
   * Clusters from these locations may be missing from 'clusters',
   * or may only have partial information returned.
   * </pre>
   *
   * <code>repeated string failed_locations = 2;</code>
   */
  java.lang.String getFailedLocations(int index);
  /**
   * <pre>
   * Locations from which Cluster information could not be retrieved,
   * due to an outage or some other transient condition.
   * Clusters from these locations may be missing from 'clusters',
   * or may only have partial information returned.
   * </pre>
   *
   * <code>repeated string failed_locations = 2;</code>
   */
  com.google.protobuf.ByteString
      getFailedLocationsBytes(int index);

  /**
   * <pre>
   * Set if not all clusters could be returned in a single response.
   * Pass this value to `page_token` in another request to get the next
   * page of results.
   * </pre>
   *
   * <code>optional string next_page_token = 3;</code>
   */
  java.lang.String getNextPageToken();
  /**
   * <pre>
   * Set if not all clusters could be returned in a single response.
   * Pass this value to `page_token` in another request to get the next
   * page of results.
   * </pre>
   *
   * <code>optional string next_page_token = 3;</code>
   */
  com.google.protobuf.ByteString
      getNextPageTokenBytes();
}
