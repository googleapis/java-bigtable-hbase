/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.mirroring.core.utils.faillog;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

class FakeCellScanner implements CellScanner {
  @Override
  public Cell current() {
    return null;
  }

  @Override
  public boolean advance() throws IOException {
    return false;
  }
}

@RunWith(JUnit4.class)
public class DefaultSerializerTest {
  private ClientProtos.MutationProto verifyAndExtractMutation(
      Date beforeTest, String expectedRowKey, String expectedMutationType, String jsonText)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode deserialized = mapper.readTree(jsonText);
    final JsonNode timestampJson = deserialized.get("timestamp");
    final Date timestamp = new java.util.Date(timestampJson.asLong());
    final String cause = deserialized.get("cause").asText();
    final String stackTrace = deserialized.get("stackTrace").asText();
    final String rowKey = deserialized.get("rowKey").asText();
    final String mutationType = deserialized.get("mutationType").asText();
    final String mutation = deserialized.get("mutation").toString();
    Date afterTest = new Date();
    assertTrue(beforeTest.compareTo(timestamp) <= 0);
    assertTrue(afterTest.compareTo(timestamp) >= 0);
    assertThat(cause).isEqualTo("OMG!");
    assertThat(stackTrace).contains(this.getClass().getName());
    assertThat(BaseEncoding.base64().decode(rowKey))
        .isEqualTo(expectedRowKey.getBytes(StandardCharsets.UTF_8));
    assertThat(mutationType).isEqualTo(expectedMutationType);
    ClientProtos.MutationProto mutationProto = ClientProtos.MutationProto.newBuilder().build();
    return JsonSerializer.getInstance().deserialize(mutationProto, mutation);
  }

  @Test
  public void put() throws IOException, InterruptedException {
    Date beforeTest = new Date();
    Serializer serializer = new DefaultSerializer();
    Put put = new Put(new byte[] {'r'});

    for (int i = 0; i < 2; i++) {
      byte[] bytes = Bytes.toBytes(i);
      put.addColumn(
          bytes,
          "foo\nbar\rbaz".getBytes(StandardCharsets.UTF_8),
          12345,
          "asd".getBytes(StandardCharsets.UTF_8));
    }
    put.setDurability(Durability.ASYNC_WAL);
    put.setACL("foo", new Permission(Permission.Action.EXEC));

    String serialized;
    try {
      throw new RuntimeException("OMG!");
    } catch (Throwable throwable) {
      serialized = new String(serializer.serialize(put, throwable));
    }

    ClientProtos.MutationProto mutationProto =
        verifyAndExtractMutation(beforeTest, "r", "PUT", serialized);
    Put recreatedPut = ProtobufUtil.toPut(mutationProto, new FakeCellScanner());

    assertThat(recreatedPut.toJSON()).isEqualTo(put.toJSON());
  }

  @Test
  public void delete() throws IOException, InterruptedException {
    Date beforeTest = new Date();
    Serializer serializer = new DefaultSerializer();
    Delete delete = new Delete(new byte[] {'r'});

    for (int i = 0; i < 2; i++) {
      byte[] bytes = Bytes.toBytes(i);
      delete.addColumn(bytes, "foo\nbar\rbaz".getBytes(StandardCharsets.UTF_8));
    }
    delete.setDurability(Durability.ASYNC_WAL);
    delete.setACL("foo", new Permission(Permission.Action.EXEC));

    String serialized;
    try {
      throw new RuntimeException("OMG!");
    } catch (Throwable throwable) {
      serialized = new String(serializer.serialize(delete, throwable));
    }

    ClientProtos.MutationProto mutationProto =
        verifyAndExtractMutation(beforeTest, "r", "DELETE", serialized);
    Delete recreateDelete = ProtobufUtil.toDelete(mutationProto, new FakeCellScanner());

    assertThat(recreateDelete.toJSON()).isEqualTo(delete.toJSON());
  }

  @Test
  public void increment() throws IOException, InterruptedException {
    Date beforeTest = new Date();
    Serializer serializer = new DefaultSerializer();
    Increment increment = new Increment(new byte[] {'r'});

    for (int i = 0; i < 2; i++) {
      byte[] bytes = Bytes.toBytes(i);
      increment.addColumn(bytes, "foo\nbar\rbaz".getBytes(StandardCharsets.UTF_8), 13);
    }
    increment.setDurability(Durability.ASYNC_WAL);
    increment.setTimeRange(123, 456);
    increment.setReturnResults(false);
    increment.setACL("foo", new Permission(Permission.Action.EXEC));

    String serialized;
    try {
      throw new RuntimeException("OMG!");
    } catch (Throwable throwable) {
      serialized = new String(serializer.serialize(increment, throwable));
    }

    ClientProtos.MutationProto mutationProto =
        verifyAndExtractMutation(beforeTest, "r", "INCREMENT", serialized);
    Increment recreatedIncrement = ProtobufUtil.toIncrement(mutationProto, new FakeCellScanner());

    // We can't compare the whole increments because of an HBase idiosyncrasy - even though
    // timestamp is not used in increments, it has different values when it's created via its
    // constructor and when it is deserialized. There is no way to change that.
    assertThat(recreatedIncrement.getRow()).isEqualTo(increment.getRow());
  }

  @Test
  public void append() throws IOException, InterruptedException {
    Date beforeTest = new Date();
    Serializer serializer = new DefaultSerializer();
    Append append = new Append(new byte[] {'r'});

    for (int i = 0; i < 2; i++) {
      byte[] bytes = Bytes.toBytes(i);
      append.add(
          bytes,
          "foo\nbar\rbaz".getBytes(StandardCharsets.UTF_8),
          "asd".getBytes(StandardCharsets.UTF_8));
    }
    append.setDurability(Durability.ASYNC_WAL);
    append.setACL("foo", new Permission(Permission.Action.EXEC));

    String serialized;
    try {
      throw new RuntimeException("OMG!");
    } catch (Throwable throwable) {
      serialized = new String(serializer.serialize(append, throwable));
    }

    ClientProtos.MutationProto mutationProto =
        verifyAndExtractMutation(beforeTest, "r", "APPEND", serialized);
    Append recreatedAppend = ProtobufUtil.toAppend(mutationProto, new FakeCellScanner());

    assertThat(recreatedAppend.toJSON()).isEqualTo(append.toJSON());
  }
}
