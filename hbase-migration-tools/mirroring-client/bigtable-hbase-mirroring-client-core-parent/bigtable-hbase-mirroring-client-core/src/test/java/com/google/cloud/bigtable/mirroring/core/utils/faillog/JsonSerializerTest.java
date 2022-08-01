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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonSerializerTest {
  @Test
  public void serializeAndDeserialize() throws IOException {
    Increment increment = new Increment(new byte[] {'r'});
    for (int i = 0; i < 2; i++) {
      byte[] bytes = Bytes.toBytes(i);
      increment.addColumn(bytes, "foo\nbar\rbaz".getBytes(StandardCharsets.UTF_8), 13);
    }
    increment.setDurability(Durability.ASYNC_WAL);
    increment.setTimeRange(123, 456);
    increment.setReturnResults(false);
    increment.setACL("foo", new Permission(Permission.Action.EXEC));
    ClientProtos.MutationProto req =
        RequestConverter.buildMutateRequest(
                "no_region".getBytes(StandardCharsets.UTF_8), increment, 0, 1)
            .getMutation();

    String serializedJson = JsonSerializer.getInstance().serialize(req);
    // make sure the serialized JSON does not contain any newline characters
    assertFalse(
        "Serialized JSON should not contain newline characters",
        serializedJson.contains("\n") || serializedJson.contains("\r"));

    ClientProtos.MutationProto deserializedRequest =
        JsonSerializer.getInstance()
            .deserialize(ClientProtos.MutationProto.newBuilder().build(), serializedJson);
    assertEquals(req, deserializedRequest);
  }
}
