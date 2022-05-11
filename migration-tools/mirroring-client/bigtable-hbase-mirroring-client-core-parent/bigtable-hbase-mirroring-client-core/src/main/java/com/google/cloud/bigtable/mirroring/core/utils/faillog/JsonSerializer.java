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

import com.google.bigtable.hbase.mirroring.shaded.com.googlecode.protobuf.format.JsonJacksonFormat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.DynamicMessage;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessage;

/** The singleton of this class translates HBase protos into JSON and vice-versa. */
public class JsonSerializer {
  private static final JsonSerializer singleton = new JsonSerializer();
  private static final JsonJacksonFormat jsonFormat = new JsonJacksonFormat();

  private JsonSerializer() {}

  public static JsonSerializer getInstance() {
    return singleton;
  }

  /**
   * Serialize the given message to a single-line JSON.
   *
   * @param message the message to be serialized
   * @return the single-line JSON representation of the message
   */
  public String serialize(GeneratedMessage message) {
    try {
      ByteArrayOutputStream jsonOutputStream = new ByteArrayOutputStream();
      jsonFormat.print(message, jsonOutputStream, StandardCharsets.UTF_8);
      return new String(jsonOutputStream.toByteArray(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a message from JSON
   *
   * @param messagePrototype any message of the desired type (may be newly created); it is required
   *     to obtain the appropriate protobuf descriptor; the contents of that message will not be
   *     modified nor inspected
   * @param serializedMessage the JSON representation the message to deserialize
   * @param <T> the type of the message to deserialize the JSON representation to
   * @return a new message whose contents reflect the JSON representation
   * @throws IOException in case of a parse error
   */
  public <T extends GeneratedMessage> T deserialize(
      final T messagePrototype, String serializedMessage) throws IOException {
    Descriptor descriptor = messagePrototype.getDescriptorForType();
    DynamicMessage.Builder translatedMessageBuilder = DynamicMessage.newBuilder(descriptor);
    ByteArrayInputStream jsonInputStream =
        new ByteArrayInputStream(serializedMessage.getBytes(StandardCharsets.UTF_8));
    jsonFormat.merge(jsonInputStream, translatedMessageBuilder);
    return (T)
        messagePrototype
            .getParserForType()
            .parseFrom(translatedMessageBuilder.build().toByteArray());
  }
}
