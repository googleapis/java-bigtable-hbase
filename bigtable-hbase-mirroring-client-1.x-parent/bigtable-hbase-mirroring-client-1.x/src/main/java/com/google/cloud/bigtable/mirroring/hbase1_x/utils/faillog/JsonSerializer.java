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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.GeneratedMessage;

/**
 * The singleton of this class translates internal HBase protos into JSON and vice-versa.
 *
 * <p>The internal HBase protos are shaded, so they cannot be passed directly to JsonFormatter. In
 * order to do that, we serialize the protos to a byte array (in binary format) and then translate
 * that to a JSON. In order for this translation to have the names of the fields (rather than
 * numbers), unshaded JsonFormat needs to be provided with the information about those protos. This
 * is performed by serializing and then parsing the proto file descriptors (only once per program
 * execution).
 */
public class JsonSerializer {
  private static final JsonSerializer singleton = new JsonSerializer();
  private final Map<String, Descriptors.FileDescriptor> translatedFileCache = new HashMap<>();
  private final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
  private final JsonFormat.Parser parser = JsonFormat.parser();

  private JsonSerializer() {}

  public static JsonSerializer getInstance() {
    return singleton;
  }

  /**
   * A caching wrapper around {@link
   * #translateFile(org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.FileDescriptor)}
   *
   * @param fileDescriptor the file descriptor of the shaded, generated protobuf message
   * @return the translated file descriptor
   */
  private synchronized Descriptors.FileDescriptor translateFileCached(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.FileDescriptor
          fileDescriptor) {
    Descriptors.FileDescriptor translated = translatedFileCache.get(fileDescriptor.getName());
    if (translated != null) {
      return translated;
    }
    translated = translateFile(fileDescriptor);
    translatedFileCache.put(fileDescriptor.getName(), translated);
    return translated;
  }

  /**
   * Translate a protobuf file descriptor from the shaded HBase message to an unshaded protobuf
   * descriptor.
   *
   * <p>The file may have dependencies, so they are translated recursively. The results are cached
   * to make sure that every file is translated only once,
   *
   * @param fileDescriptor the file descriptor of the shaded, generated protobuf message
   * @return the translated file descriptor
   */
  private synchronized Descriptors.FileDescriptor translateFile(
      org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.FileDescriptor
          fileDescriptor) {
    ArrayList<Descriptors.FileDescriptor> deps = new ArrayList<>();
    for (org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.FileDescriptor sourceDep :
        fileDescriptor.getDependencies()) {
      deps.add(translateFileCached(sourceDep));
    }
    Descriptors.FileDescriptor[] depsArray = new Descriptors.FileDescriptor[deps.size()];
    depsArray = deps.toArray(depsArray);
    try {
      return Descriptors.FileDescriptor.buildFrom(
          DescriptorProtos.FileDescriptorProto.parseFrom(fileDescriptor.toProto().toByteArray()),
          depsArray);
    } catch (Descriptors.DescriptorValidationException | InvalidProtocolBufferException e) {
      // This would be an exceptional situation even among exceptional situations, so let's not
      // force the user to handle it. It's unclear if and when it can happen, but if it does, the
      // fix is likely in libraries' versions.
      throw new RuntimeException(
          "Failed to translate HBase proto description " + fileDescriptor.getName(), e);
    }
  }

  /**
   * Serialize the given message to a single-line JSON.
   *
   * @param message the message to be serialized
   * @return the single-line JSON representation of the message
   */
  public String serialize(GeneratedMessage message) {
    org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor descriptor =
        message.getDescriptorForType();
    String typeName = descriptor.getName();
    Descriptors.FileDescriptor translatedFileDescriptor = translateFileCached(descriptor.getFile());
    Descriptors.Descriptor translatedMessageDescriptor =
        translatedFileDescriptor.findMessageTypeByName(typeName);
    try {
      DynamicMessage translatedMessage =
          DynamicMessage.parseFrom(translatedMessageDescriptor, message.toByteArray());
      return printer.print(translatedMessage);
    } catch (InvalidProtocolBufferException e) {
      // This situation can only happen on libraries' misconfiguration. We can skip it.
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
   * @throws InvalidProtocolBufferException in case of a parse error
   */
  public <T extends GeneratedMessage> T deserialize(
      final T messagePrototype, String serializedMessage) throws InvalidProtocolBufferException {
    org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.Descriptor descriptor =
        messagePrototype.getDescriptorForType();
    String typeName = descriptor.getName();
    Descriptors.FileDescriptor translatedFileDescriptor = translateFileCached(descriptor.getFile());
    Descriptors.Descriptor translatedMessageDescriptor =
        translatedFileDescriptor.findMessageTypeByName(typeName);

    DynamicMessage.Builder translatedMessageBuilder =
        DynamicMessage.newBuilder(translatedMessageDescriptor);
    parser.merge(serializedMessage, translatedMessageBuilder);
    try {
      return (T)
          messagePrototype
              .getParserForType()
              .parseFrom(translatedMessageBuilder.build().toByteArray());
    } catch (org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException e) {
      throw new InvalidProtocolBufferException(e);
    }
  }
}
