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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * A default, JSON based implementation of the {@link Serializer}.
 *
 * <p>{@link JsonSerializer} is used under the hood to serialize the mutations. Apart from the
 * mutations, additional context is serialized as well, i.e. timestamp, failure reason, stack trace,
 * row key and mutation type (to make deserializing easier).
 *
 * <p>Every log entry is guaranteed to be a single line.
 */
public class DefaultSerializer implements Serializer {
  ObjectMapper objectMapper = new ObjectMapper();

  private LogEntry.OperationType operationTypeFromMutation(Mutation mutation) {
    if (mutation instanceof Append) {
      return LogEntry.OperationType.APPEND;
    } else if (mutation instanceof Delete) {
      return LogEntry.OperationType.DELETE;
    } else if (mutation instanceof Increment) {
      return LogEntry.OperationType.INCREMENT;
    } else if (mutation instanceof Put) {
      return LogEntry.OperationType.PUT;
    } else {
      throw new IllegalArgumentException(
          "Invalid mutation type: " + mutation.getClass().toString());
    }
  }

  @Override
  public byte[] serialize(Mutation mutation, Throwable failureCause) {
    LogEntry.OperationType operationType = operationTypeFromMutation(mutation);

    try {
      ClientProtos.MutationProto mutationProto =
          serializeMutationToProto(mutation, operationType.mutationType);
      LogEntry logEntry =
          new LogEntry(
              failureCause,
              mutation.getRow(),
              operationType,
              JsonSerializer.getInstance().serialize(mutationProto));
      return (objectMapper.writeValueAsString(logEntry) + '\n').getBytes(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ClientProtos.MutationProto serializeMutationToProto(
      Mutation mutation, ClientProtos.MutationProto.MutationType mutationType) throws IOException {
    ClientProtos.MutationProto mutationProto =
        (mutationType == ClientProtos.MutationProto.MutationType.INCREMENT)
            ? ProtobufUtil.toMutation(
                (Increment) mutation, ClientProtos.MutationProto.newBuilder(), 0)
            : ProtobufUtil.toMutation(mutationType, mutation, 0);
    return mutationProto;
  }

  private static class LogEntry {
    public Date timestamp;
    public String cause;
    public String stackTrace;
    public byte[] rowKey;
    public OperationType mutationType;
    @com.fasterxml.jackson.annotation.JsonRawValue public String mutation;

    public LogEntry(Throwable cause, byte[] rowKey, OperationType mutationType, String mutation) {
      this.timestamp = new Date();
      this.cause = (cause != null) ? cause.getMessage() : null;
      this.stackTrace = (cause != null) ? Throwables.getStackTraceAsString(cause) : null;
      this.rowKey = rowKey;
      this.mutationType = mutationType;
      this.mutation = mutation;
    }

    public enum OperationType {
      PUT("put", ClientProtos.MutationProto.MutationType.PUT),
      INCREMENT("inc", ClientProtos.MutationProto.MutationType.INCREMENT),
      APPEND("append", ClientProtos.MutationProto.MutationType.APPEND),
      DELETE("delete", ClientProtos.MutationProto.MutationType.DELETE);

      private final String name;
      private final ClientProtos.MutationProto.MutationType mutationType;

      OperationType(String name, ClientProtos.MutationProto.MutationType mutationType) {
        this.name = name;
        this.mutationType = mutationType;
      }
    }
  }
}
