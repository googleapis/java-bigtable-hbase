/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam.validation;

import static com.google.common.truth.Truth.assertWithMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import junit.framework.TestCase;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HashBasedSourceSerializationTest extends TestCase {

  public static final String SOURCE_HASH_DIR = "gs://my-bucket/outputDir";
  public static final String PROJECT_ID = "test-project";
  private static final ImmutableBytesWritable START_ROW =
      new ImmutableBytesWritable("a".getBytes());
  private static final ImmutableBytesWritable STOP_ROW = new ImmutableBytesWritable("y".getBytes());

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testSerializeWithValueProviders() throws IOException {
    checkSerialization(
        new HadoopHashTableSource(
            StaticValueProvider.of(PROJECT_ID), StaticValueProvider.of(SOURCE_HASH_DIR)));
  }

  @Test
  public void testSerializeWithStartStop() throws IOException {
    checkSerialization(
        new HadoopHashTableSource(
            StaticValueProvider.of(PROJECT_ID),
            StaticValueProvider.of(SOURCE_HASH_DIR),
            new ImmutableBytesWritable(START_ROW),
            new ImmutableBytesWritable(STOP_ROW)));
  }

  @Test
  public void testBufferedSourceSerialize() {
    checkSerialization(
        new BufferedHadoopHashTableSource(
            new HadoopHashTableSource(
                StaticValueProvider.of(PROJECT_ID), StaticValueProvider.of(SOURCE_HASH_DIR))));
  }

  @Test
  public void testBufferedSourceSerializeWithBatchSize() {
    checkSerialization(
        new BufferedHadoopHashTableSource(
            new HadoopHashTableSource(
                StaticValueProvider.of(PROJECT_ID), StaticValueProvider.of(SOURCE_HASH_DIR)),
            5));
  }

  private static void checkSerialization(Object source) {
    try {
      Object deserialized = serializeDeserialize(source);
      checkClassDeclaresSerialVersionUid(source.getClass());
      assertEquals(source, deserialized);
    } catch (IOException | ClassNotFoundException e) {
      fail(e.toString());
    }
  }

  private static void checkClassDeclaresSerialVersionUid(Class cls) {
    String uid = "serialVersionUID";
    for (Field field : cls.getDeclaredFields()) {
      if (field.getName() == uid) {
        int modifiers = field.getModifiers();
        assertWithMessage(field + " is not static").that(Modifier.isStatic(modifiers)).isTrue();
        assertWithMessage(field + " is not final").that(Modifier.isFinal(modifiers)).isTrue();
        assertWithMessage(field + " is not private").that(Modifier.isPrivate(modifiers)).isTrue();
        assertWithMessage(field + " must be long")
            .that(field.getType().getSimpleName())
            .isEqualTo("long");
        return;
      }
    }
    fail(cls + " does not declare serialVersionUID");
  }

  private static Object serializeDeserialize(Object obj)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream outStream = new ObjectOutputStream(bos)) {
      outStream.writeObject(obj);
    }

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    try (ObjectInputStream inStream = new ObjectInputStream(bis)) {
      return inStream.readObject();
    }
  }
}
