/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ImFlog.data;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.github.ImFlog.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit test for RecordBuilderBase.
 */
public class RecordBuilderBaseTest {
  private static Set<Schema.Type> primitives;
  private static Set<Schema.Type> nonNullPrimitives;

  @BeforeAll()
  public static void setUpBeforeClass() {
    primitives = new HashSet<>(Arrays.asList(Schema.Type.values()));
    primitives.removeAll(Arrays.asList(Schema.Type.RECORD, Schema.Type.ENUM, Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION, Schema.Type.FIXED));

    nonNullPrimitives = new HashSet<>(primitives);
    nonNullPrimitives.remove(Schema.Type.NULL);
  }

  @Test
  void isValidValueWithPrimitives() {
    // Verify that a non-null value is valid for all primitives:
    for (Schema.Type type : primitives) {
      Schema.Field f = new Schema.Field("f", Schema.create(type), null, null);
      assertTrue(RecordBuilderBase.isValidValue(f, new Object()));
    }

    // Verify that null is not valid for all non-null primitives:
    for (Schema.Type type : nonNullPrimitives) {
      Schema.Field f = new Schema.Field("f", Schema.create(type), null, null);
      assertFalse(RecordBuilderBase.isValidValue(f, null));
    }
  }

  @Test
  void isValidValueWithNullField() {
    // Verify that null is a valid value for null fields:
    assertTrue(RecordBuilderBase.isValidValue(new Schema.Field("f", Schema.create(Schema.Type.NULL), null, null), null));
  }

  @Test
  void isValidValueWithUnion() {
    // Verify that null values are not valid for a union with no null type:
    Schema unionWithoutNull = Schema
        .createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.BOOLEAN)));

    assertTrue(RecordBuilderBase.isValidValue(new Schema.Field("f", unionWithoutNull, null, null), new Object()));
    assertFalse(RecordBuilderBase.isValidValue(new Schema.Field("f", unionWithoutNull, null, null), null));

    // Verify that null values are valid for a union with a null type:
    Schema unionWithNull = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));

    assertTrue(RecordBuilderBase.isValidValue(new Schema.Field("f", unionWithNull, null, null), new Object()));
    assertTrue(RecordBuilderBase.isValidValue(new Schema.Field("f", unionWithNull, null, null), null));
  }
}
