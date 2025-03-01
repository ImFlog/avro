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
package com.github.imflog.avro;

import com.github.imflog.avro.Schema.Field;
import com.github.imflog.avro.Schema.Type;
import com.github.imflog.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestSchema {
  @Test
  void splitSchemaBuild() {
    Schema s = SchemaBuilder.record("HandshakeRequest").namespace("org.apache.avro.ipc").fields().name("clientProtocol")
        .type().optional().stringType().name("meta").type().optional().map().values().bytesType().endRecord();

    String schemaString = s.toString();
    int mid = schemaString.length() / 2;

    Schema parsedStringSchema = new Schema.Parser().parse(s.toString());
    Schema parsedArrayOfStringSchema = new Schema.Parser().parse(schemaString.substring(0, mid),
        schemaString.substring(mid));
    assertNotNull(parsedStringSchema);
    assertNotNull(parsedArrayOfStringSchema);
    assertEquals(parsedStringSchema.toString(), parsedArrayOfStringSchema.toString());
  }

  @Test
  void defaultRecordWithDuplicateFieldName() {
    String recordName = "name";
    Schema schema = Schema.createRecord(recordName, "doc", "namespace", false);
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("field_name", Schema.create(Type.NULL), null, null));
    fields.add(new Field("field_name", Schema.create(Type.INT), null, null));
    try {
      schema.setFields(fields);
      fail("Should not be able to create a record with duplicate field name.");
    } catch (AvroRuntimeException are) {
      assertTrue(are.getMessage().contains("Duplicate field field_name in record " + recordName));
    }
  }

  @Test
  void createUnionVarargs() {
    List<Schema> types = new ArrayList<>();
    types.add(Schema.create(Type.NULL));
    types.add(Schema.create(Type.LONG));
    Schema expected = Schema.createUnion(types);

    Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
    assertEquals(expected, schema);
  }

  @Test
  void recordWithNullDoc() {
    Schema schema = Schema.createRecord("name", null, "namespace", false);
    String schemaString = schema.toString();
    assertNotNull(schemaString);
  }

  @Test
  void recordWithNullNamespace() {
    Schema schema = Schema.createRecord("name", "doc", null, false);
    String schemaString = schema.toString();
    assertNotNull(schemaString);
  }

  @Test
  void emptyRecordSchema() {
    Schema schema = createDefaultRecord();
    String schemaString = schema.toString();
    assertNotNull(schemaString);
  }

  @Test
  void parseEmptySchema() {
    assertThrows(SchemaParseException.class, () -> {
      new Schema.Parser().parse("");
    });
  }

  @Test
  void schemaWithFields() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("field_name1", Schema.create(Type.NULL), null, null));
    fields.add(new Field("field_name2", Schema.create(Type.INT), null, null));
    Schema schema = createDefaultRecord();
    schema.setFields(fields);
    String schemaString = schema.toString();
    assertNotNull(schemaString);
    assertEquals(2, schema.getFields().size());
  }

  @Test
  void schemaWithNullFields() {
    assertThrows(NullPointerException.class, () -> {
      Schema.createRecord("name", "doc", "namespace", false, null);
    });
  }

  @Test
  void isUnionOnUnionWithMultipleElements() {
    Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
    assertTrue(schema.isUnion());
  }

  @Test
  void isUnionOnUnionWithOneElement() {
    Schema schema = Schema.createUnion(Schema.create(Type.LONG));
    assertTrue(schema.isUnion());
  }

  @Test
  void isUnionOnRecord() {
    Schema schema = createDefaultRecord();
    assertFalse(schema.isUnion());
  }

  @Test
  void isUnionOnArray() {
    Schema schema = Schema.createArray(Schema.create(Type.LONG));
    assertFalse(schema.isUnion());
  }

  @Test
  void isUnionOnEnum() {
    Schema schema = Schema.createEnum("name", "doc", "namespace", Collections.singletonList("value"));
    assertFalse(schema.isUnion());
  }

  @Test
  void isUnionOnFixed() {
    Schema schema = Schema.createFixed("name", "doc", "space", 10);
    assertFalse(schema.isUnion());
  }

  @Test
  void isUnionOnMap() {
    Schema schema = Schema.createMap(Schema.create(Type.LONG));
    assertFalse(schema.isUnion());
  }

  @Test
  void isNullableOnUnionWithNull() {
    Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
    assertTrue(schema.isNullable());
  }

  @Test
  void isNullableOnUnionWithoutNull() {
    Schema schema = Schema.createUnion(Schema.create(Type.LONG));
    assertFalse(schema.isNullable());
  }

  @Test
  void isNullableOnRecord() {
    Schema schema = createDefaultRecord();
    assertFalse(schema.isNullable());
  }

  private Schema createDefaultRecord() {
    return Schema.createRecord("name", "doc", "namespace", false);
  }

  @Test
  void serialization() throws IOException, ClassNotFoundException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        InputStream jsonSchema = getClass().getResourceAsStream("/SchemaBuilder.avsc")) {

      Schema payload = new Schema.Parser().parse(jsonSchema);
      oos.writeObject(payload);

      try (ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
          ObjectInputStream ois = new ObjectInputStream(bis)) {
        Schema sp = (Schema) ois.readObject();
        assertEquals(payload, sp);
      }
    }
  }

  @Test
  void reconstructSchemaStringWithoutInlinedChildReference() {
    String child = "{\"type\":\"record\"," + "\"name\":\"Child\"," + "\"namespace\":\"org.apache.avro.nested\","
        + "\"fields\":" + "[{\"name\":\"childField\",\"type\":\"string\"}]}";
    String parent = "{\"type\":\"record\"," + "\"name\":\"Parent\"," + "\"namespace\":\"org.apache.avro.nested\","
        + "\"fields\":" + "[{\"name\":\"child\",\"type\":\"Child\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema childSchema = parser.parse(child);
    Schema parentSchema = parser.parse(parent);
    String parentWithoutInlinedChildReference = parentSchema.toString(Collections.singleton(childSchema), false);
    // The generated string should be the same as the original parent
    // schema string that did not have the child schema inlined.
    assertEquals(parent, parentWithoutInlinedChildReference);
  }

  @Test
  void intDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.INT), "doc", 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1, field.defaultVal());
    assertEquals(1, GenericData.get().getDefaultValue(field));

    field = new Field("myField", Schema.create(Type.INT), "doc", Integer.MIN_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MIN_VALUE, field.defaultVal());
    assertEquals(Integer.MIN_VALUE, GenericData.get().getDefaultValue(field));

    field = new Field("myField", Schema.create(Type.INT), "doc", Integer.MAX_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MAX_VALUE, field.defaultVal());
    assertEquals(Integer.MAX_VALUE, GenericData.get().getDefaultValue(field));
  }

  @Test
  void validLongAsIntDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.INT), "doc", 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1, field.defaultVal());
    assertEquals(1, GenericData.get().getDefaultValue(field));

    field = new Field("myField", Schema.create(Type.INT), "doc", Long.valueOf(Integer.MIN_VALUE));
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MIN_VALUE, field.defaultVal());
    assertEquals(Integer.MIN_VALUE, GenericData.get().getDefaultValue(field));

    field = new Field("myField", Schema.create(Type.INT), "doc", Long.valueOf(Integer.MAX_VALUE));
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MAX_VALUE, field.defaultVal());
    assertEquals(Integer.MAX_VALUE, GenericData.get().getDefaultValue(field));
  }

  @Test
  void invalidLongAsIntDefaultValue() {
    assertThrows(AvroTypeException.class, () -> {
      new Field("myField", Schema.create(Type.INT), "doc", Integer.MAX_VALUE + 1L);
    });
  }

  @Test
  void doubleAsIntDefaultValue() {
    assertThrows(AvroTypeException.class, () -> {
      new Field("myField", Schema.create(Type.INT), "doc", 1.0);
    });
  }

  @Test
  void longDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.LONG), "doc", 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1L, field.defaultVal());
    assertEquals(1L, GenericData.get().getDefaultValue(field));

    field = new Field("myField", Schema.create(Type.LONG), "doc", Long.MIN_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Long.MIN_VALUE, field.defaultVal());
    assertEquals(Long.MIN_VALUE, GenericData.get().getDefaultValue(field));

    field = new Field("myField", Schema.create(Type.LONG), "doc", Long.MAX_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Long.MAX_VALUE, field.defaultVal());
    assertEquals(Long.MAX_VALUE, GenericData.get().getDefaultValue(field));
  }

  @Test
  void intAsLongDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.LONG), "doc", 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1L, field.defaultVal());
    assertEquals(1L, GenericData.get().getDefaultValue(field));
  }

  @Test
  void doubleAsLongDefaultValue() {
    assertThrows(AvroTypeException.class, () -> {
      new Field("myField", Schema.create(Type.LONG), "doc", 1.0);
    });
  }

  @Test
  void doubleDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.DOUBLE), "doc", 1.0);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  void intAsDoubleDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.DOUBLE), "doc", 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  void longAsDoubleDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.DOUBLE), "doc", 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  void floatAsDoubleDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.DOUBLE), "doc", 1.0f);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  void floatDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.FLOAT), "doc", 1.0f);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  void intAsFloatDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.FLOAT), "doc", 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  void longAsFloatDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.FLOAT), "doc", 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  void doubleAsFloatDefaultValue() {
    Field field = new Field("myField", Schema.create(Type.FLOAT), "doc", 1.0d);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  void enumSymbolAsNull() {
    assertThrows(SchemaParseException.class, () -> {
      Schema.createEnum("myField", "doc", "namespace", Collections.singletonList(null));
    });
  }

  @Test
  void schemaFieldWithoutSchema() {
    assertThrows(NullPointerException.class, () -> {
      new Field("f", null);
    });
  }

  @Test
  void parseRecordWithNameAsType() {
    final String schemaString = "{\n  \"type\" : \"record\",\n  \"name\" : \"ns.int\",\n"
        + "  \"fields\" : [ \n    {\"name\" : \"value\", \"type\" : \"int\"}, \n"
        + "    {\"name\" : \"next\", \"type\" : [ \"null\", \"ns.int\" ]}\n  ]\n}";
    final Schema schema = new Schema.Parser().parse(schemaString);
    String toString = schema.toString(true);

    final Schema schema2 = new Schema.Parser().parse(toString);
    assertEquals(schema, schema2);
  }

  @Test
  void qualifiedName() {
    Arrays.stream(Type.values()).forEach((Type t) -> {
      final Schema.Name name = new Schema.Name(t.getName(), "space");
      assertEquals("space." + t.getName(), name.getQualified("space"));
      assertEquals("space." + t.getName(), name.getQualified("otherdefault"));
    });
    final Schema.Name name = new Schema.Name("name", "space");
    assertEquals("name", name.getQualified("space"));
    assertEquals("space.name", name.getQualified("otherdefault"));

    final Schema.Name nameInt = new Schema.Name("Int", "space");
    assertEquals("Int", nameInt.getQualified("space"));
  }

  @Test
  void testContentAfterAvsc() throws Exception {
    Schema.Parser parser = new Schema.Parser();
    parser.setValidate(true);
    parser.setValidateDefaults(true);
    assertThrows(SchemaParseException.class, () -> parser.parse("{\"type\": \"string\"}; DROP TABLE STUDENTS"));
  }

  @Test
  void testContentAfterAvscInInputStream() throws Exception {
    Schema.Parser parser = new Schema.Parser();
    parser.setValidate(true);
    parser.setValidateDefaults(true);
    String avsc = "{\"type\": \"string\"}; DROP TABLE STUDENTS";
    ByteArrayInputStream is = new ByteArrayInputStream(avsc.getBytes(StandardCharsets.UTF_8));
    Schema schema = parser.parse(is);
    assertNotNull(schema);
  }

  @Test
  void testContentAfterAvscInFile() throws Exception {
    File avscFile = Files.createTempFile("testContentAfterAvscInFile", null).toFile();
    try (FileWriter writer = new FileWriter(avscFile)) {
      writer.write("{\"type\": \"string\"}; DROP TABLE STUDENTS");
      writer.flush();
    }

    Schema.Parser parser = new Schema.Parser();
    parser.setValidate(true);
    parser.setValidateDefaults(true);
    assertThrows(SchemaParseException.class, () -> parser.parse(avscFile));
  }

  @Test
  void unknownTypeAccepted() {
    final String inputSchema = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"remote\",\n"
        + "  \"namespace\" : \"ns\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"value\",\n"
        + "    \"type\" : \"T\"\n" + "  } ]\n" + "}";
    Schema.Parser parser = new Schema.Parser();

    String result = parser.setValidateUnknownTypes(false).parse(inputSchema).toString(true);
    assertEquals(inputSchema, result);
  }

  @Test
  void unknownTypeFailsWithoutFlag() {
    final String inputSchema = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"remote\",\n"
        + "  \"namespace\" : \"ns\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"value\",\n"
        + "    \"type\" : \"T\"\n" + "  } ]\n" + "}";
    assertThrows(SchemaParseException.class, () -> new Schema.Parser().parse(inputSchema));
  }
}
