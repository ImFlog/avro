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
package com.github.ImFlog.avro;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.github.ImFlog.avro.generic.GenericData;
import com.github.ImFlog.avro.generic.GenericDatumReader;
import com.github.ImFlog.avro.generic.GenericDatumWriter;
import com.github.ImFlog.avro.util.Utf8;
import com.github.ImFlog.avro.io.BinaryDecoder;
import com.github.ImFlog.avro.io.DatumReader;
import com.github.ImFlog.avro.io.DatumWriter;
import com.github.ImFlog.avro.io.Decoder;
import com.github.ImFlog.avro.io.DecoderFactory;
import com.github.ImFlog.avro.io.Encoder;
import com.github.ImFlog.avro.io.EncoderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit-tests for SchemaCompatibility.
 */
public class TestSchemaCompatibility {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaCompatibility.class);
  // -----------------------------------------------------------------------------------------------

  private static final Schema WRITER_SCHEMA = Schema
      .createRecord(TestSchemas.list(new Schema.Field("oldfield1", TestSchemas.INT_SCHEMA, null, null),
          new Schema.Field("oldfield2", TestSchemas.STRING_SCHEMA, null, null)));

  @Test
  void validateSchemaPairMissingField() {
    final List<Schema.Field> readerFields = TestSchemas
        .list(new Schema.Field("oldfield1", TestSchemas.INT_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final SchemaCompatibility.SchemaPairCompatibility expectedResult = new SchemaCompatibility.SchemaPairCompatibility(
        SchemaCompatibility.SchemaCompatibilityResult.compatible(), reader, WRITER_SCHEMA,
        SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);

    // Test omitting a field.
    Assertions.assertEquals(expectedResult, SchemaCompatibility.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  void validateSchemaPairMissingSecondField() {
    final List<Schema.Field> readerFields = TestSchemas
        .list(new Schema.Field("oldfield2", TestSchemas.STRING_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final SchemaCompatibility.SchemaPairCompatibility expectedResult = new SchemaCompatibility.SchemaPairCompatibility(
        SchemaCompatibility.SchemaCompatibilityResult.compatible(), reader, WRITER_SCHEMA,
        SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);

    // Test omitting other field.
    Assertions.assertEquals(expectedResult, SchemaCompatibility.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  void validateSchemaPairAllFields() {
    final List<Schema.Field> readerFields = TestSchemas.list(
        new Schema.Field("oldfield1", TestSchemas.INT_SCHEMA, null, null),
        new Schema.Field("oldfield2", TestSchemas.STRING_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    final SchemaCompatibility.SchemaPairCompatibility expectedResult = new SchemaCompatibility.SchemaPairCompatibility(
        SchemaCompatibility.SchemaCompatibilityResult.compatible(), reader, WRITER_SCHEMA,
        SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);

    // Test with all fields.
    Assertions.assertEquals(expectedResult, SchemaCompatibility.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  void validateSchemaNewFieldWithDefault() {
    final List<Schema.Field> readerFields = TestSchemas.list(
        new Schema.Field("oldfield1", TestSchemas.INT_SCHEMA, null, null),
        new Schema.Field("newfield1", TestSchemas.INT_SCHEMA, null, 42));
    final Schema reader = Schema.createRecord(readerFields);
    final SchemaCompatibility.SchemaPairCompatibility expectedResult = new SchemaCompatibility.SchemaPairCompatibility(
        SchemaCompatibility.SchemaCompatibilityResult.compatible(), reader, WRITER_SCHEMA,
        SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);

    // Test new field with default value.
    Assertions.assertEquals(expectedResult, SchemaCompatibility.checkReaderWriterCompatibility(reader, WRITER_SCHEMA));
  }

  @Test
  void validateSchemaNewField() {
    final List<Schema.Field> readerFields = TestSchemas.list(
        new Schema.Field("oldfield1", TestSchemas.INT_SCHEMA, null, null),
        new Schema.Field("newfield1", TestSchemas.INT_SCHEMA, null, null));
    final Schema reader = Schema.createRecord(readerFields);
    SchemaCompatibility.SchemaPairCompatibility compatibility = SchemaCompatibility
        .checkReaderWriterCompatibility(reader, WRITER_SCHEMA);

    // Test new field without default value.
    assertEquals(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, compatibility.getType());
    assertEquals(SchemaCompatibility.SchemaCompatibilityResult.incompatible(
        SchemaCompatibility.SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE, reader, WRITER_SCHEMA,
        "newfield1", asList("", "fields", "1")), compatibility.getResult());
    assertEquals(String.format(
        "Data encoded using writer schema:%n%s%n" + "will or may fail to decode using reader schema:%n%s%n",
        WRITER_SCHEMA.toString(true), reader.toString(true)), compatibility.getDescription());
    assertEquals(reader, compatibility.getReader());
    assertEquals(WRITER_SCHEMA, compatibility.getWriter());
  }

  @Test
  void validateArrayWriterSchema() {
    final Schema validReader = Schema.createArray(TestSchemas.STRING_SCHEMA);
    final Schema invalidReader = Schema.createMap(TestSchemas.STRING_SCHEMA);
    final SchemaCompatibility.SchemaPairCompatibility validResult = new SchemaCompatibility.SchemaPairCompatibility(
        SchemaCompatibility.SchemaCompatibilityResult.compatible(), validReader, TestSchemas.STRING_ARRAY_SCHEMA,
        SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);
    final SchemaCompatibility.SchemaPairCompatibility invalidResult = new SchemaCompatibility.SchemaPairCompatibility(
        SchemaCompatibility.SchemaCompatibilityResult.incompatible(
            SchemaCompatibility.SchemaIncompatibilityType.TYPE_MISMATCH, invalidReader, TestSchemas.STRING_ARRAY_SCHEMA,
            "reader type: MAP not compatible with writer type: ARRAY", Collections.singletonList("")),
        invalidReader, TestSchemas.STRING_ARRAY_SCHEMA,
        String.format(
            "Data encoded using writer schema:%n%s%n" + "will or may fail to decode using reader schema:%n%s%n",
            TestSchemas.STRING_ARRAY_SCHEMA.toString(true), invalidReader.toString(true)));

    Assertions.assertEquals(validResult,
        SchemaCompatibility.checkReaderWriterCompatibility(validReader, TestSchemas.STRING_ARRAY_SCHEMA));
    Assertions.assertEquals(invalidResult,
        SchemaCompatibility.checkReaderWriterCompatibility(invalidReader, TestSchemas.STRING_ARRAY_SCHEMA));
  }

  @Test
  void validatePrimitiveWriterSchema() {
    final Schema validReader = Schema.create(Schema.Type.STRING);
    final SchemaCompatibility.SchemaPairCompatibility validResult = new SchemaCompatibility.SchemaPairCompatibility(
        SchemaCompatibility.SchemaCompatibilityResult.compatible(), validReader, TestSchemas.STRING_SCHEMA,
        SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);
    final SchemaCompatibility.SchemaPairCompatibility invalidResult = new SchemaCompatibility.SchemaPairCompatibility(
        SchemaCompatibility.SchemaCompatibilityResult.incompatible(
            SchemaCompatibility.SchemaIncompatibilityType.TYPE_MISMATCH, TestSchemas.INT_SCHEMA,
            TestSchemas.STRING_SCHEMA, "reader type: INT not compatible with writer type: STRING",
            Collections.singletonList("")),
        TestSchemas.INT_SCHEMA, TestSchemas.STRING_SCHEMA,
        String.format(
            "Data encoded using writer schema:%n%s%n" + "will or may fail to decode using reader schema:%n%s%n",
            TestSchemas.STRING_SCHEMA.toString(true), TestSchemas.INT_SCHEMA.toString(true)));

    Assertions.assertEquals(validResult,
        SchemaCompatibility.checkReaderWriterCompatibility(validReader, TestSchemas.STRING_SCHEMA));
    Assertions.assertEquals(invalidResult,
        SchemaCompatibility.checkReaderWriterCompatibility(TestSchemas.INT_SCHEMA, TestSchemas.STRING_SCHEMA));
  }

  /**
   * Reader union schema must contain all writer union branches.
   */
  @Test
  void unionReaderWriterSubsetIncompatibility() {
    final Schema unionWriter = Schema
        .createUnion(TestSchemas.list(TestSchemas.INT_SCHEMA, TestSchemas.STRING_SCHEMA, TestSchemas.LONG_SCHEMA));
    final Schema unionReader = Schema.createUnion(TestSchemas.list(TestSchemas.INT_SCHEMA, TestSchemas.STRING_SCHEMA));
    final SchemaCompatibility.SchemaPairCompatibility result = SchemaCompatibility
        .checkReaderWriterCompatibility(unionReader, unionWriter);
    Assertions.assertEquals(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, result.getType());
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Collection of reader/writer schema pair that are compatible.
   */
  public static final List<TestSchemas.ReaderWriter> COMPATIBLE_READER_WRITER_TEST_CASES = TestSchemas.list(
      new TestSchemas.ReaderWriter(TestSchemas.BOOLEAN_SCHEMA, TestSchemas.BOOLEAN_SCHEMA),

      new TestSchemas.ReaderWriter(TestSchemas.INT_SCHEMA, TestSchemas.INT_SCHEMA),

      new TestSchemas.ReaderWriter(TestSchemas.LONG_SCHEMA, TestSchemas.INT_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.LONG_SCHEMA, TestSchemas.LONG_SCHEMA),

      // Avro spec says INT/LONG can be promoted to FLOAT/DOUBLE.
      // This is arguable as this causes a loss of precision.
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_SCHEMA, TestSchemas.INT_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_SCHEMA, TestSchemas.LONG_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_SCHEMA, TestSchemas.LONG_SCHEMA),

      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_SCHEMA, TestSchemas.INT_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_SCHEMA, TestSchemas.FLOAT_SCHEMA),

      new TestSchemas.ReaderWriter(TestSchemas.STRING_SCHEMA, TestSchemas.STRING_SCHEMA),

      new TestSchemas.ReaderWriter(TestSchemas.BYTES_SCHEMA, TestSchemas.BYTES_SCHEMA),

      new TestSchemas.ReaderWriter(TestSchemas.INT_ARRAY_SCHEMA, TestSchemas.INT_ARRAY_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.LONG_ARRAY_SCHEMA, TestSchemas.INT_ARRAY_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.INT_MAP_SCHEMA, TestSchemas.INT_MAP_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.LONG_MAP_SCHEMA, TestSchemas.INT_MAP_SCHEMA),

      new TestSchemas.ReaderWriter(TestSchemas.ENUM1_AB_SCHEMA, TestSchemas.ENUM1_AB_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.ENUM1_ABC_SCHEMA, TestSchemas.ENUM1_AB_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.ENUM1_AB_SCHEMA_DEFAULT, TestSchemas.ENUM1_ABC_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.ENUM1_AB_SCHEMA, TestSchemas.ENUM1_AB_SCHEMA_NAMESPACE_1),
      new TestSchemas.ReaderWriter(TestSchemas.ENUM1_AB_SCHEMA_NAMESPACE_1, TestSchemas.ENUM1_AB_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.ENUM1_AB_SCHEMA_NAMESPACE_1, TestSchemas.ENUM1_AB_SCHEMA_NAMESPACE_2),

      // String-to/from-bytes, introduced in Avro 1.7.7
      new TestSchemas.ReaderWriter(TestSchemas.STRING_SCHEMA, TestSchemas.BYTES_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.BYTES_SCHEMA, TestSchemas.STRING_SCHEMA),

      // Tests involving unions:
      new TestSchemas.ReaderWriter(TestSchemas.EMPTY_UNION_SCHEMA, TestSchemas.EMPTY_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_UNION_SCHEMA, TestSchemas.EMPTY_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_UNION_SCHEMA, TestSchemas.INT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_UNION_SCHEMA, TestSchemas.LONG_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_UNION_SCHEMA, TestSchemas.INT_LONG_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.INT_UNION_SCHEMA, TestSchemas.INT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.INT_STRING_UNION_SCHEMA, TestSchemas.STRING_INT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.INT_UNION_SCHEMA, TestSchemas.EMPTY_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.LONG_UNION_SCHEMA, TestSchemas.EMPTY_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.LONG_UNION_SCHEMA, TestSchemas.INT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_UNION_SCHEMA, TestSchemas.INT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_UNION_SCHEMA, TestSchemas.INT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_UNION_SCHEMA, TestSchemas.LONG_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_UNION_SCHEMA, TestSchemas.LONG_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_UNION_SCHEMA, TestSchemas.EMPTY_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_UNION_SCHEMA, TestSchemas.FLOAT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.STRING_UNION_SCHEMA, TestSchemas.EMPTY_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.STRING_UNION_SCHEMA, TestSchemas.BYTES_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.BYTES_UNION_SCHEMA, TestSchemas.EMPTY_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.BYTES_UNION_SCHEMA, TestSchemas.STRING_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_UNION_SCHEMA, TestSchemas.INT_FLOAT_UNION_SCHEMA),

      // Readers capable of reading all branches of a union are compatible
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_SCHEMA, TestSchemas.INT_FLOAT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.LONG_SCHEMA, TestSchemas.INT_LONG_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_SCHEMA, TestSchemas.INT_FLOAT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.DOUBLE_SCHEMA, TestSchemas.INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA),

      // Special case of singleton unions:
      new TestSchemas.ReaderWriter(TestSchemas.FLOAT_SCHEMA, TestSchemas.FLOAT_UNION_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.INT_UNION_SCHEMA, TestSchemas.INT_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.INT_SCHEMA, TestSchemas.INT_UNION_SCHEMA),
      // Fixed types
      new TestSchemas.ReaderWriter(TestSchemas.FIXED_4_BYTES, TestSchemas.FIXED_4_BYTES),

      // Tests involving records:
      new TestSchemas.ReaderWriter(TestSchemas.EMPTY_RECORD1, TestSchemas.EMPTY_RECORD1),
      new TestSchemas.ReaderWriter(TestSchemas.EMPTY_RECORD1, TestSchemas.A_INT_RECORD1),

      new TestSchemas.ReaderWriter(TestSchemas.A_INT_RECORD1, TestSchemas.A_INT_RECORD1),
      new TestSchemas.ReaderWriter(TestSchemas.A_DINT_RECORD1, TestSchemas.A_INT_RECORD1),
      new TestSchemas.ReaderWriter(TestSchemas.A_DINT_RECORD1, TestSchemas.A_DINT_RECORD1),
      new TestSchemas.ReaderWriter(TestSchemas.A_INT_RECORD1, TestSchemas.A_DINT_RECORD1),

      new TestSchemas.ReaderWriter(TestSchemas.A_LONG_RECORD1, TestSchemas.A_INT_RECORD1),

      new TestSchemas.ReaderWriter(TestSchemas.A_INT_RECORD1, TestSchemas.A_INT_B_INT_RECORD1),
      new TestSchemas.ReaderWriter(TestSchemas.A_DINT_RECORD1, TestSchemas.A_INT_B_INT_RECORD1),

      new TestSchemas.ReaderWriter(TestSchemas.A_INT_B_DINT_RECORD1, TestSchemas.A_INT_RECORD1),
      new TestSchemas.ReaderWriter(TestSchemas.A_DINT_B_DINT_RECORD1, TestSchemas.EMPTY_RECORD1),
      new TestSchemas.ReaderWriter(TestSchemas.A_DINT_B_DINT_RECORD1, TestSchemas.A_INT_RECORD1),
      new TestSchemas.ReaderWriter(TestSchemas.A_INT_B_INT_RECORD1, TestSchemas.A_DINT_B_DINT_RECORD1),

      new TestSchemas.ReaderWriter(TestSchemas.INT_LIST_RECORD, TestSchemas.INT_LIST_RECORD),
      new TestSchemas.ReaderWriter(TestSchemas.LONG_LIST_RECORD, TestSchemas.LONG_LIST_RECORD),
      new TestSchemas.ReaderWriter(TestSchemas.LONG_LIST_RECORD, TestSchemas.INT_LIST_RECORD),

      new TestSchemas.ReaderWriter(TestSchemas.NULL_SCHEMA, TestSchemas.NULL_SCHEMA),
      new TestSchemas.ReaderWriter(TestSchemas.ENUM_AB_ENUM_DEFAULT_A_RECORD,
          TestSchemas.ENUM_ABC_ENUM_DEFAULT_A_RECORD),
      new TestSchemas.ReaderWriter(TestSchemas.ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD,
          TestSchemas.ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD),

      // This is comparing two records that have an inner array of records with
      // different namespaces.
      new TestSchemas.ReaderWriter(TestSchemas.NS_RECORD1, TestSchemas.NS_RECORD2),
      new TestSchemas.ReaderWriter(TestSchemas.WITHOUT_NS, TestSchemas.WITH_NS));

  // -----------------------------------------------------------------------------------------------

  /**
   * The reader/writer pairs that are incompatible are now moved to specific test
   * classes, one class per error case (for easier pinpointing of errors). The
   * method to validate incompatibility is still here.
   */
  public static void validateIncompatibleSchemas(Schema reader, Schema writer,
      SchemaCompatibility.SchemaIncompatibilityType incompatibility, String message, String location) {
    validateIncompatibleSchemas(reader, writer, Collections.singletonList(incompatibility),
        Collections.singletonList(message), Collections.singletonList(location));
  }

  // -----------------------------------------------------------------------------------------------

  public static void validateIncompatibleSchemas(Schema reader, Schema writer,
      List<SchemaCompatibility.SchemaIncompatibilityType> incompatibilityTypes, List<String> messages,
      List<String> locations) {
    SchemaCompatibility.SchemaPairCompatibility compatibility = SchemaCompatibility
        .checkReaderWriterCompatibility(reader, writer);
    SchemaCompatibility.SchemaCompatibilityResult compatibilityResult = compatibility.getResult();
    assertEquals(reader, compatibility.getReader());
    assertEquals(writer, compatibility.getWriter());
    Assertions.assertEquals(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE,
        compatibilityResult.getCompatibility());

    assertEquals(incompatibilityTypes.size(), compatibilityResult.getIncompatibilities().size());
    for (int i = 0; i < incompatibilityTypes.size(); i++) {
      SchemaCompatibility.Incompatibility incompatibility = compatibilityResult.getIncompatibilities().get(i);
      TestSchemas.assertSchemaContains(incompatibility.getReaderFragment(), reader);
      TestSchemas.assertSchemaContains(incompatibility.getWriterFragment(), writer);
      assertEquals(incompatibilityTypes.get(i), incompatibility.getType());
      assertEquals(messages.get(i), incompatibility.getMessage());
      assertEquals(locations.get(i), incompatibility.getLocation());
    }

    String description = String.format(
        "Data encoded using writer schema:%n%s%n" + "will or may fail to decode using reader schema:%n%s%n",
        writer.toString(true), reader.toString(true));
    assertEquals(description, compatibility.getDescription());
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Tests reader/writer compatibility validation.
   */
  @Test
  void readerWriterCompatibility() {
    for (TestSchemas.ReaderWriter readerWriter : COMPATIBLE_READER_WRITER_TEST_CASES) {
      final Schema reader = readerWriter.getReader();
      final Schema writer = readerWriter.getWriter();
      LOG.debug("Testing compatibility of reader {} with writer {}.", reader, writer);
      final SchemaCompatibility.SchemaPairCompatibility result = SchemaCompatibility
          .checkReaderWriterCompatibility(reader, writer);
      Assertions.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, result.getType(), String
          .format("Expecting reader %s to be compatible with writer %s, but tested incompatible.", reader, writer));
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Descriptor for a test case that encodes a datum according to a given writer
   * schema, then decodes it according to reader schema and validates the decoded
   * value.
   */
  private static final class DecodingTestCase {
    /**
     * Writer schema used to encode the datum.
     */
    private final Schema mWriterSchema;

    /**
     * Datum to encode according to the specified writer schema.
     */
    private final Object mDatum;

    /**
     * Reader schema used to decode the datum encoded using the writer schema.
     */
    private final Schema mReaderSchema;

    /**
     * Expected datum value when using the reader schema to decode from the writer
     * schema.
     */
    private final Object mDecodedDatum;

    public DecodingTestCase(final Schema writerSchema, final Object datum, final Schema readerSchema,
        final Object decoded) {
      mWriterSchema = writerSchema;
      mDatum = datum;
      mReaderSchema = readerSchema;
      mDecodedDatum = decoded;
    }

    public Schema getReaderSchema() {
      return mReaderSchema;
    }

    public Schema getWriterSchema() {
      return mWriterSchema;
    }

    public Object getDatum() {
      return mDatum;
    }

    public Object getDecodedDatum() {
      return mDecodedDatum;
    }
  }

  // -----------------------------------------------------------------------------------------------

  public static final List<DecodingTestCase> DECODING_COMPATIBILITY_TEST_CASES = TestSchemas.list(
      new DecodingTestCase(TestSchemas.INT_SCHEMA, 1, TestSchemas.INT_SCHEMA, 1),
      new DecodingTestCase(TestSchemas.INT_SCHEMA, 1, TestSchemas.LONG_SCHEMA, 1L),
      new DecodingTestCase(TestSchemas.INT_SCHEMA, 1, TestSchemas.FLOAT_SCHEMA, 1.0f),
      new DecodingTestCase(TestSchemas.INT_SCHEMA, 1, TestSchemas.DOUBLE_SCHEMA, 1.0d),

      // This is currently accepted but causes a precision loss:
      // IEEE 754 floats have 24 bits signed mantissa
      new DecodingTestCase(TestSchemas.INT_SCHEMA, (1 << 24) + 1, TestSchemas.FLOAT_SCHEMA, (float) ((1 << 24) + 1)),

      // new DecodingTestCase(LONG_SCHEMA, 1L, INT_SCHEMA, 1), // should work in
      // best-effort!

      new DecodingTestCase(TestSchemas.ENUM1_AB_SCHEMA, new GenericData.EnumSymbol(TestSchemas.ENUM1_AB_SCHEMA, "A"),
          TestSchemas.ENUM1_ABC_SCHEMA, new GenericData.EnumSymbol(TestSchemas.ENUM1_ABC_SCHEMA, "A")),

      new DecodingTestCase(TestSchemas.ENUM1_ABC_SCHEMA, new GenericData.EnumSymbol(TestSchemas.ENUM1_ABC_SCHEMA, "A"),
          TestSchemas.ENUM1_AB_SCHEMA, new GenericData.EnumSymbol(TestSchemas.ENUM1_AB_SCHEMA, "A")),

      new DecodingTestCase(TestSchemas.ENUM1_ABC_SCHEMA, new GenericData.EnumSymbol(TestSchemas.ENUM1_ABC_SCHEMA, "B"),
          TestSchemas.ENUM1_BC_SCHEMA, new GenericData.EnumSymbol(TestSchemas.ENUM1_BC_SCHEMA, "B")),

      new DecodingTestCase(TestSchemas.ENUM_ABC_ENUM_DEFAULT_A_SCHEMA,
          new GenericData.EnumSymbol(TestSchemas.ENUM_ABC_ENUM_DEFAULT_A_SCHEMA, "C"),
          TestSchemas.ENUM_AB_ENUM_DEFAULT_A_SCHEMA,
          new GenericData.EnumSymbol(TestSchemas.ENUM_AB_ENUM_DEFAULT_A_SCHEMA, "A")),

      new DecodingTestCase(TestSchemas.INT_STRING_UNION_SCHEMA, "the string", TestSchemas.STRING_SCHEMA,
          new Utf8("the string")),

      new DecodingTestCase(TestSchemas.INT_STRING_UNION_SCHEMA, "the string", TestSchemas.STRING_UNION_SCHEMA,
          new Utf8("the string")));

  /**
   * Tests the reader/writer compatibility at decoding time.
   */
  @Test
  void readerWriterDecodingCompatibility() throws Exception {
    for (DecodingTestCase testCase : DECODING_COMPATIBILITY_TEST_CASES) {
      final Schema readerSchema = testCase.getReaderSchema();
      final Schema writerSchema = testCase.getWriterSchema();
      final Object datum = testCase.getDatum();
      final Object expectedDecodedDatum = testCase.getDecodedDatum();

      LOG.debug("Testing incompatibility of reader {} with writer {}.", readerSchema, writerSchema);

      LOG.debug("Encode datum {} with writer {}.", datum, writerSchema);
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      final DatumWriter<Object> datumWriter = new GenericDatumWriter<>(writerSchema);
      datumWriter.write(datum, encoder);
      encoder.flush();

      LOG.debug("Decode datum {} whose writer is {} with reader {}.", datum, writerSchema, readerSchema);
      final byte[] bytes = baos.toByteArray();
      final Decoder decoder = DecoderFactory.get().resolvingDecoder(writerSchema, readerSchema,
          DecoderFactory.get().binaryDecoder(bytes, null));
      final DatumReader<Object> datumReader = new GenericDatumReader<>(readerSchema);
      final Object decodedDatum = datumReader.read(null, decoder);

      assertEquals(expectedDecodedDatum, decodedDatum,
          String.format(
              "Expecting decoded value %s when decoding value %s whose writer schema is %s "
                  + "using reader schema %s, but value was %s.",
              expectedDecodedDatum, datum, writerSchema, readerSchema, decodedDatum));
    }
  }

  private Schema readSchemaFromResources(String name) throws IOException {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(name)) {
      final String result = new BufferedReader(new InputStreamReader(inputStream)).lines()
          .collect(Collectors.joining("\n"));

      return new Schema.Parser().parse(result);
    }
  }

  @Test
  void checkResolvingDecoder() throws IOException {
    final Schema locationSchema = readSchemaFromResources("schema-location.json");
    final Schema writeSchema = readSchemaFromResources("schema-location-write.json");

    // For the read schema the long field has been removed
    // And a new field has been added, called long_r2
    // This one should be null.
    final Schema readSchema = readSchemaFromResources("schema-location-read.json");

    // Create some testdata
    GenericData.Record record = new GenericData.Record(writeSchema);
    GenericData.Record location = new GenericData.Record(locationSchema);

    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);

    HashMap<String, GenericData.Record> locations = new HashMap<>();
    locations.put("l1", location);
    record.put("location", locations);

    // Write the record to bytes
    byte[] payload;
    try (ByteArrayOutputStream bbos = new ByteArrayOutputStream()) {
      DatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(writeSchema);
      Encoder enc = EncoderFactory.get().binaryEncoder(bbos, null);
      datumWriter.write(record, enc);
      enc.flush();

      payload = bbos.toByteArray();
    }

    // Read the record, and decode it using the read with the long
    // And project it using the other schema with the long_r2
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
    GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>();
    reader.setSchema(writeSchema);
    reader.setExpected(readSchema);

    // Get the object we're looking for
    GenericData.Record r = reader.read(null, decoder);
    HashMap<Utf8, GenericData.Record> locs = (HashMap<Utf8, GenericData.Record>) r.get("location");
    GenericData.Record loc = locs.get(new Utf8("l1"));

    assertNotNull(loc.get("lat"));
    // This is a new field, and should be null
    assertNull(loc.get("long_r2"));
  }
}
