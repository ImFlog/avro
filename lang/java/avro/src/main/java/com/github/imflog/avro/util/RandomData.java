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
package com.github.imflog.avro.util;

import java.io.File;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.github.imflog.avro.LogicalType;
import com.github.imflog.avro.LogicalTypes;
import com.github.imflog.avro.Schema;
import com.github.imflog.avro.generic.GenericData;
import com.github.imflog.avro.generic.GenericDatumWriter;
import com.github.imflog.avro.generic.GenericArray;
import com.github.imflog.avro.generic.GenericRecord;
import com.github.imflog.avro.file.CodecFactory;
import com.github.imflog.avro.file.DataFileWriter;

/** Generates schema data as Java objects with random values. */
public class RandomData implements Iterable<Object> {
  public static final String USE_DEFAULT = "use-default";

  private static final int MILLIS_IN_DAY = (int) Duration.ofDays(1).toMillis();

  private final Schema root;
  private final long seed;
  private final int count;
  private final boolean utf8ForString;

  public RandomData(Schema schema, int count) {
    this(schema, count, false);
  }

  public RandomData(Schema schema, int count, long seed) {
    this(schema, count, seed, false);
  }

  public RandomData(Schema schema, int count, boolean utf8ForString) {
    this(schema, count, System.currentTimeMillis(), utf8ForString);
  }

  public RandomData(Schema schema, int count, long seed, boolean utf8ForString) {
    this.root = schema;
    this.seed = seed;
    this.count = count;
    this.utf8ForString = utf8ForString;
  }

  @Override
  public Iterator<Object> iterator() {
    return new Iterator<Object>() {
      private int n;
      private Random random = new Random(seed);

      @Override
      public boolean hasNext() {
        return n < count;
      }

      @Override
      public Object next() {
        n++;
        return generate(root, random, 0);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @SuppressWarnings(value = "unchecked")
  private Object generate(Schema schema, Random random, int d) {
    switch (schema.getType()) {
    case RECORD:
      GenericRecord record = new GenericData.Record(schema);
      for (Schema.Field field : schema.getFields()) {
        Object value = (field.getObjectProp(USE_DEFAULT) == null) ? generate(field.schema(), random, d + 1)
            : GenericData.get().getDefaultValue(field);
        record.put(field.name(), value);
      }
      return record;
    case ENUM:
      List<String> symbols = schema.getEnumSymbols();
      return new GenericData.EnumSymbol(schema, symbols.get(random.nextInt(symbols.size())));
    case ARRAY:
      int length = (random.nextInt(5) + 2) - d;
      @SuppressWarnings("rawtypes")
      GenericArray<Object> array = new GenericData.Array(length <= 0 ? 0 : length, schema);
      for (int i = 0; i < length; i++)
        array.add(generate(schema.getElementType(), random, d + 1));
      return array;
    case MAP:
      length = (random.nextInt(5) + 2) - d;
      Map<Object, Object> map = new HashMap<>(length <= 0 ? 0 : length);
      for (int i = 0; i < length; i++) {
        map.put(randomString(random, 40), generate(schema.getValueType(), random, d + 1));
      }
      return map;
    case UNION:
      List<Schema> types = schema.getTypes();
      return generate(types.get(random.nextInt(types.size())), random, d);
    case FIXED:
      byte[] bytes = new byte[schema.getFixedSize()];
      random.nextBytes(bytes);
      return new GenericData.Fixed(schema, bytes);
    case STRING:
      return randomString(random, 40);
    case BYTES:
      return randomBytes(random, 40);
    case INT:
      return this.randomInt(random, schema.getLogicalType());
    case LONG:
      return this.randomLong(random, schema.getLogicalType());
    case FLOAT:
      return random.nextFloat();
    case DOUBLE:
      return random.nextDouble();
    case BOOLEAN:
      return random.nextBoolean();
    case NULL:
      return null;
    default:
      throw new RuntimeException("Unknown type: " + schema);
    }
  }

  private static final Charset UTF8 = StandardCharsets.UTF_8;

  private int randomInt(Random random, LogicalType type) {
    if (type instanceof LogicalTypes.TimeMillis) {
      return random.nextInt(RandomData.MILLIS_IN_DAY - 1);
    }
    // LogicalTypes.Date LocalDate.MAX.toEpochDay() > Integer.MAX;
    return random.nextInt();
  }

  private long randomLong(Random random, LogicalType type) {
    if (type instanceof LogicalTypes.TimeMicros) {
      return ThreadLocalRandom.current().nextLong(RandomData.MILLIS_IN_DAY * 1000L);
    }
    // For LogicalTypes.TimestampMillis, every long would be OK,
    // Instant.MAX.toEpochMilli() failed and would be > Long.MAX_VALUE.
    return random.nextLong();
  }

  private Object randomString(Random random, int maxLength) {
    int length = random.nextInt(maxLength);
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) ('a' + random.nextInt('z' - 'a'));
    }
    return utf8ForString ? new Utf8(bytes) : new String(bytes, UTF8);
  }

  private static ByteBuffer randomBytes(Random rand, int maxLength) {
    ByteBuffer bytes = ByteBuffer.allocate(rand.nextInt(maxLength));
    ((Buffer) bytes).limit(bytes.capacity());
    rand.nextBytes(bytes.array());
    return bytes;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 4) {
      System.out.println("Usage: RandomData <schemafile> <outputfile> <count> [codec]");
      System.exit(-1);
    }
    Schema sch = new Schema.Parser().parse(new File(args[0]));
    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.setCodec(CodecFactory.fromString(args.length >= 4 ? args[3] : "null"));
      writer.setMeta("user_metadata", "someByteArray".getBytes(StandardCharsets.UTF_8));
      writer.create(sch, new File(args[1]));

      for (Object datum : new RandomData(sch, Integer.parseInt(args[2]))) {
        writer.append(datum);
      }
    }
  }
}
