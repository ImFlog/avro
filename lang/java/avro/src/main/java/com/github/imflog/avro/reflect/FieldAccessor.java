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
package com.github.imflog.avro.reflect;

import java.io.IOException;
import java.lang.reflect.Field;

import com.github.imflog.avro.io.Decoder;
import com.github.imflog.avro.io.Encoder;

abstract class FieldAccessor {
  FieldAccessor() {
  }

  protected abstract Object get(Object object) throws IllegalAccessException;

  protected abstract void set(Object object, Object value) throws IllegalAccessException, IOException;

  protected void read(Object object, Decoder in) throws IOException {
  }

  protected void write(Object object, Encoder out) throws IOException {
  }

  protected boolean supportsIO() {
    return false;
  }

  protected abstract Field getField();

  protected boolean isStringable() {
    return false;
  }

  protected boolean isCustomEncoded() {
    return false;
  }

}
