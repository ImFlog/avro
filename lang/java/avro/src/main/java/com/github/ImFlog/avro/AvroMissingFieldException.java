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

import java.util.ArrayList;
import java.util.List;

/** Avro exception in case of missing fields. */
public class AvroMissingFieldException extends AvroRuntimeException {
  private List<Schema.Field> chainOfFields = new ArrayList<>(8);

  public AvroMissingFieldException(String message, Schema.Field field) {
    super(message);
    chainOfFields.add(field);
  }

  public void addParentField(Schema.Field field) {
    chainOfFields.add(field);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (Schema.Field field : chainOfFields) {
      result.insert(0, " --> " + field.name());
    }
    return "Path in schema:" + result;
  }
}
