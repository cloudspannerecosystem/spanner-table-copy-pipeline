/*
 * Copyright 2022 Google LLC
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>https://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions;

import com.google.api.client.util.Strings;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.ValueBinder;
import java.util.List;
import org.apache.beam.sdk.transforms.SimpleFunction;

/** Converts a Spanner Struct to a Spanner Mutation depending on the Write Mode. */
class StructToMutation extends SimpleFunction<Struct, Mutation> {

  private final String destinationTable;
  private final Mutation.Op writeMode;

  public StructToMutation(String destinationTable, Mutation.Op writeMode) {
    this.writeMode = writeMode;
    this.destinationTable = destinationTable;
  }

  @Override
  public Mutation apply(Struct row) {
    Mutation.WriteBuilder writeBuilder;
    switch (writeMode) {
      case DELETE:
        // Deletes are handled by converting column values to a Key object
        // and by using a Delete mutation.
        return Mutation.delete(destinationTable, buildKeyFromStruct(row));

      case INSERT:
        writeBuilder = Mutation.newInsertBuilder(destinationTable);
        break;
      case UPDATE:
        writeBuilder = Mutation.newUpdateBuilder(destinationTable);
        break;
      case REPLACE:
        writeBuilder = Mutation.newReplaceBuilder(destinationTable);
        break;
      case INSERT_OR_UPDATE:
        writeBuilder = Mutation.newInsertOrUpdateBuilder(destinationTable);
        break;
      default:
        throw new IllegalArgumentException("Invalid writeMode: " + writeMode);
    }

    setMutationColumnValues(row, writeBuilder);
    return writeBuilder.build();
  }

  private void setMutationColumnValues(Struct row, WriteBuilder mutation) {
    // Build Write Mutation object using the row's column names and values
    List<StructField> cols = row.getType().getStructFields();
    for (int colNum = 0; colNum < row.getColumnCount(); colNum++) {
      final String columnName = cols.get(colNum).getName();
      if (Strings.isNullOrEmpty(columnName)) {
        throw new IllegalArgumentException("Anonymous column at position: " + colNum);
      }
      ValueBinder<WriteBuilder> pendingValue = mutation.set(columnName);

      Value columnValue = row.getValue(colNum);
      switch (columnValue.getType().getCode()) {
        case BOOL:
          pendingValue.to(columnValue.getBool());
          break;
        case INT64:
          pendingValue.to(columnValue.getInt64());
          break;
        case NUMERIC:
          pendingValue.to(columnValue.getNumeric());
          break;
        case FLOAT64:
          pendingValue.to(columnValue.getFloat64());
          break;
        case STRING:
          pendingValue.to(columnValue.getString());
          break;
        case JSON:
          pendingValue.to(columnValue.getJson());
          break;
        case BYTES:
          pendingValue.to(columnValue.getBytes());
          break;
        case TIMESTAMP:
          pendingValue.to(columnValue.getTimestamp());
          break;
        case DATE:
          pendingValue.to(columnValue.getDate());
          break;
        case ARRAY:
          switch (columnValue.getType().getArrayElementType().getCode()) {
            case BOOL:
              pendingValue.toBoolArray(columnValue.getBoolArray());
              break;
            case INT64:
              pendingValue.toInt64Array(columnValue.getInt64Array());
              break;
            case NUMERIC:
              pendingValue.toNumericArray(columnValue.getNumericArray());
              break;
            case FLOAT64:
              pendingValue.toFloat64Array(columnValue.getFloat64Array());
              break;
            case STRING:
              pendingValue.toStringArray(columnValue.getStringArray());
              break;
            case JSON:
              pendingValue.toJsonArray(columnValue.getJsonArray());
              break;
            case BYTES:
              pendingValue.toBytesArray(columnValue.getBytesArray());
              break;
            case TIMESTAMP:
              pendingValue.toTimestampArray(columnValue.getTimestampArray());
              break;
            case DATE:
              pendingValue.toDateArray(columnValue.getDateArray());
              break;
            default:
              throw new IllegalArgumentException(
                  "Unsupported array column value type in position: "
                      + colNum
                      + ": "
                      + columnValue.getType().getArrayElementType().getCode().toString());
          }
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported column type in position: "
                  + colNum
                  + ": "
                  + columnValue.getType().getCode().toString());
      }
    }
  }

  private Key buildKeyFromStruct(Struct row) {
    Key.Builder keyBuilder = Key.newBuilder();
    for (int colNum = 0; colNum < row.getColumnCount(); colNum++) {
      Value value = row.getValue(colNum);
      switch (value.getType().getCode()) {
        case BOOL:
          keyBuilder.append(value.getBool());
          break;
        case INT64:
          keyBuilder.append(value.getInt64());
          break;
        case NUMERIC:
          keyBuilder.append(value.getNumeric());
          break;
        case FLOAT64:
          keyBuilder.append(value.getFloat64());
          break;
        case STRING:
          keyBuilder.append(value.getString());
          break;
        case BYTES:
          keyBuilder.append(value.getBytes());
          break;
        case TIMESTAMP:
          keyBuilder.append(value.getTimestamp());
          break;
        case DATE:
          keyBuilder.append(value.getDate());
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported key column type in position: "
                  + colNum
                  + ": "
                  + value.getType().getCode());
      }
    }
    return keyBuilder.build();
  }
}
