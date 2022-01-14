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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Value;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link StructToMutation} */
@RunWith(JUnit4.class)
public class StructToMutationTest {

  private static Struct.Builder buildStruct(Struct.Builder structBuilder) {
    structBuilder
        .set("booleanCol")
        .to(true)
        .set("int64Col")
        .to(100L)
        .set("numericCol")
        .to(BigDecimal.valueOf(124567890123456.7890123456789))
        .set("float64Col")
        .to(123.45)
        .set("stringCol")
        .to("Hello World")
        .set("jsonCol")
        .to("{\"key\": \"value\"}") // JSON values returned as strings
        .set("bytesCol")
        .to(ByteArray.copyFrom("goodbye world"))
        .set("timestampCol")
        .to(Timestamp.parseTimestamp("2022-02-14T16:32:00Z"))
        .set("dateCol")
        .to(Date.parseDate("2022-03-30"));
    return structBuilder;
  }

  private static Struct.Builder buildStructWithArrays(Struct.Builder structBuilder) {
    structBuilder
        .set("booleanArrayCol")
        .toBoolArray(new boolean[] {true, false})
        .set("int64ArrayCol")
        .toInt64Array(new long[] {123, 4321})
        .set("numericArrayCol")
        .toNumericArray(
            Arrays.asList(BigDecimal.valueOf(124567890123456.7890123456789), BigDecimal.TEN))
        .set("float64ArrayCol")
        .toFloat64Array(new double[] {123.45, 6789.10})
        .set("stringArrayCol")
        .toStringArray(Arrays.asList("Hello World", "Foo, Bar"))
        .set("jsonArrayCol")
        .toJsonArray(Arrays.asList("{\"key\": \"value\"}", "{\"otherkey\": \"value2\"}"))
        .set("bytesArrayCol")
        .toBytesArray(
            Arrays.asList(ByteArray.copyFrom("goodbye world"), ByteArray.copyFrom("LipsumOrem")))
        .set("timestampArrayCol")
        .toTimestampArray(
            Arrays.asList(Timestamp.parseTimestamp("2022-02-14T16:32:00Z"), Timestamp.now()))
        .set("dateArrayCol")
        .toDateArray(
            Arrays.asList(Date.parseDate("2022-03-30"), Date.fromYearMonthDay(2020, 5, 27)));

    return structBuilder;
  }

  private static void validateMutationCols(Mutation m, Struct s) {
    Map<String, Value> colValueMap = m.asMap();

    // Mutation has all cols in struct.
    assertThat(colValueMap).hasSize(s.getColumnCount());

    String colName = "booleanCol";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.BOOL);
    assertThat(colValueMap.get(colName).getBool()).isEqualTo(s.getBoolean(colName));

    colName = "int64Col";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.INT64);
    assertThat(colValueMap.get(colName).getInt64()).isEqualTo(s.getLong(colName));

    colName = "numericCol";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.NUMERIC);
    assertThat(colValueMap.get(colName).getNumeric()).isEqualTo(s.getBigDecimal(colName));

    colName = "float64Col";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.FLOAT64);
    assertThat(colValueMap.get(colName).getFloat64()).isEqualTo(s.getDouble(colName));

    colName = "stringCol";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.STRING);
    assertThat(colValueMap.get(colName).getString()).isEqualTo(s.getString(colName));

    colName = "jsonCol";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.STRING);
    assertThat(colValueMap.get(colName).getString()).isEqualTo(s.getString(colName));

    colName = "bytesCol";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.BYTES);
    assertThat(colValueMap.get(colName).getBytes()).isEqualTo(s.getBytes(colName));

    colName = "timestampCol";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.TIMESTAMP);
    assertThat(colValueMap.get(colName).getTimestamp()).isEqualTo(s.getTimestamp(colName));

    colName = "dateCol";
    assertThat(colValueMap.get(colName).getType().getCode()).isEqualTo(Code.DATE);
    assertThat(colValueMap.get(colName).getDate()).isEqualTo(s.getDate(colName));

    colName = "booleanArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.BOOL);
    assertThat(colValueMap.get(colName).getBoolArray())
        .containsExactlyElementsIn(s.getBooleanList(colName));

    colName = "int64ArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.INT64);
    assertThat(colValueMap.get(colName).getInt64Array())
        .containsExactlyElementsIn(s.getLongList(colName));

    colName = "numericArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.NUMERIC);
    assertThat(colValueMap.get(colName).getNumericArray())
        .containsExactlyElementsIn(s.getBigDecimalList(colName));

    colName = "float64ArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.FLOAT64);
    assertThat(colValueMap.get(colName).getFloat64Array())
        .containsExactlyElementsIn(s.getDoubleList(colName));

    colName = "stringArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.STRING);
    assertThat(colValueMap.get(colName).getStringArray())
        .containsExactlyElementsIn(s.getStringList(colName));

    colName = "jsonArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.JSON);
    assertThat(colValueMap.get(colName).getJsonArray())
        .containsExactlyElementsIn(s.getJsonList(colName));

    colName = "bytesArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.BYTES);
    assertThat(colValueMap.get(colName).getBytesArray())
        .containsExactlyElementsIn(s.getBytesList(colName));

    colName = "timestampArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.TIMESTAMP);
    assertThat(colValueMap.get(colName).getTimestampArray())
        .containsExactlyElementsIn(s.getTimestampList(colName));

    colName = "dateArrayCol";
    assertThat(colValueMap.get(colName).getType().getArrayElementType().getCode())
        .isEqualTo(Code.DATE);
    assertThat(colValueMap.get(colName).getDateArray())
        .containsExactlyElementsIn(s.getDateList(colName));
  }

  @Test
  public void validateAllTypesTested() {
    // Test to catch that all data types supported are specified in the struct
    Set<Code> allCodes = new HashSet<>(Arrays.asList(Code.values()));
    // remove Struct  as it is not supported
    allCodes.remove(Code.STRUCT);
    // remove Array as it is tested elsewhere
    allCodes.remove(Code.ARRAY);
    // remove JSON as it is passed as a STRING
    allCodes.remove(Code.JSON);

    Struct s = buildStruct(Struct.newBuilder()).build();
    for (int colNum = 0; colNum < s.getColumnCount(); colNum++) {
      allCodes.remove(s.getColumnType(colNum).getCode());
    }
    assertThat(allCodes).isEmpty();
  }

  @Test
  public void validateAllArrayTypesTested() {
    // Test to catch that all array data types supported are specified in the struct
    Set<Code> allCodes = new HashSet<>(Arrays.asList(Code.values()));
    // remove Struct as it is not supported
    allCodes.remove(Code.STRUCT);
    // remove Array as Arrays of Arrays are not supported
    allCodes.remove(Code.ARRAY);

    Struct s = buildStructWithArrays(Struct.newBuilder()).build();
    for (int colNum = 0; colNum < s.getColumnCount(); colNum++) {
      allCodes.remove(s.getColumnType(colNum).getArrayElementType().getCode());
    }
    assertThat(allCodes).isEmpty();
  }

  @Test
  public void structToInsertMutation() {
    Struct s = buildStructWithArrays(buildStruct(Struct.newBuilder())).build();

    Mutation m = new StructToMutation("testTable", Op.INSERT).apply(s);

    assertThat(m.getOperation()).isEqualTo(Op.INSERT);
    assertThat(m.getTable()).isEqualTo("testTable");
    validateMutationCols(m, s);
  }

  @Test
  public void structToInsertOrUpdateMutation() {
    Struct s = buildStructWithArrays(buildStruct(Struct.newBuilder())).build();
    Mutation m = new StructToMutation("testTable2", Op.INSERT_OR_UPDATE).apply(s);

    assertThat(m.getOperation()).isEqualTo(Op.INSERT_OR_UPDATE);
    assertThat(m.getTable()).isEqualTo("testTable2");
    validateMutationCols(m, s);
  }

  @Test
  public void structToUpdateMutation() {
    Struct s = buildStructWithArrays(buildStruct(Struct.newBuilder())).build();
    Mutation m = new StructToMutation("testTable3", Op.UPDATE).apply(s);

    assertThat(m.getOperation()).isEqualTo(Op.UPDATE);
    assertThat(m.getTable()).isEqualTo("testTable3");
    validateMutationCols(m, s);
  }

  @Test
  public void structToReplaceMutation() {
    Struct s = buildStructWithArrays(buildStruct(Struct.newBuilder())).build();
    Mutation m = new StructToMutation("testTable4", Op.REPLACE).apply(s);

    assertThat(m.getOperation()).isEqualTo(Op.REPLACE);
    assertThat(m.getTable()).isEqualTo("testTable4");
    validateMutationCols(m, s);
  }

  @Test
  public void structToDeleteMutation() {
    Struct s = buildStruct(Struct.newBuilder()).build();
    Mutation m = new StructToMutation("testTable5", Op.DELETE).apply(s);

    assertThat(m.getOperation()).isEqualTo(Op.DELETE);
    assertThat(m.getTable()).isEqualTo("testTable5");
    List<Object> keyParts =
        ImmutableList.copyOf(m.getKeySet().getKeys().iterator().next().getParts());

    assertThat(keyParts).hasSize(9);
    // booleanCol
    assertThat(keyParts.get(0)).isEqualTo(Boolean.TRUE);
    // int64Col
    assertThat(keyParts.get(1)).isEqualTo(100L);
    // numericCol
    assertThat(keyParts.get(2)).isEqualTo(BigDecimal.valueOf(124567890123456.7890123456789));
    // float64Col
    assertThat(keyParts.get(3)).isEqualTo(123.45);
    // stringCol
    assertThat(keyParts.get(4)).isEqualTo(("Hello World"));
    // jsonCol
    assertThat(keyParts.get(5)).isEqualTo(("{\"key\": \"value\"}"));
    // bytesCol
    assertThat(keyParts.get(6)).isEqualTo(ByteArray.copyFrom("goodbye world"));
    // timestampCol
    assertThat(keyParts.get(7)).isEqualTo(Timestamp.parseTimestamp("2022-02-14T16:32:00Z"));
    // dateCol
    assertThat(keyParts.get(8)).isEqualTo(Date.parseDate("2022-03-30"));
  }

  @Test
  public void mutationConvertAnonymousColumns() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new StructToMutation("test", Op.INSERT)
                    .apply(Struct.newBuilder().set("").to("value").build()));
    assertThat(e.getMessage()).isEqualTo("Anonymous column at position: 0");
  }

  @Test
  public void mutationConvertUnsupportedTypes() {
    Struct kvStruct = Struct.newBuilder().set("key").to("value").build();

    // Structs not supported
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new StructToMutation("test", Op.INSERT)
                    .apply(Struct.newBuilder().set("structCol").to(kvStruct).build()));
    assertThat(e.getMessage()).isEqualTo("Unsupported column type in position: 0: STRUCT");

    // Array of Structs not supported
    e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new StructToMutation("test", Op.INSERT)
                    .apply(
                        Struct.newBuilder()
                            .set("arrayOfStructCol")
                            .toStructArray(kvStruct.getType(), Arrays.asList(kvStruct, kvStruct))
                            .build()));
    assertThat(e.getMessage())
        .isEqualTo("Unsupported array column value type in position: 0: STRUCT");

    // Structs not possible as key values for Delete
    e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new StructToMutation("test", Op.DELETE)
                    .apply(Struct.newBuilder().set("structCol").to(kvStruct).build()));
    assertThat(e.getMessage()).isEqualTo("Unsupported key column type in position: 0: STRUCT");

    // Arrays not possible as key values for Delete
    e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new StructToMutation("test", Op.DELETE)
                    .apply(
                        Struct.newBuilder()
                            .set("arrayCol")
                            .toInt64Array(new long[] {0, 1})
                            .build()));
    assertThat(e.getMessage()).isEqualTo("Unsupported key column type in position: 0: ARRAY");
  }
}
