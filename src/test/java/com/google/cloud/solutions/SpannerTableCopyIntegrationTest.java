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

import com.google.cloud.Timestamp;
import com.google.cloud.solutions.SpannerTableCopy.SpannerTableCopyOptions;
import com.google.cloud.solutions.testing.SpannerEmulator;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This test requires the Cloud Spanner Emulator to be installed.
 *
 * <p>see https://cloud.google.com/spanner/docs/emulator#installing_and_running_the_emulator
 */
@RunWith(JUnit4.class)
public class SpannerTableCopyIntegrationTest {

  // Use custom ports to avoid collision with other emulators.
  private static final int EMULATOR_PORT = 29010;
  private static final String EMULATOR_HOST = "localhost:" + EMULATOR_PORT;
  private static final int EMULATOR_REST_PORT = 29020;

  public static final String PROJECT_ID = "dummy-project-id";
  public static final String INSTANCE_ID = "test";
  public static final String DATABASE_ID = "test";

  private static final ImmutableMap<Long, String> TEST_DATA =
      ImmutableMap.of(
          1L, "Hello World",
          2L, "Goodbye World");

  @ClassRule
  public static SpannerEmulator emulator =
      SpannerEmulator.builder()
          .projectId(PROJECT_ID)
          .instanceId(INSTANCE_ID)
          .databaseId(DATABASE_ID)
          .emulatorPort(EMULATOR_PORT)
          .emulatorRestPort(EMULATOR_REST_PORT)
          .ddlStatements(
              "CREATE TABLE source (key INT64, value STRING(MAX)) PRIMARY KEY(key)",
              "CREATE TABLE dest (key INT64, value STRING(MAX)) PRIMARY KEY(key)")
          .build();

  @Before
  public void setUpTestDb() {
    emulator
        .getDatabaseClient()
        .writeAtLeastOnce(
            TEST_DATA.entrySet().stream()
                .map(
                    (entry) ->
                        Mutation.newInsertBuilder("source")
                            .set("key")
                            .to(entry.getKey())
                            .set("value")
                            .to(entry.getValue())
                            .build())
                .collect(Collectors.toList()));
  }

  @After
  public void tearDownTestDb() {
    emulator
        .getDatabaseClient()
        .readWriteTransaction()
        .run(
            txn -> {
              txn.executeUpdate(Statement.of("DELETE FROM source WHERE TRUE"));
              txn.executeUpdate(Statement.of("DELETE FROM dest WHERE TRUE"));
              return null;
            });
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private static final ImmutableList<String> COMMON_ARGS =
      ImmutableList.of(
          "--sourceProjectId=" + PROJECT_ID,
          "--sourceInstanceId=" + INSTANCE_ID,
          "--sourceDatabaseId=" + DATABASE_ID,
          "--sourceEmulatorHost=" + EMULATOR_HOST,
          "--destinationProjectId=" + PROJECT_ID,
          "--destinationInstanceId=" + INSTANCE_ID,
          "--destinationDatabaseId=" + DATABASE_ID,
          "--destinationEmulatorHost=" + EMULATOR_HOST);

  @Test
  public void copySourceToDest() throws IOException {
    ImmutableList<String> args =
        new ImmutableList.Builder<String>()
            .addAll(COMMON_ARGS)
            .add("--sqlQuery=SELECT * FROM source")
            .add("--destinationTable=dest")
            .add("--writeMode=INSERT")
            .add("--mutationReportFile=" + folder.newFile("mutations").getPath())
            .add("--failureLogFile=" + folder.newFile("failures").getPath())
            .build();

    SpannerTableCopy.buildPipeline(
            PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
                .withValidation()
                .as(SpannerTableCopyOptions.class),
            pipeline)
        .run()
        .waitUntilFinish();

    assertThat(readAllRowsFromTable("dest")).containsExactlyEntriesIn(TEST_DATA);

    assertThat(readLinesFromFiles("mutations"))
        .containsExactlyElementsIn(
            TEST_DATA.entrySet().stream()
                .map(
                    entry ->
                        String.format(
                            "insert(dest{key=%d,value=%s})", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));

    assertThat(readLinesFromFiles("failures")).isEmpty();
  }

  @Test
  public void copySourceToDestDryRun() throws IOException {
    ImmutableList<String> args =
        new ImmutableList.Builder<String>()
            .addAll(COMMON_ARGS)
            .add("--sqlQuery=SELECT * FROM source")
            .add("--destinationTable=dest")
            .add("--writeMode=INSERT")
            .add("--mutationReportFile=" + folder.newFile("mutations").getPath())
            .add("--failureLogFile=" + folder.newFile("failures").getPath())
            .add("--dryRun")
            .build();

    SpannerTableCopy.buildPipeline(
            PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
                .withValidation()
                .as(SpannerTableCopyOptions.class),
            pipeline)
        .run()
        .waitUntilFinish();

    assertThat(readAllRowsFromTable("dest")).isEmpty();

    assertThat(readLinesFromFiles("mutations"))
        .containsExactlyElementsIn(
            TEST_DATA.entrySet().stream()
                .map(
                    entry ->
                        String.format(
                            "insert(dest{key=%d,value=%s})", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));

    assertThat(readLinesFromFiles("failures")).isEmpty();
  }

  @Test
  public void pointInTimeRecoverDeletedRowsToDest() throws InterruptedException, IOException {
    // allow some time to pass to avoid flakyness. Emulator does not seem
    // to support sub-second commit timestamps, so the delete/insert during @After and @Before
    // may be at the same time...

    Thread.sleep(1000);
    Timestamp timestamp = Timestamp.now();
    Thread.sleep(1000);

    // Delete all rows
    emulator
        .getDatabaseClient()
        .writeAtLeastOnce(ImmutableList.of(Mutation.delete("source", KeySet.all())));
    // verify source is empty
    assertThat(readAllRowsFromTable("source")).isEmpty();

    ImmutableList<String> args =
        new ImmutableList.Builder<String>()
            .addAll(COMMON_ARGS)
            .add("--sqlQuery=SELECT * FROM source")
            .add("--readTimestamp=" + timestamp)
            .add("--destinationTable=dest")
            .add("--writeMode=REPLACE")
            .add("--mutationReportFile=" + folder.newFile("mutations").getPath())
            .add("--failureLogFile=" + folder.newFile("failures").getPath())
            .build();

    SpannerTableCopy.buildPipeline(
            PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
                .withValidation()
                .as(SpannerTableCopyOptions.class),
            pipeline)
        .run()
        .waitUntilFinish();

    assertThat(readAllRowsFromTable("dest")).containsExactlyEntriesIn(TEST_DATA);

    assertThat(readLinesFromFiles("mutations"))
        .containsExactlyElementsIn(
            TEST_DATA.entrySet().stream()
                .map(
                    entry ->
                        String.format(
                            "replace(dest{key=%d,value=%s})", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));

    assertThat(readLinesFromFiles("failures")).isEmpty();
  }

  @Test
  public void transformRowsToDest() throws IOException {
    ImmutableList<String> args =
        new ImmutableList.Builder<String>()
            .addAll(COMMON_ARGS)
            .add(
                "--sqlQuery=SELECT key*100 as key,REPLACE(value, \"World\", \"Jupiter\") as value"
                    + " FROM source")
            .add("--destinationTable=dest")
            .add("--writeMode=INSERT")
            .add("--mutationReportFile=" + folder.newFile("mutations").getPath())
            .add("--failureLogFile=" + folder.newFile("failures").getPath())
            .build();

    SpannerTableCopy.buildPipeline(
            PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
                .withValidation()
                .as(SpannerTableCopyOptions.class),
            pipeline)
        .run()
        .waitUntilFinish();

    assertThat(readAllRowsFromTable("dest"))
        .containsExactlyEntriesIn(
            TEST_DATA.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        (e) -> (e.getKey() * 100L),
                        (e) -> e.getValue().replace("World", "Jupiter"))));

    assertThat(readLinesFromFiles("mutations"))
        .containsExactlyElementsIn(
            TEST_DATA.entrySet().stream()
                .map(
                    entry ->
                        String.format(
                            "insert(dest{key=%d,value=%s})",
                            entry.getKey() * 100, entry.getValue().replace("World", "Jupiter")))
                .collect(Collectors.toList()));
    assertThat(readLinesFromFiles("failures")).isEmpty();
  }

  @Test
  public void catchingFailuresBadColumnNames() throws IOException {
    ImmutableList<String> args =
        new ImmutableList.Builder<String>()
            .addAll(COMMON_ARGS)
            .add("--sqlQuery=SELECT key*15 as notkey, \"hello\" as notvalue from source") // bad col
            // names
            .add("--destinationTable=dest")
            .add("--writeMode=INSERT")
            .add("--mutationReportFile=" + folder.newFile("mutations").getPath())
            .add("--failureLogFile=" + folder.newFile("failures").getPath())
            .build();

    SpannerTableCopy.buildPipeline(
            PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
                .withValidation()
                .as(SpannerTableCopyOptions.class),
            pipeline)
        .run()
        .waitUntilFinish();

    assertThat(readAllRowsFromTable("dest")).isEmpty();

    final List<String> expectedMutations =
        TEST_DATA.keySet().stream()
            .map(k -> String.format("insert(dest{notkey=%d,notvalue=hello})", k * 15))
            .collect(Collectors.toList());
    assertThat(readLinesFromFiles("mutations")).containsExactlyElementsIn(expectedMutations);
    assertThat(readLinesFromFiles("failures")).containsExactlyElementsIn(expectedMutations);
  }

  @Test
  public void catchingFailuresAnonymousColumn() throws IOException {
    ImmutableList<String> args =
        new ImmutableList.Builder<String>()
            .addAll(COMMON_ARGS)
            .add("--sqlQuery=SELECT key*15 as notkey, \"hello\" from source") // anon column 1
            .add("--destinationTable=dest")
            .add("--writeMode=INSERT")
            .add("--mutationReportFile=" + folder.newFile("mutations").getPath())
            .add("--failureLogFile=" + folder.newFile("failures").getPath())
            .build();

    SpannerTableCopy.buildPipeline(
        PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
            .withValidation()
            .as(SpannerTableCopyOptions.class),
        pipeline);

    PipelineExecutionException e =
        assertThrows(PipelineExecutionException.class, () -> pipeline.run().waitUntilFinish());
    assertThat(e.getMessage()).contains("Anonymous column at position: 1");

    assertThat(readAllRowsFromTable("dest")).isEmpty();
    assertThat(readLinesFromFiles("mutations")).isEmpty();
    assertThat(readLinesFromFiles("failures")).isEmpty();
  }

  @Test
  public void catchingFailuresExistingRow() throws IOException {

    emulator
        .getDatabaseClient()
        .writeAtLeastOnce(
            ImmutableList.of(
                Mutation.newInsertBuilder("dest")
                    .set("key")
                    .to(1)
                    .set("value")
                    .to("existing row")
                    .build()));

    ImmutableList<String> args =
        new ImmutableList.Builder<String>()
            .addAll(COMMON_ARGS)
            .add("--sqlQuery=SELECT key,value from source")
            .add("--destinationTable=dest")
            .add("--writeMode=INSERT")
            .add("--mutationReportFile=" + folder.newFile("mutations").getPath())
            .add("--failureLogFile=" + folder.newFile("failures").getPath())
            .build();

    SpannerTableCopy.buildPipeline(
            PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
                .withValidation()
                .as(SpannerTableCopyOptions.class),
            pipeline)
        .run()
        .waitUntilFinish();

    assertThat(readAllRowsFromTable("dest"))
        .containsExactly(1L, "existing row", 2L, "Goodbye World");

    assertThat(readLinesFromFiles("mutations"))
        .containsExactlyElementsIn(
            TEST_DATA.entrySet().stream()
                .map(
                    entry ->
                        String.format(
                            "insert(dest{key=%d,value=%s})", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));

    assertThat(readLinesFromFiles("failures"))
        .containsExactly("insert(dest{key=1,value=Hello World})");
  }

  @Test
  public void deleteFromDest() throws IOException {

    emulator
        .getDatabaseClient()
        .writeAtLeastOnce(
            ImmutableList.of(
                Mutation.newInsertBuilder("dest")
                    .set("key")
                    .to(1)
                    .set("value")
                    .to("existing row")
                    .build(),
                Mutation.newInsertBuilder("dest")
                    .set("key")
                    .to(3)
                    .set("value")
                    .to("existing row3")
                    .build()));

    ImmutableList<String> args =
        new ImmutableList.Builder<String>()
            .addAll(COMMON_ARGS)
            .add("--sqlQuery=SELECT key FROM source")
            .add("--destinationTable=dest")
            .add("--writeMode=DELETE")
            .add("--mutationReportFile=" + folder.newFile("mutations").getPath())
            .add("--failureLogFile=" + folder.newFile("failures").getPath())
            .build();

    SpannerTableCopy.buildPipeline(
            PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
                .withValidation()
                .as(SpannerTableCopyOptions.class),
            pipeline)
        .run()
        .waitUntilFinish();

    assertThat(readAllRowsFromTable("dest")).containsExactly(3L, "existing row3");

    assertThat(readLinesFromFiles("mutations"))
        .containsExactlyElementsIn(
            TEST_DATA.keySet().stream()
                .map(k -> String.format("delete(dest{[%d]})", k))
                .collect(Collectors.toList()));

    assertThat(readLinesFromFiles("failures")).isEmpty();
  }

  private ImmutableMap<Long, String> readAllRowsFromTable(String table) {
    try (ResultSet resultSet =
        emulator
            .getDatabaseClient()
            .singleUse()
            .read(table, KeySet.all(), ImmutableList.of("key", "value"))) {
      ImmutableMap.Builder<Long, String> rb = new ImmutableMap.Builder<>();
      while (resultSet.next()) {
        Struct row = resultSet.getCurrentRowAsStruct();
        rb.put(row.getLong("key"), row.getString("value"));
      }
      return rb.build();
    }
  }

  private List<String> readLinesFromFiles(String filePrefix) throws IOException {
    String[] files = folder.getRoot().list((dir, name) -> name.startsWith(filePrefix));
    assertThat(files).isNotNull();
    List<String> mutations = new LinkedList<>();
    for (String file : files) {
      mutations.addAll(
          Files.readAllLines(new File(folder.getRoot(), file).toPath(), StandardCharsets.UTF_8));
    }
    return mutations;
  }
}
