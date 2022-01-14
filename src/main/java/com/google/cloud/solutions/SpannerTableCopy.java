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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam pipeline to run a query on a Spanner database and write the results to a spanner table.
 *
 * <p>This pipeline can be used for various functions to transform/update a database, without
 * needing to be concerned about transaction mutation limitations
 *
 * <ul>
 *   <li>Transforming read data using SQL and writing the results back, eg. the equivalent of the
 *       following pseudo-sql:
 *       <pre>
 *   INSERT OR UPDATE INTO &lt;table&gt; (key, value)
 *   (
 *      SELECT key, value*100 FROM &lt;table&gt; WHERE &lt;condition&gt;
 *   )
 *   </pre>
 *   <li>Point-in-time recovery by reading the database state at a point in the past (within the
 *       Spanner database's <a href="https://cloud.google.com/spanner/docs/pitr">version retention
 *       period</a>, and then writing it back. eg. the equivalent of the following pseudo-sql:
 *       <pre>
 *   INSERT OR UPDATE INTO &lt;table&gt; (key, value)
 *   (
 *      SELECT key, value
 *      FROM &lt;table&gt; FOR SYSTEM_TIME AS OF &lt;timestamp-RFC3339&gt;
 *      WHERE &lt;condition&gt;
 *   )
 *   </pre>
 *   <li>Copying data from one database to another, eg. the equivalent of the following pseudo-sql:
 *       <pre>
 *  INSERT OR UPDATE INTO &lt;table@project2/instance2/database2&gt; (key, value)
 *  (SELECT key, value FROM &lt;table@project1/instance1/database1&gt;)
 *  </pre>
 * </ul>
 *
 * <h2>Usage:</h2>
 *
 * <p>Show help text:
 *
 * <pre>
 *     mvn compile exec:java -Dexec.mainClass=com.google.cloud.solutions.SpannerTableCopy
 *         -Dexec.args='--help=com.google.cloud.solutions.SpannerTableCopy$SpannerTableCopyOptions'
 *  </pre>
 *
 * <p>Execute the pipeline:
 *
 * <pre>
 *  mvn compile exec:java -Dexec.mainClass=com.google.cloud.solutions.SpannerTableCopy \
 *   -Pdataflow-runner
 *   -Dexec.args="
 *      --runner=DataflowRunner
 *
 *      --sourceProjectId=SOURCE_PROJECT
 *      --sourceInstanceId=SOURCE_INSTANCE
 *      --sourceDatabaseId=SOURCE_DATABASE
 *
 *      --sqlQuery='select * from SOURCE_TABLE'
 *      --readTimestamp=2022-01-01T12:00:00Z
 *
 *      --destinationProjectId=DEST_PROJECT
 *      --destinationInstanceId=DEST_INSTANCE
 *      --destinationDatabaseId=DEST_DATABASE
 *      --destinationTable=DEST_TABLE
 *      --writeMode=INSERT_OR_UPDATE
 *
 *      --mutationReportFile=gs://BUCKET/PATH/report"
 *      --failureLogFile=gs://BUCKET/PATH/failures"
 *  </pre>
 *
 * <p>The <tt>--dryRun</tt> parameter can be used to create the mutation report file without
 * actually writing to the database.
 *
 * <p><tt>--writeMode</tt> values correspond to the values of <a
 * href="https://googleapis.dev/java/google-cloud-spanner/latest/com/google/cloud/spanner/Mutation.Op.html">Mutation.Op</a>
 */
public class SpannerTableCopy {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerTableCopy.class);

  /** Enum to be used fot the Source/Destination Priority command line argument. */
  public enum RpcPriority {
    DEFAULT,
    LOW,
    HIGH
  }

  private static final SimpleFunction<Mutation, String> MUTATION_TO_STRING =
      new SimpleFunction<Mutation, String>() {
        @Override
        public String apply(Mutation input) {
          return input.toString();
        }
      };

  public static final DoFn<MutationGroup, String> MUTATION_GROUP_TO_MUTATION_STRINGS =
      new DoFn<MutationGroup, String>() {
        @ProcessElement
        public void processElement(@Element MutationGroup input, OutputReceiver<String> out) {
          // expand MutationGroups
          for (Mutation m : input) {
            out.output(MUTATION_TO_STRING.apply(m));
          }
        }
      };

  /** Options supported by {@link SpannerTableCopy}. */
  public interface SpannerTableCopyOptions extends PipelineOptions {

    @Description(
        "Timestamp to read database state. Must be in the past, and within the GC window.Input"
            + " string should be in the RFC 3339 format, like '2020-12-01T10:15:30.000Z' or with"
            + " the timezone offset, such as '2020-12-01T10:15:30+01:00'. Defaults to the time when"
            + " the pipeline was launched.")
    String getReadTimestamp();

    void setReadTimestamp(String value);

    @Description("Project ID for the source database. Defaults to the current project")
    String getSourceProjectId();

    void setSourceProjectId(String value);

    @Description("Instance ID of the source database")
    @Validation.Required
    String getSourceInstanceId();

    void setSourceInstanceId(String value);

    @Description("Database ID of the source database")
    @Validation.Required
    String getSourceDatabaseId();

    void setSourceDatabaseId(String value);

    @Description("Hostname of the Cloud Spanner Emulator if used as source")
    String getSourceEmulatorHost();

    void setSourceEmulatorHost(String value);

    @Description("Set the read priority")
    @Default.Enum("DEFAULT")
    RpcPriority getSourcePriority();

    void setSourcePriority(RpcPriority value);

    @Description(
        "SQL Query to use to read the source table. For optimal performance, the query"
            + "must be able to be partitioned "
            + "(see https://cloud.google.com/spanner/docs/reads#read_data_in_parallel) ")
    @Validation.Required
    String getSqlQuery();

    void setSqlQuery(String value);

    @Description("Project ID of the destination database. Defaults to the current project ID")
    String getDestinationProjectId();

    void setDestinationProjectId(String value);

    @Description("Instance ID of the destination database")
    @Validation.Required
    String getDestinationInstanceId();

    void setDestinationInstanceId(String value);

    @Description("Database ID of the destination database")
    @Validation.Required
    String getDestinationDatabaseId();

    void setDestinationDatabaseId(String value);

    @Description("Hostname of the Cloud Spanner Emulator if used as destination")
    String getDestinationEmulatorHost();

    void setDestinationEmulatorHost(String value);

    @Description("Destination table name.")
    @Validation.Required
    String getDestinationTable();

    void setDestinationTable(String value);

    @Description(
        "Method for writing data. See method descriptions at"
            + " https://googleapis.dev/java/google-cloud-spanner/latest/com/google/cloud/spanner/Mutation.Op.html"
            + " Note DELETE mode requires that the source sql statement selects ONLY the key"
            + " columns IN PRIMARY KEY ORDER")
    @Validation.Required
    Mutation.Op getWriteMode();

    void setWriteMode(Mutation.Op value);

    @Description("Set the destination write priority")
    @Default.Enum("DEFAULT")
    RpcPriority getDestinationPriority();

    void setDestinationPriority(RpcPriority value);

    @Description("Path to a writable GCS bucket where any failed writes can be logged.")
    String getFailureLogFile();

    void setFailureLogFile(String value);

    @Description(
        "Dry Run mode: No writes to the destination database are made."
            + " The the list of mutations can be written to the file in --mutationReportFile")
    @Default.Boolean(false)
    boolean getDryRun();

    void setDryRun(boolean value);

    @Description(
        "GCS location to write a report of all mutations attempted. This is useful with "
            + "--dry-run mode to get a list of actions that would be performed.")
    String getMutationReportFile();

    void setMutationReportFile(String value);

    @Description(
        "Wait for the pipeline to finish"
            + " (if submitted to an external service such as Dataflow).")
    boolean getWaitUntilFinish();

    void setWaitUntilFinish(boolean value);
  }

  @VisibleForTesting
  static Pipeline buildPipeline(SpannerTableCopyOptions options, Pipeline p) {

    SpannerIO.Read spannerRead =
        SpannerIO.read()
            .withInstanceId(options.getSourceInstanceId())
            .withDatabaseId(options.getSourceDatabaseId())
            .withQuery(options.getSqlQuery());

    if (!Strings.isNullOrEmpty(options.getReadTimestamp())) {
      Timestamp timestamp = Timestamp.parseTimestamp(options.getReadTimestamp());
      LOGGER.info("Stale read at timestamp: " + timestamp);
      spannerRead = spannerRead.withTimestamp(timestamp);
    }
    if (!Strings.isNullOrEmpty(options.getSourceProjectId())) {
      spannerRead = spannerRead.withProjectId(options.getSourceProjectId());
    }
    if (!Strings.isNullOrEmpty(options.getSourceEmulatorHost())) {
      spannerRead = spannerRead.withEmulatorHost(options.getSourceEmulatorHost());
    }
    switch (options.getSourcePriority()) {
      case LOW:
        spannerRead.withLowPriority();
        break;
      case HIGH:
        spannerRead.withHighPriority();
        break;
      case DEFAULT:
        // ignore
        break;
      default:
        throw new IllegalArgumentException(
            "Unhandled --sourcePriority " + options.getSourcePriority());
    }

    PCollection<Mutation> mutations =
        p.apply("Read from Spanner", spannerRead)
            .apply(
                "Struct To Mutation",
                MapElements.via(
                    new StructToMutation(options.getDestinationTable(), options.getWriteMode())));

    if (options.getDryRun()) {
      LOGGER.info("Dry run, no mutations will be written to the Database");
    } else {
      // Write to Spanner
      SpannerIO.Write spannerWrite =
          SpannerIO.write()
              .withInstanceId(options.getDestinationInstanceId())
              .withDatabaseId(options.getDestinationDatabaseId())
              .withFailureMode(FailureMode.REPORT_FAILURES);

      if (!Strings.isNullOrEmpty(options.getDestinationProjectId())) {
        spannerWrite = spannerWrite.withProjectId(options.getDestinationProjectId());
      }
      if (!Strings.isNullOrEmpty(options.getDestinationEmulatorHost())) {
        spannerWrite = spannerWrite.withEmulatorHost(options.getDestinationEmulatorHost());
      }
      switch (options.getDestinationPriority()) {
        case LOW:
          spannerWrite.withLowPriority();
          break;
        case HIGH:
          spannerWrite.withHighPriority();
          break;
        case DEFAULT:
          // ignore
          break;
        default:
          throw new IllegalArgumentException(
              "Unhandled --destinationPriority " + options.getDestinationPriority());
      }

      SpannerWriteResult result = mutations.apply("Write to Spanner", spannerWrite);

      if (!Strings.isNullOrEmpty(options.getFailureLogFile())) {
        // Write a file listing all the mutations that have failed.
        LOGGER.info("Failed mutations will be written to " + options.getFailureLogFile());
        result
            .getFailedMutations()
            .apply("MutationGroup To Strings", ParDo.of(MUTATION_GROUP_TO_MUTATION_STRINGS))
            .apply("Write Failures Log", TextIO.write().to(options.getFailureLogFile()));
      }
    }

    if (!Strings.isNullOrEmpty(options.getMutationReportFile())) {
      // Write a file listing all the mutations that are attempted.
      LOGGER.info("Report of all mutations will be written to " + options.getMutationReportFile());
      mutations
          .apply("Mutation To Strings", MapElements.via(MUTATION_TO_STRING))
          .apply("Write Mutation Report", TextIO.write().to(options.getMutationReportFile()));
    }

    return p;
  }

  /**
   * Main entry point.
   *
   * @param args command line
   */
  public static void main(String[] args) {
    PipelineOptionsFactory.register(SpannerTableCopyOptions.class);
    SpannerTableCopyOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerTableCopyOptions.class);

    LOGGER.info("Building pipeline");
    Pipeline p = buildPipeline(options, Pipeline.create(options));

    LOGGER.info("Starting pipeline");
    PipelineResult result = p.run();

    if (options.getWaitUntilFinish()) {
      result.waitUntilFinish();
      LOGGER.info("Pipeline completed");
    }
  }
}
