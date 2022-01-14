# Spanner Table Copy

Beam pipeline to run a query on a Spanner database and write the results to a
spanner table.

This pipeline can be used for various functions to transform/update a database,
without needing to be concerned about transaction mutation limitations

* Transforming read data using SQL and writing the results back, e.g. the
  equivalent of the following pseudo-sql:

```sql
  INSERT OR UPDATE INTO <table> (key, value)
  (
     SELECT key, value*100 FROM <table> WHERE <condition>
  )
```

* Point-in-time recovery by reading the database state at a point in the past
  (within the Spanner database's
  <a href="https://cloud.google.com/spanner/docs/pitr">version retention
  period</a>, and then writing it back. e.g. the equivalent of the following
  pseudo-sql:

```sql
  INSERT OR UPDATE INTO <table> (key, value)
  (
     SELECT key, value
     FROM <table> FOR SYSTEM_TIME AS OF <timestamp_RFC3339>
     WHERE <condition>
  )
```

* Copying data from one database to another, e.g. the equivalent of the
  following pseudo-sql:

```sql
 INSERT OR UPDATE INTO <table@project2/instance2/database2> (key, value)
 (
    SELECT key, value FROM <table@project1/instance1/database1>
 )
```

## Usage:

### Show help text:

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.solutions.SpannerTableCopy
    -Dexec.args='--help=com.google.cloud.solutions.SpannerTableCopy$SpannerTableCopyOptions'
```

### Execute the pipeline:

For example copying a table at a timestamp in the past to a different database:

```
mvn compile exec:java
    -Dexec.mainClass=com.google.cloud.solutions.SpannerTableCopy \
    -Pdataflow-runner
    -Dexec.args="
         --runner=DataflowRunner

         --sourceProjectId=SOURCE_PROJECT
         --sourceInstanceId=SOURCE_INSTANCE
         --sourceDatabaseId=SOURCE_DATABASE

         --sqlQuery='select * from SOURCE_TABLE'
         --readTimestamp=2022-01-01T12:00:00Z

         --destinationProjectId=DEST_PROJECT
         --destinationInstanceId=DEST_INSTANCE
         --destinationDatabaseId=DEST_DATABASE
         --destinationTable=DEST_TABLE
         --writeMode=WRITE_MODE

         --mutationReportFile=gs://BUCKET/PATH/report
         --failureLogFile=gs://BUCKET/PATH/failures
    "
```

More examples can be found in the `SpannerTableCopyIntegrationTest`

Note: take care of Bash quoting with `-Dexec.args` and the `--sqlQuery` value.

The `--dryRun` parameter can be used to create the mutation report file without
actually writing to the database.

`--writeMode` values correspond to the values of the
[Mutation.Op enum](https://googleapis.dev/java/google-cloud-spanner/latest/com/google/cloud/spanner/Mutation.Op.html)
