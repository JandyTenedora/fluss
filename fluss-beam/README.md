<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Bounded Fluss reads for Apache Beam

This module reads explicit `[start, end)` offset ranges from non-partitioned
Fluss **log tables**. Each range is one Beam input element. A Splittable DoFn
uses Beam's `OffsetRangeTracker` to claim log positions, split work and expose
remaining work to the runner. Records are converted using a caller-supplied
serializable mapper and output coder.

Only reads are included. There is no sink, mapping lookup, continuous tailing,
partition discovery, KV snapshot read, lake-file read, event-time watermark policy
or general Fluss-to-Beam schema conversion. Beam SDK 2.71.0 (Apache-2.0) supplies
the SDK APIs; DirectRunner is a test dependency. There is no Flink dependency.
Source code uses Java 8 syntax; the Fluss build uses Java 11.

## Example: read part of an INT log table

The table in this example has one `INT` column at index 0. Resolve its metadata
and offsets once before constructing the pipeline. Do not resolve `latest` anew
inside each retry, since that would change the finite range.

```java
Configuration configuration = new Configuration();
configuration.setString("bootstrap.servers", "localhost:9123");
TablePath path = TablePath.of("fluss", "events");
FlussReadRange range;
try (Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin()) {
    TableInfo info = admin.getTableInfo(path).get();
    long end = admin.listOffsets(path, Collections.singletonList(0),
                    new OffsetSpec.LatestSpec())
            .bucketResult(0).get();
    long start = admin.listOffsets(path, Collections.singletonList(0),
                    new OffsetSpec.EarliestSpec())
            .bucketResult(0).get();
    range = FlussReadRange.of(info, 0, start, end);
}

Pipeline pipeline = Pipeline.create(options);
PCollection<Integer> ids = pipeline.apply(FlussIO.read(
        configuration,
        Collections.singletonList(range),
        row -> row.getInt(0),
        VarIntCoder.of()));
// Attach the application's processing to ids before running.
pipeline.run().waitUntilFinish();
```

A caller may supply several disjoint bucket ranges for parallelism. Overlapping
ranges intentionally reread overlapping records. Empty ranges emit no records.
Table identity and schema ID are captured in `FlussReadRange`; reads fail on a
changed identity/schema, an expired starting offset, or an end beyond the log.
Historical records with another schema ID also fail rather than being silently
interpreted using an incompatible mapper. A new job needs its ranges supplied
again: this API does not maintain an external consumer group or cross-job cursor.

The mapper sees a borrowed `InternalRow`; copy all retained data into detached
output values. Do not return the row itself or retain its internal buffers. The
mapper must not perform external effects because failed work can be replayed.
Configuration is serialized in the pipeline graph; use worker-accessible
credentials rather than embedding secrets. Input ranges contain metadata only.

## How recovery works

1. `FlussReadFn.initialRestriction` supplies `[start, end)` to Beam.
2. `newTracker` uses Beam's built-in offset tracker, including progress reporting.
3. `process` opens a scanner at the restriction's start and calls `tryClaim`
   before mapping/emitting a record. A rejected claim stops processing.
4. After at most 1,000 emitted records, `ProcessContinuation.resume()` lets the
   runner checkpoint the remaining restriction. Dynamic splits also constrain
   the active range.
5. Recovery opens another scanner from the work assigned by the runner. An
   in-memory claim is **not** a durable commit; failed work may be reread.

Offset gaps are not counted as records. Scanner `consumedUpToOffset` progress is
claimed only after preceding records have been emitted. Empty polls alone never
advance the restriction. A read fails after 30 seconds without progress on known
retained backlog; metadata offset requests also have a 30-second timeout.
Resources are closed per invocation and per worker. Teardown is cleanup, not a
commit step. Output uses default Beam timestamps and does not promise log order
across workers or ranges.

## Read the implementation in this order

- `FlussIO.java`: construct the range collection and apply the Splittable DoFn.
- `FlussReadRange.java`: the stable identity and finite work requested by the caller.
- `FlussReadFn.java`: restriction, coder, tracker, claims, continuations and resource lifecycle.
- `FlussReadFnTest.java`: checkpoint residuals and progress-only offset gaps.
- `FlussReadITCase.java`: a local Fluss server plus DirectRunner, including
  exclusive boundaries, empty ranges, continuation and an invalid future end.

## Validation

From the repository root, with Java 11 and dependencies available:

```bash
./mvnw -pl fluss-beam -am spotless:check
./mvnw -pl fluss-beam -am verify -Dcheckstyle.skip=false
```

For a focused run after installing matching upstream modules locally:

```bash
./mvnw -pl fluss-beam verify
```

Local DirectRunner coverage does not certify Dataflow/Flink compatibility,
worker-loss recovery or production scaling. Those require runner-specific tests.
The connector does not provide exactly-once external side effects.
