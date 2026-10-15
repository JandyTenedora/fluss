/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.beam;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.utils.Preconditions.checkState;

/** Maps one bucket range onto Beam's offset restriction protocol. */
@DoFn.BoundedPerElement
final class FlussReadFn<T> extends DoFn<FlussReadRange, T> {
    private static final long serialVersionUID = 1L;
    private static final long OPERATION_TIMEOUT_SECONDS = 30;
    private final Map<String, String> configuration;
    private final SerializableFunction<InternalRow, T> mapper;
    private transient Connection connection;

    FlussReadFn(Map<String, String> configuration, SerializableFunction<InternalRow, T> mapper) {
        this.configuration = new HashMap<>(configuration);
        this.mapper = mapper;
    }

    /** Provides the finite work range for a bucket. */
    @GetInitialRestriction
    public OffsetRange initialRestriction(@Element FlussReadRange range) {
        return new OffsetRange(range.from, range.to);
    }

    /** Serializes restrictions for runner persistence and redistribution. */
    @GetRestrictionCoder
    public Coder<OffsetRange> restrictionCoder() {
        return OffsetRange.Coder.of();
    }

    /** Uses Beam's built-in progress, splitting and completion tracking. */
    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
        return new OffsetRangeTracker(range);
    }

    /** Estimates remaining work in log positions, which may include positions without rows. */
    @GetSize
    public double size(@Restriction OffsetRange range) {
        return (double) range.getTo() - range.getFrom();
    }

    /** Creates a client in the worker process. */
    @Setup
    public void setup() {
        connection = ConnectionFactory.createConnection(Configuration.fromMap(configuration));
    }

    /** Closes worker resources; source correctness does not depend on teardown execution. */
    @Teardown
    public void teardown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    /** Reads a bounded amount of work before allowing Beam to checkpoint the residual. */
    @ProcessElement
    public ProcessContinuation process(
            @Element FlussReadRange range,
            RestrictionTracker<OffsetRange, Long> tracker,
            OutputReceiver<T> output)
            throws Exception {
        OffsetRange restriction = tracker.currentRestriction();
        if (restriction.getFrom() == restriction.getTo()) {
            tracker.tryClaim(restriction.getTo());
            return ProcessContinuation.stop();
        }
        TablePath path = TablePath.of(range.database, range.table);
        try (Admin admin = connection.getAdmin();
                Table table = connection.getTable(path)) {
            TableInfo info = table.getTableInfo();
            checkState(info.getTableId() == range.tableId, "Fluss table was replaced: %s", path);
            checkState(info.getSchemaId() == range.schemaId, "Fluss schema changed: %s", path);
            checkState(
                    !info.hasPrimaryKey() && !info.isPartitioned(),
                    "Expected a non-partitioned log table");
            long earliest = offset(admin, path, range.bucket, new OffsetSpec.EarliestSpec());
            long latest = offset(admin, path, range.bucket, new OffsetSpec.LatestSpec());
            checkState(restriction.getFrom() >= earliest, "Required Fluss history has expired");
            checkState(restriction.getTo() <= latest, "Read end exceeds the current Fluss log end");
            TableBucket bucket = new TableBucket(range.tableId, range.bucket);
            try (LogScanner scanner = table.newScan().createLogScanner()) {
                scanner.subscribe(range.bucket, restriction.getFrom());
                long next = restriction.getFrom();
                long lastProgress = System.nanoTime();
                int emitted = 0;
                while (true) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Fluss read interrupted");
                    }
                    if (next >= tracker.currentRestriction().getTo()) {
                        tracker.tryClaim(next);
                        return ProcessContinuation.stop();
                    }
                    ScanRecords polled = scanner.poll(Duration.ofMillis(100));
                    for (ScanRecord record : polled.records(bucket)) {
                        checkState(record.logOffset() >= next, "Regressing Fluss log offset");
                        if (!tracker.tryClaim(record.logOffset())) {
                            return ProcessContinuation.stop();
                        }
                        checkState(
                                record.getSchemaId() == range.schemaId,
                                "Fluss record schema changed");
                        output.output(mapper.apply(record.getRow()));
                        next = record.logOffset() + 1;
                        lastProgress = System.nanoTime();
                        if (++emitted >= 1000) {
                            return ProcessContinuation.resume();
                        }
                    }
                    long advanced =
                            advancePastGaps(polled.consumedUpToOffset(bucket), next, tracker);
                    if (advanced < 0) {
                        return ProcessContinuation.stop();
                    }
                    if (advanced > next) {
                        next = advanced;
                        lastProgress = System.nanoTime();
                    }
                    if (System.nanoTime() - lastProgress
                            > TimeUnit.SECONDS.toNanos(OPERATION_TIMEOUT_SECONDS)) {
                        throw new TimeoutException("No progress reading retained Fluss log range");
                    }
                }
            }
        }
    }

    // Consume invisible log positions only after all preceding materialized rows were emitted.
    static long advancePastGaps(
            Long consumed, long next, RestrictionTracker<OffsetRange, Long> tracker) {
        if (consumed == null || consumed <= next) {
            return next;
        }
        long end = tracker.currentRestriction().getTo();
        long target = Math.min(consumed, end) - 1;
        if (target >= next && !tracker.tryClaim(target)) {
            return -1;
        }
        return target + 1;
    }

    private static long offset(Admin admin, TablePath path, int bucket, OffsetSpec spec)
            throws Exception {
        return admin.listOffsets(path, Collections.singletonList(bucket), spec)
                .bucketResult(bucket)
                .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
