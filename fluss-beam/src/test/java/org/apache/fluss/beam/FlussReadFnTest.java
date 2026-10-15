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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the boundary between consumed Fluss positions and Beam restrictions. */
class FlussReadFnTest {
    @Test
    void testProgressOnlyPollCompletesExclusiveRange() {
        OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(5, 10));
        assertThat(tracker.tryClaim(5L)).isTrue();
        assertThat(FlussReadFn.advancePastGaps(20L, 6L, tracker)).isEqualTo(10);
        assertThat(tracker.tryClaim(10L)).isFalse();
        tracker.checkDone();
    }

    @Test
    void testCheckpointResidualStartsAfterLastClaimedPosition() {
        OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(5, 10));
        assertThat(tracker.tryClaim(5L)).isTrue();
        assertThat(FlussReadFn.advancePastGaps(8L, 6L, tracker)).isEqualTo(8);
        SplitResult<OffsetRange> split = tracker.trySplit(0);
        assertThat(split).isNotNull();
        assertThat(split.getPrimary()).isEqualTo(new OffsetRange(5, 8));
        assertThat(split.getResidual()).isEqualTo(new OffsetRange(8, 10));
        assertThat(tracker.tryClaim(8L)).isFalse();
        tracker.checkDone();
        OffsetRangeTracker resumed = new OffsetRangeTracker(split.getResidual());
        assertThat(resumed.tryClaim(8L)).isTrue();
        assertThat(resumed.tryClaim(9L)).isTrue();
        assertThat(resumed.tryClaim(10L)).isFalse();
        resumed.checkDone();
    }

    @Test
    void testEmptyPollDoesNotClaimUnseenWork() {
        OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(5, 10));
        assertThat(FlussReadFn.advancePastGaps(null, 5L, tracker)).isEqualTo(5);
        assertThat(FlussReadFn.advancePastGaps(5L, 5L, tracker)).isEqualTo(5);
        assertThatThrownBy(tracker::checkDone).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRangeValidation() {
        TableInfo info =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                                .distributedBy(2)
                                .build());
        assertThatThrownBy(() -> FlussReadRange.of(info, 2, 0, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FlussReadRange.of(info, 0, 10, 9))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FlussReadRange.of(info, 0, -1, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(FlussReadRange.of(info, 0, 10, 10).from).isEqualTo(10);
        TableInfo keyed =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(2)
                                .build());
        assertThatThrownBy(() -> FlussReadRange.of(keyed, 0, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("log tables");
        TableInfo partitioned =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                                .partitionedBy("id")
                                .distributedBy(2)
                                .build());
        assertThatThrownBy(() -> FlussReadRange.of(partitioned, 0, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-partitioned");
    }

    private static TableInfo tableInfo(TableDescriptor descriptor) {
        return TableInfo.of(TablePath.of("db", "events"), 1L, 0, descriptor, null, 0, 0);
    }
}
