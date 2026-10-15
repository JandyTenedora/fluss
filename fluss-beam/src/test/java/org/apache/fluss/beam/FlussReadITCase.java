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
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises finite source reads through Beam against a local Fluss cluster. */
class FlussReadITCase {
    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    void testExclusiveRangeAndContinuation() throws Exception {
        TablePath path = TablePath.of("fluss", "beam_finite");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .build())
                                    .distributedBy(1)
                                    .build()
                                    .withReplicationFactor(1),
                            false)
                    .get(30, TimeUnit.SECONDS);
            try (Table table = connection.getTable(path)) {
                AppendWriter writer = table.newAppend().createWriter();
                for (int i = 0; i < 1205; i++) {
                    writer.append(GenericRow.of(i));
                }
                writer.flush();
                long end =
                        admin.listOffsets(
                                        path,
                                        Collections.singletonList(0),
                                        new OffsetSpec.LatestSpec())
                                .bucketResult(0)
                                .get(30, TimeUnit.SECONDS);
                assertThat(end).isEqualTo(1205);
                TableInfo info = table.getTableInfo();
                Pipeline pipeline = pipeline();
                PCollection<Integer> rows =
                        pipeline.apply(
                                FlussIO.read(
                                        CLUSTER.getClientConfig(),
                                        Arrays.asList(
                                                FlussReadRange.of(info, 0, 2, 1203),
                                                FlussReadRange.of(info, 0, 1205, 1205)),
                                        row -> row.getInt(0),
                                        VarIntCoder.of()));
                assertThat(rows.isBounded()).isEqualTo(PCollection.IsBounded.BOUNDED);
                List<Integer> expected =
                        IntStream.range(2, 1203).boxed().collect(Collectors.toList());
                PAssert.that(rows).containsInAnyOrder(expected);
                pipeline.run().waitUntilFinish();

                Pipeline invalid = pipeline();
                invalid.apply(
                        FlussIO.read(
                                CLUSTER.getClientConfig(),
                                Collections.singletonList(FlussReadRange.of(info, 0, 0, end + 1)),
                                row -> row.getInt(0),
                                VarIntCoder.of()));
                assertThatThrownBy(() -> invalid.run().waitUntilFinish())
                        .hasStackTraceContaining("Read end exceeds");
            }
        }
    }

    private static Pipeline pipeline() {
        DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.setRunner(DirectRunner.class);
        options.setTargetParallelism(2);
        return Pipeline.create(options);
    }
}
