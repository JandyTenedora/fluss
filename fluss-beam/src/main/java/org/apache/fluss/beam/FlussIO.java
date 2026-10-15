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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.row.InternalRow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Beam transforms for finite Fluss bucket log reads. */
@PublicEvolving
public final class FlussIO {
    private FlussIO() {}

    /**
     * Reads explicit, exclusive-end bucket ranges with Beam-managed offset restrictions.
     *
     * <p>The mapper must copy any values retained from the borrowed Fluss row into a detached
     * output value. It must be serializable, deterministic and free of external side effects: Beam
     * may rerun it after failure. Output timestamps are Beam's default timestamps. Overlapping
     * input ranges intentionally produce overlapping output.
     *
     * <p>Configuration is copied into the pipeline graph; use worker-accessible credentials rather
     * than embedding secrets. This transform provides no sink or exactly-once external effects. It
     * requires retained log history, not lake files or a KV snapshot.
     */
    public static <T> PTransform<PBegin, PCollection<T>> read(
            Configuration configuration,
            List<FlussReadRange> ranges,
            SerializableFunction<InternalRow, T> mapper,
            Coder<T> coder) {
        checkNotNull(configuration, "configuration");
        checkNotNull(ranges, "ranges");
        for (FlussReadRange range : ranges) {
            checkNotNull(range, "range");
        }
        return new Read<>(
                configuration.toMap(),
                ranges,
                checkNotNull(mapper, "mapper"),
                checkNotNull(coder, "coder"));
    }

    private static final class Read<T> extends PTransform<PBegin, PCollection<T>> {
        private final Map<String, String> configuration;
        private final List<FlussReadRange> ranges;
        private final SerializableFunction<InternalRow, T> mapper;
        private final Coder<T> coder;

        private Read(
                Map<String, String> configuration,
                List<FlussReadRange> ranges,
                SerializableFunction<InternalRow, T> mapper,
                Coder<T> coder) {
            this.configuration = new HashMap<>(configuration);
            this.ranges = new ArrayList<>(ranges);
            this.mapper = mapper;
            this.coder = coder;
        }

        @Override
        public PCollection<T> expand(PBegin input) {
            return input.apply(
                            "Bucket ranges",
                            Create.of(ranges).withCoder(SerializableCoder.of(FlussReadRange.class)))
                    .apply("Read Fluss log", ParDo.of(new FlussReadFn<>(configuration, mapper)))
                    .setCoder(coder);
        }
    }
}
