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
import org.apache.fluss.metadata.TableInfo;

import java.io.Serializable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A finite bucket log range, pinned to a table identity and schema. */
@PublicEvolving
public final class FlussReadRange implements Serializable {
    private static final long serialVersionUID = 1L;

    final String database;
    final String table;
    final long tableId;
    final int schemaId;
    final int bucket;
    final long from;
    final long to;

    private FlussReadRange(TableInfo info, int bucket, long from, long to) {
        this.database = info.getTablePath().getDatabaseName();
        this.table = info.getTablePath().getTableName();
        this.tableId = info.getTableId();
        this.schemaId = info.getSchemaId();
        this.bucket = bucket;
        this.from = from;
        this.to = to;
    }

    /**
     * Describes offsets {@code [from, to)} of a non-partitioned log table.
     *
     * <p>The caller resolves the table metadata and explicit offsets before constructing the
     * pipeline. The read fails if the table identity/schema changes or required history expires.
     */
    public static FlussReadRange of(TableInfo info, int bucket, long from, long to) {
        checkNotNull(info, "table metadata");
        checkArgument(!info.hasPrimaryKey(), "Only log tables are supported");
        checkArgument(!info.isPartitioned(), "Only non-partitioned tables are supported");
        checkArgument(bucket >= 0 && bucket < info.getNumBuckets(), "Invalid bucket: %s", bucket);
        checkArgument(from >= 0 && to >= from, "Expected 0 <= from <= to");
        return new FlussReadRange(info, bucket, from, to);
    }
}
