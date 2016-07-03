/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class KafkaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KafkaTableHandle table;
    private final List<KafkaPartition> partitions;
    private final List<ColumnHandle> partitionColumns;
    private final TupleDomain<ColumnHandle> promisedPredicate;

    @JsonCreator
    public KafkaTableLayoutHandle(@JsonProperty("table") KafkaTableHandle table)
    {
        this.table = requireNonNull(table, "table is null");
        this.partitionColumns = null;
        this.promisedPredicate = null;
        this.partitions = null;
    }

    public KafkaTableLayoutHandle(
        KafkaTableHandle table,
        List<ColumnHandle> partitionColumns,
        List<KafkaPartition> partitions,
        TupleDomain<ColumnHandle> promisedPredicate)
    {
        this.table = requireNonNull(table, "table is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.promisedPredicate = requireNonNull(promisedPredicate, "promisedPredicate is null");
    }

    @JsonProperty
    public KafkaTableHandle getTable()
    {
        return table;
    }

    @JsonIgnore
    public List<ColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonIgnore
    public TupleDomain<ColumnHandle> getPromisedPredicate()
    {
        return promisedPredicate;
    }

    @JsonIgnore
    public Optional<List<KafkaPartition>> getPartitions()
    {
        return Optional.ofNullable(partitions);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
//    @Override
//    public boolean equals(Object o)
//    {
//        if (this == o) {
//            return true;
//        }
//        if (o == null || getClass() != o.getClass()) {
//            return false;
//        }
//        KafkaTableLayoutHandle that = (KafkaTableLayoutHandle) o;
//        return Objects.equals(table, that.table) &&
//            Objects.equals(partitionColumns, that.partitionColumns) &&
//            Objects.equals(partitions, that.partitions);
//    }
//
//    @Override
//    public int hashCode()
//    {
//        return Objects.hash(table, partitionColumns, partitions);
//    }
}
