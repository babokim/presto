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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;

import static java.util.Objects.requireNonNull;

public class KafkaPartition
{
    public static final int UNPARTITIONED_ID = -1;

    private final SchemaTableName tableName;
    private final TupleDomain<KafkaColumnHandle> effectivePredicate;
    private final KafkaSegment segment;
    private final Map<ColumnHandle, NullableValue> partitionKeys;

    public KafkaPartition(SchemaTableName tableName,
                          TupleDomain<KafkaColumnHandle> effectivePredicate,
                          Map<ColumnHandle, NullableValue> partitionKeys,
                          KafkaSegment segment)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.segment = requireNonNull(segment, "segment is null");
        this.partitionKeys = ImmutableMap.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public KafkaSegment getSegment()
    {
        return segment;
    }

    public Map<ColumnHandle, NullableValue> getPartitionKeys()
    {
        return partitionKeys;
    }

    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.fromFixedValues(partitionKeys);
    }

    public TupleDomain<KafkaColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, segment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        KafkaPartition other = (KafkaPartition) obj;
        return Objects.equals(this.tableName, other.tableName) &&
            Objects.equals(this.segment, other.segment);
    }

    @Override
    public String toString()
    {
        return tableName + ":" + segment;
    }

    public long getPredicateOffset()
    {
        Map<KafkaColumnHandle, Domain> domains = effectivePredicate.getDomains().get();
        ImmutableSortedSet.Builder<Long> offsetValueBuilder = ImmutableSortedSet.naturalOrder();
        for (Map.Entry<KafkaColumnHandle, Domain> entry : domains.entrySet()) {
            Domain domain = entry.getValue();
            if (entry.getKey().getName().equals(KafkaInternalFieldDescription.PARTITION_OFFSET_FIELD.getName())) {
                ImmutableList<Long> values = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        ImmutableList.Builder<Long> columnValues = ImmutableList.builder();
                        for (Range range : ranges.getOrderedRanges()) {
                            // if the range is not a single value, we can not perform partition pruning
                            if (range.isSingleValue()) {
                                columnValues.add((Long) range.getSingleValue());
                            }
                            else {
                                if (range.getLow().getValueBlock().isPresent()) {
                                    columnValues.add((Long) range.getLow().getValue());
                                }
                                if (range.getHigh().getValueBlock().isPresent()) {
                                    columnValues.add((Long) range.getHigh().getValue());
                                }
                            }
                        }
                        return columnValues.build();
                    },
                    discreteValues -> ImmutableList.of(),
                    allOrNone -> ImmutableList.of()
                );

                offsetValueBuilder.addAll(values);
            }
        }
        SortedSet<Long> offsetValues = offsetValueBuilder.build();
        if (offsetValues.isEmpty()) {
            return -1;
        }

        for (Long value : offsetValues) {
            if (value >= segment.getStartOffset() && value <= segment.getEndOffset()) {
                return value;
            }
        }
        return -1;
    }
}
