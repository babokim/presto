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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertTableHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KafkaPartitionManager
{
  private static final Logger log = Logger.get(KafkaPartitionManager.class);

  private final String connectorId;
  private KafkaSimpleConsumerManager consumerManager;
  private KafkaConnectorConfig config;

  @Inject
  public KafkaPartitionManager(KafkaConnectorId connectorId,
                               KafkaSimpleConsumerManager consumerManager,
                               KafkaConnectorConfig config)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.consumerManager = consumerManager;
    this.config = config;
  }

  public KafkaPartitionResult getPartitions(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ConnectorTableMetadata tableMeta,
                                            TupleDomain<ColumnHandle> effectivePredicate)
  {
    KafkaTableHandle kafkaTableHandle = convertTableHandle(tableHandle);
    requireNonNull(effectivePredicate, "effectivePredicate is null");

    ImmutableList.Builder<KafkaColumnHandle> partitionColumnBuilder = ImmutableList.builder();

    int index = 0;
    for (ColumnMetadata columnMeta : tableMeta.getColumns()) {
      if (columnMeta.getName().equals(KafkaInternalFieldDescription.PARTITION_ID_FIELD.getName())) {
        partitionColumnBuilder.add(KafkaInternalFieldDescription.PARTITION_ID_FIELD.getColumnHandle(connectorId, index, false));
      }
      else if (columnMeta.getName().equals(KafkaInternalFieldDescription.PARTITION_OFFSET_FIELD.getName())) {
        partitionColumnBuilder.add(KafkaInternalFieldDescription.PARTITION_OFFSET_FIELD.getColumnHandle(connectorId, index, false));
      }
      index++;
    }

    List<KafkaColumnHandle> partitionColumns = partitionColumnBuilder.build();
    TupleDomain<KafkaColumnHandle> compactEffectivePredicate = toCompactTupleDomain(effectivePredicate);

    //If internal fields are hidden
    if (partitionColumns.isEmpty()) {
      return new KafkaPartitionResult(partitionColumns, ImmutableList.of(), TupleDomain.none(), TupleDomain.none());
    }

    if (effectivePredicate.isNone()) {
      return new KafkaPartitionResult(partitionColumns, ImmutableList.of(), TupleDomain.none(), TupleDomain.none());
    }

    // Get all partitions of a table.
    List<KafkaSegment> topicSegments = getKafkaPartitionAndSegments(kafkaTableHandle.toSchemaTableName());

    //===================================>
    //filter 조건에 맞는 partition을 찾으면 된다
    ImmutableList.Builder<KafkaPartition> partitions = ImmutableList.builder();
    for (KafkaSegment eachSegment : topicSegments) {
      Optional<Map<ColumnHandle, NullableValue>> partitionKeys = includesPartitionValue(eachSegment, partitionColumns, effectivePredicate);
      if (partitionKeys.isPresent()) {
        partitions.add(
            new KafkaPartition(
                kafkaTableHandle.toSchemaTableName(),
                compactEffectivePredicate,
                partitionKeys.get(),
                eachSegment));
      }
    }

    return new KafkaPartitionResult(partitionColumns, partitions.build(), effectivePredicate, TupleDomain.none());
  }

  private static TupleDomain<KafkaColumnHandle> toCompactTupleDomain(TupleDomain<ColumnHandle> effectivePredicate)
  {
    int threshold = 100;
    checkArgument(effectivePredicate.getDomains().isPresent());

    ImmutableMap.Builder<KafkaColumnHandle, Domain> builder = ImmutableMap.builder();
    for (Map.Entry<ColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
      KafkaColumnHandle kafkaColumnHandle = KafkaColumnHandle.class.cast(entry.getKey());

//      ValueSet values = entry.getValue().getValues();
//      ValueSet compactValueSet = values.getValuesProcessor().<Optional<ValueSet>>transform(
//          ranges -> ranges.getRangeCount() > threshold ? Optional.of(ValueSet.ofRanges(ranges.getSpan())) : Optional.empty(),
//          discreteValues -> discreteValues.getValues().size() > threshold ? Optional.of(ValueSet.all(values.getType())) : Optional.empty(),
//          allOrNone -> Optional.empty())
//          .orElse(values);
//      builder.put(kafkaColumnHandle, Domain.create(compactValueSet, entry.getValue().isNullAllowed()));
      builder.put(kafkaColumnHandle, entry.getValue());
    }
    return TupleDomain.withColumnDomains(builder.build());
  }

  private List<KafkaSegment> getKafkaPartitionAndSegments(SchemaTableName tableName)
  {
    List<HostAddress> brokers = new ArrayList<>(config.getNodes());
    HostAddress node = brokers.get(ThreadLocalRandom.current().nextInt(brokers.size()));
    String topicName = tableName.toString();

    SimpleConsumer simpleConsumer = consumerManager.getConsumer(node);

    TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ImmutableList.of());
    TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

    ImmutableList.Builder<KafkaSegment> builder = ImmutableList.builder();

    for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
      if (topicName != null && !metadata.topic().equals(topicName)) {
        continue;
      }
      for (PartitionMetadata part : metadata.partitionsMetadata()) {
        Broker leader = part.leader();
        if (leader == null) { // Leader election going on...
          continue;
        }

        HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());

        SimpleConsumer leaderConsumer = consumerManager.getConsumer(partitionLeader);

        long[] offsets = KafkaSplitManager.findAllOffsets(leaderConsumer, metadata.topic(), part.partitionId());

        for (int i = offsets.length - 1; i > 0; i--) {
          builder.add(new KafkaSegment(part.partitionId(), offsets[i], offsets[i - 1], partitionLeader));
        }
      }
    }

    return builder.build();
  }

  private Optional<Map<ColumnHandle, NullableValue>> includesPartitionValue(KafkaSegment eachSegment, List<KafkaColumnHandle> partitionColumns, TupleDomain<ColumnHandle> predicate)
  {
    checkArgument(predicate.getDomains().isPresent());

    Map<ColumnHandle, Domain> domains = predicate.getDomains().get();
    ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
    for (int i = 0; i < partitionColumns.size(); i++) {
      KafkaColumnHandle column = partitionColumns.get(i);
      NullableValue value = NullableValue.of(column.getType(), i == 0 ? eachSegment.getPartitionId() : eachSegment.getEndOffset());

      Domain allowedDomain = domains.get(column);
//      if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
//        return Optional.empty();
//      }
      builder.put(column, value);
    }

    return Optional.of(builder.build());
  }
}
