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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KafkaTopicMetaRecordSet implements RecordSet
{
  private static final Logger log = Logger.get(KafkaTopicMetaRecordSet.class);

  private final KafkaSplit split;
  private final KafkaSimpleConsumerManager consumerManager;
  private final List<DecoderColumnHandle> columnHandles;
  private final List<Type> columnTypes;

  public KafkaTopicMetaRecordSet(KafkaSplit split,
                                 KafkaSimpleConsumerManager consumerManager,
                                 List<DecoderColumnHandle> columnHandles)
  {
    this.split = requireNonNull(split, "split is null");
    this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");
    this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

    ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

    for (DecoderColumnHandle handle : columnHandles) {
      typeBuilder.add(handle.getType());
    }

    this.columnTypes = typeBuilder.build();
  }

  @Override
  public List<Type> getColumnTypes()
  {
    return columnTypes;
  }

  @Override
  public RecordCursor cursor()
  {
    return new KafkaTopicMetaRecordCursor();
  }

  private static <T> T selectRandom(Iterable<T> iterable)
  {
    List<T> list = ImmutableList.copyOf(iterable);
    return list.get(ThreadLocalRandom.current().nextInt(list.size()));
  }

  public class KafkaTopicMetaRecordCursor
      implements RecordCursor
  {
    private final AtomicBoolean topicMetaLoaded = new AtomicBoolean();
    private Iterator<Set<FieldValueProvider>> topicMetaRecordIterator;
    private FieldValueProvider[] fieldValueProviders;

    KafkaTopicMetaRecordCursor()
    {
    }

    @Override
    public long getTotalBytes()
    {
      return 0;
    }

    @Override
    public long getCompletedBytes()
    {
      return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
      return 0;
    }

    @Override
    public Type getType(int field)
    {
      checkArgument(field < columnHandles.size(), "Invalid field index");
      return columnHandles.get(field).getType();
    }

    private void loadTopicMeta()
    {
      SimpleConsumer simpleConsumer = consumerManager.getConsumer(selectRandom(split.getAddresses()));

      TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ImmutableList.of());
      TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

      ImmutableList.Builder<Set<FieldValueProvider>> builder = ImmutableList.builder();

      for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
        if ("__consumer_offsets".equals(metadata.topic()) || "topics".equals(metadata.topic())) {
          continue;
        }
        for (PartitionMetadata part : metadata.partitionsMetadata()) {
          Broker leader = part.leader();
          if (leader == null) { // Leader election going on...
            continue;
          }

          HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());

          SimpleConsumer leaderConsumer = consumerManager.getConsumer(partitionLeader);
          // Kafka contains a reverse list of "end - start" pairs for the splits

          long[] offsets = KafkaSplitManager.findAllOffsets(leaderConsumer, metadata.topic(), part.partitionId());

          long maxOffset = offsets.length == 0 ? 0 : offsets[0];
          for (int i = offsets.length - 1; i >= 0; i--) {
            Set<FieldValueProvider> fieldValueProviders = new HashSet<>();

            fieldValueProviders.add(KafkaInternalFieldDescription.TOPIC_NAME_FIELD.forByteValue(metadata.topic().getBytes()));
            fieldValueProviders.add(KafkaInternalFieldDescription.PARTITION_ID_FIELD.forLongValue(part.partitionId()));
            fieldValueProviders.add(KafkaInternalFieldDescription.PARTITION_OFFSET_FIELD.forLongValue(maxOffset));
            fieldValueProviders.add(KafkaInternalFieldDescription.SEGMENT_END_FIELD.forLongValue(offsets[i]));

            builder.add(fieldValueProviders);
          }
        }
      }

      topicMetaRecordIterator = builder.build().iterator();
    }

    @Override
    public boolean advanceNextPosition()
    {
      if (!topicMetaLoaded.get()) {
        synchronized (topicMetaLoaded) {
          loadTopicMeta();
          topicMetaLoaded.set(true);
        }
      }
      if (!topicMetaRecordIterator.hasNext()) {
        return false;
      }
      this.fieldValueProviders = new FieldValueProvider[columnHandles.size()];

      Set<FieldValueProvider> currentfieldValueProviders = topicMetaRecordIterator.next();
      for (int i = 0; i < columnHandles.size(); i++) {
        for (FieldValueProvider fieldValueProvider : currentfieldValueProviders) {
          if (fieldValueProvider.accept(columnHandles.get(i))) {
            this.fieldValueProviders[i] = fieldValueProvider;
            break; // for(InternalColumnProvider...
          }
        }
      }

      return true;
    }

    private void checkFieldType(int field, Class<?> expected)
    {
      Class<?> actual = getType(field).getJavaType();
      checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @SuppressWarnings("SimplifiableConditionalExpression")
    @Override
    public boolean getBoolean(int field)
    {
      return false;
    }

    @Override
    public long getLong(int field)
    {
      checkArgument(field < columnHandles.size(), "Invalid field index");

      checkFieldType(field, long.class);
      return isNull(field) ? 0L : fieldValueProviders[field].getLong();
    }

    @Override
    public double getDouble(int field)
    {
      checkArgument(field < columnHandles.size(), "Invalid field index");

      checkFieldType(field, double.class);
      return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
      checkArgument(field < columnHandles.size(), "Invalid field index");

      checkFieldType(field, Slice.class);
      return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
    }

    @Override
    public Object getObject(int field)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
      checkArgument(field < columnHandles.size(), "Invalid field index");

      return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
    }

    @Override
    public void close()
    {
    }
  }
}
