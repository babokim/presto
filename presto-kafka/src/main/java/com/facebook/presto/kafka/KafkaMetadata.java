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

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.NullableValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertColumnHandle;
import static com.facebook.presto.kafka.KafkaHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

/**
 * Manages the Kafka connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link KafkaInternalFieldDescription} for a list
 * of per-topic additional columns.
 */
public class KafkaMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(KafkaMetadata.class);
    private final String connectorId;
    private final boolean hideInternalColumns;
    private final Map<SchemaTableName, KafkaTopicDescription> tableDescriptions;
    private final Set<KafkaInternalFieldDescription> internalFieldDescriptions;
    private KafkaPartitionManager partitionManager;
    private final String topicMetaSchema;

    @Inject
    public KafkaMetadata(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            Supplier<Map<SchemaTableName, KafkaTopicDescription>> kafkaTableDescriptionSupplier,
            Set<KafkaInternalFieldDescription> internalFieldDescriptions,
            KafkaPartitionManager partitionManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.hideInternalColumns = kafkaConnectorConfig.isHideInternalColumns();

        requireNonNull(kafkaTableDescriptionSupplier, "kafkaTableDescriptionSupplier is null");
        this.tableDescriptions = kafkaTableDescriptionSupplier.get();
        this.internalFieldDescriptions = requireNonNull(internalFieldDescriptions, "internalFieldDescriptions is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.topicMetaSchema = kafkaConnectorConfig.getTopicMetaSchema();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            builder.add(tableName.getSchemaName());
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            return null;
        }

        return new KafkaTableHandle(connectorId,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getTopicName(),
                getDataFormat(table.getKey()),
                getDataFormat(table.getMessage()));
    }

    private static String getDataFormat(KafkaTopicFieldGroup fieldGroup)
    {
        return (fieldGroup == null) ? DummyRowDecoder.NAME : fieldGroup.getDataFormat();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = convertTableHandle(tableHandle);

        KafkaTopicDescription kafkaTopicDescription = tableDescriptions.get(kafkaTableHandle.toSchemaTableName());
        if (kafkaTopicDescription == null) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;

        if (kafkaTableHandle.getSchemaName().equals(topicMetaSchema) && kafkaTableHandle.getTableName().equals(KafkaConnectorConfig.TOPIC_META_TABLE_NAME)) {
            for (KafkaInternalFieldDescription kafkaInternalFieldDescription : KafkaInternalFieldDescription.getTopicMetaFields()) {
                columnHandles.put(kafkaInternalFieldDescription.getName(), kafkaInternalFieldDescription.getColumnHandle(connectorId, index++, false));
            }
            return columnHandles.build();
        }

        KafkaTopicFieldGroup key = kafkaTopicDescription.getKey();
        if (key != null) {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                    columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, true, index++));
                }
            }
        }

        KafkaTopicFieldGroup message = kafkaTopicDescription.getMessage();
        if (message != null) {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                    columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, false, index++));
                }
            }
        }

        for (KafkaInternalFieldDescription kafkaInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(kafkaInternalFieldDescription.getName(), kafkaInternalFieldDescription.getColumnHandle(connectorId, index++, hideInternalColumns));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        KafkaTableHandle handle = convertTableHandle(table);
        if (handle.getSchemaName().equals(topicMetaSchema) && handle.getTableName().equals(KafkaConnectorConfig.TOPIC_META_TABLE_NAME)) {
            ConnectorTableLayout layout = new ConnectorTableLayout(new KafkaTableLayoutHandle(handle));
            return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
        }

        KafkaPartitionResult kafkaPartitionResult = partitionManager.getPartitions(session, table, getTableMetadata(handle.toSchemaTableName()), constraint.getSummary());

        ImmutableList.Builder<KafkaPartition> partitionBuilder = ImmutableList.builder();
        for (KafkaPartition partition : kafkaPartitionResult.getPartitions()) {
            Predicate<Map<ColumnHandle, NullableValue>> predicate = constraint.predicate();
            boolean matched = predicate.test(partition.getPartitionKeys());
            if (matched) {
                partitionBuilder.add(partition);
            }
        }
        List<KafkaPartition> partitions = partitionBuilder.build();

//        List<KafkaPartition> partitions = kafkaPartitionResult.getPartitions().stream()
//            .filter(partition -> constraint.predicate().test(partition.getPartitionKeys()))
//            .collect(toList());

        if (partitions.isEmpty()) {
            ConnectorTableLayout layout = new ConnectorTableLayout(new KafkaTableLayoutHandle(handle));
            return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
        }

//        return ImmutableList.of(new ConnectorTableLayoutResult(
//            getTableLayout(session, new KafkaTableLayoutHandle(handle,
//                ImmutableList.copyOf(kafkaPartitionResult.getPartitionColumns()),
//                partitions,
//                kafkaPartitionResult.getEnforcedConstraint())),
//            kafkaPartitionResult.getUnenforcedConstraint()));

        ConnectorTableLayout layout = new ConnectorTableLayout(
            new KafkaTableLayoutHandle(handle,
                ImmutableList.copyOf(kafkaPartitionResult.getPartitionColumns()),
                partitions,
                kafkaPartitionResult.getEnforcedConstraint()));

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, kafkaPartitionResult.getUnenforcedConstraint()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        if (schemaTableName.getSchemaName().equals(topicMetaSchema) && schemaTableName.getTableName().equals(KafkaConnectorConfig.TOPIC_META_TABLE_NAME)) {
            for (KafkaInternalFieldDescription fieldDescription : KafkaInternalFieldDescription.getTopicMetaFields()) {
                builder.add(fieldDescription.getColumnMetadata(false));
            }
            return new ConnectorTableMetadata(schemaTableName, builder.build());
        }

        KafkaTopicFieldGroup key = table.getKey();
        if (key != null) {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        KafkaTopicFieldGroup message = table.getMessage();
        if (message != null) {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        for (KafkaInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
