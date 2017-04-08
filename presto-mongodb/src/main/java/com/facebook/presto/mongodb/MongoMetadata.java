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
package com.facebook.presto.mongodb;

import com.facebook.presto.mongodb.MongoIndex.MongodbIndexKey;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MongoMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(MongoMetadata.class);

    private final MongoSession mongoSession;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    //lowercase -> origin name
    private final Map<String, String> originSchemaNames = new HashMap<String, String>();

    private final Map<String, String> originTableNames = new HashMap<String, String>();

    public MongoMetadata(MongoSession mongoSession)
    {
        this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
      synchronized (originSchemaNames) {
        List<String> schemas = mongoSession.getAllSchemas();
        List<String> lowercaseSchemas = new ArrayList<String>();
        schemas.stream().forEach(schema -> {
          String convertedSchemaName = schema.toLowerCase(ENGLISH).replace("-", "_");
          originSchemaNames.put(schema, schema);
          originSchemaNames.put(convertedSchemaName, schema);
          lowercaseSchemas.add(convertedSchemaName);
        });
        return lowercaseSchemas;
      }
    }

    private String getOriginSchemaName(String schema)
    {
      if (originSchemaNames.isEmpty()) {
        listSchemaNames(null);
        listTables(null, (String)null);
      }
      if (schema == null) {
        Exception e = new Exception("ERROR");
        log.error(e, "Schema is null");
      }
      synchronized (originSchemaNames) {
        return originSchemaNames.get(schema);
      }
    }

    private String getOriginTableName(String schemaName, String tableName)
    {
      return originTableNames.get(getOriginSchemaName(schemaName) + "." + tableName);
    }

    @Override
    public MongoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        String originSchemaName = getOriginSchemaName(tableName.getSchemaName());
        String originTableName = getOriginTableName(originSchemaName, tableName.getTableName());
        if (originSchemaName == null || originTableName == null) {
          log.error("Can't get origin tableName: %s, %s", originSchemaName, tableName.getTableName());
          return null;
        }
        tableName.setName(originSchemaName, originTableName);

        requireNonNull(tableName, "tableName is null");
        try {
            return mongoSession.getTable(tableName).getTableHandle();
        }
        catch (TableNotFoundException e) {
            log.debug(e, "Table(%s) not found", tableName);
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        String originSchemaName = getOriginSchemaName(tableName.getSchemaName());
        String originTableName = getOriginTableName(originSchemaName, tableName.getTableName());
        tableName.setName(originSchemaName, originTableName);
        return getTableMetadata(session, tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();

        synchronized (originTableNames) {
          for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : mongoSession.getAllTables(getOriginSchemaName(schemaName))) {
              String originSchemaName = getOriginSchemaName(schemaName);
              originTableNames.put(originSchemaName + "." + tableName.toLowerCase(ENGLISH), tableName);
              originTableNames.put(originSchemaName + "." + tableName, tableName);
              tableNames.add(new SchemaTableName(originSchemaName, tableName));
            }
          }
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        List<MongoColumnHandle> columns = mongoSession.getTable(table.getSchemaTableName()).getColumns();

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (MongoColumnHandle columnHandle : columns) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((MongoColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        MongoTableHandle tableHandle = (MongoTableHandle) table;

        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty(); //TODO: sharding key
        ImmutableList.Builder<LocalProperty<ColumnHandle>> localProperties = ImmutableList.builder();

        MongoTable tableInfo = mongoSession.getTable(tableHandle.getSchemaTableName());
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);

        for (MongoIndex index : tableInfo.getIndexes()) {
            for (MongodbIndexKey key : index.getKeys()) {
                if (!key.getSortOrder().isPresent()) {
                    continue;
                }
                if (columns.get(key.getName()) != null) {
                    localProperties.add(new SortingProperty<>(columns.get(key.getName()), key.getSortOrder().get()));
                }
            }
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(
                new MongoTableLayoutHandle(tableHandle, constraint.getSummary()),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                partitioningColumns,
                Optional.empty(),
                localProperties.build());

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        MongoTableLayoutHandle layout = (MongoTableLayoutHandle) handle;

        // tables in this connector have a single layout
        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        mongoSession.createTable(tableMetadata.getTable(), buildColumnHandles(tableMetadata));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;

        mongoSession.dropTable(table.getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        List<MongoColumnHandle> columns = buildColumnHandles(tableMetadata);

        mongoSession.createTable(tableMetadata.getTable(), columns);

        setRollback(() -> mongoSession.dropTable(tableMetadata.getTable()));

        return new MongoOutputTableHandle(
                tableMetadata.getTable(),
                columns.stream().filter(c -> !c.isHidden()).collect(toList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        clearRollback();
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        List<MongoColumnHandle> columns = mongoSession.getTable(table.getSchemaTableName()).getColumns();

        return new MongoInsertTableHandle(
                table.getSchemaTableName(),
                columns.stream().filter(c -> !c.isHidden()).collect(toList()));
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return ((MongoTableHandle) tableHandle).getSchemaTableName();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        String originSchemaName = getOriginSchemaName(tableName.getSchemaName());
        String originTableName = getOriginTableName(originSchemaName, tableName.getTableName());
        tableName.setName(originSchemaName, originTableName);

        MongoTableHandle tableHandle = mongoSession.getTable(tableName).getTableHandle();

        List<ColumnMetadata> columns = ImmutableList.copyOf(
                getColumnHandles(session, tableHandle).values().stream()
                        .map(MongoColumnHandle.class::cast)
                        .map(MongoColumnHandle::toColumnMetadata)
                        .collect(toList()));

        return new ConnectorTableMetadata(tableName, columns);
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        String originSchemaName = originSchemaNames.get(prefix.getSchemaName());
        if (originSchemaName == null) {
          return listTables(session, originSchemaName);
        }

        return ImmutableList.of(new SchemaTableName(originSchemaName,
            originTableNames.get(originSchemaName + "." + prefix.getTableName())));
    }

    private static List<MongoColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        return tableMetadata.getColumns().stream()
                .map(m -> new MongoColumnHandle(m.getName(), m.getType(), m.isHidden()))
                .collect(toList());
    }
}
