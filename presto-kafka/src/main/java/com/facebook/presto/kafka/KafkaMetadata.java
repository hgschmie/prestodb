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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.name.Named;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

public class KafkaMetadata
    extends ReadOnlyConnectorMetadata
{
    private static final Logger LOGGER = Logger.get(KafkaMetadata.class);

    private final String connectorId;
    private final KafkaConfig kafkaConfig;
    private final KafkaHandleResolver handleResolver;
    private final JsonCodec<KafkaTable> tableCodec;

    private final Supplier<Map<String, KafkaTable>> tableDefinitions;

    @Inject
    KafkaMetadata(@Named("connectorId") String connectorId,
            KafkaConfig kafkaConfig,
            KafkaHandleResolver handleResolver,
            JsonCodec<KafkaTable> tableCodec)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.kafkaConfig = checkNotNull(kafkaConfig, "kafkaConfig is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.tableCodec = checkNotNull(tableCodec, "tableCodec is null");

        LOGGER.debug("Loading kafka table definitions from %s", kafkaConfig.getTableDescriptionDir().getAbsolutePath());

        this.tableDefinitions = Suppliers.memoize(new TableSupplier());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        LOGGER.debug("listSchemaNames(%s)", session);

        List<String> result = ImmutableList.of(kafkaConfig.getSchemaName());

        LOGGER.debug("Result: %s", result);
        return result;
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        LOGGER.debug("getTableHandle(%s, %s)", session, schemaTableName);

        handleResolver.checkSchemaName(schemaTableName.getSchemaName());

        KafkaTable table = getDefinedTables().get(schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        KafkaTableHandle result = new KafkaTableHandle(connectorId,
                                                       schemaTableName.getSchemaName(),
                                                       schemaTableName.getTableName(),
                                                       table.getTopicName());

        LOGGER.debug("Result: %s", result);
        return result;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        LOGGER.debug("getTableMetadata(%s)", tableHandle);

        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);

        return getTableMetadata(kafkaTableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        LOGGER.debug("listTables(%s, %s)", session, schemaNameOrNull);

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        if (schemaNameOrNull == null || schemaNameOrNull.equals(kafkaConfig.getSchemaName())) {
            for (String tableName : getDefinedTables().keySet()) {
                builder.add(new SchemaTableName(kafkaConfig.getSchemaName(), tableName));
            }
        }

        List<SchemaTableName> result = builder.build();
        LOGGER.debug("Result: %s", result);
        return result;
    }

    @Override
    public ConnectorColumnHandle getColumnHandle(ConnectorTableHandle tableHandle, String columnName)
    {
        LOGGER.debug("getColumnHandle(%s, %s)", tableHandle, columnName);

        ConnectorColumnHandle result = getColumnHandles(tableHandle).get(columnName);

        LOGGER.debug("Result: %s", result);
        return result;
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        LOGGER.debug("getColumnHandles(%s)", tableHandle);

        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);

        KafkaTable table = getDefinedTables().get(kafkaTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : table.getColumnsMetadata()) {
            columnHandles.put(columnMetadata.getName(), new KafkaColumnHandle(connectorId, columnMetadata));
        }

        Map<String, ConnectorColumnHandle> result = columnHandles.build();
        LOGGER.debug("Result: %s", result);
        return result;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        LOGGER.debug("listTableColumns(%s, %s)", session, prefix);

        checkNotNull(prefix, "prefix is null");

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
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        LOGGER.debug("getColumnMetadata(%s, %s)", tableHandle, columnHandle);

        handleResolver.convertTableHandle(tableHandle);
        KafkaColumnHandle kafkaColumnHandle = handleResolver.convertColumnHandle(columnHandle);

        ColumnMetadata result = kafkaColumnHandle.getColumnMetadata();

        LOGGER.debug("Result: %s", result);
        return result;
    }

    @VisibleForTesting
    Map<String, KafkaTable> getDefinedTables()
    {
        return tableDefinitions.get();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        handleResolver.checkSchemaName(schemaTableName.getSchemaName());
        KafkaTable table = getDefinedTables().get(schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ConnectorTableMetadata result = new ConnectorTableMetadata(schemaTableName, table.getColumnsMetadata());

        LOGGER.debug("Result: %s", result);
        return result;
    }

    private class TableSupplier
            implements Supplier<Map<String, KafkaTable>>
    {
        @Override
        public Map<String, KafkaTable> get()
        {
            ImmutableMap.Builder<String, KafkaTable> builder = ImmutableMap.builder();

            try {
                for (File file : listFiles(kafkaConfig.getTableDescriptionDir())) {
                    if (file.isFile() && file.getName().endsWith(".json")) {
                        KafkaTable table = tableCodec.fromJson(Files.toByteArray(file));
                        LOGGER.debug("Kafka table %s: %s", table.getTableName(), table);
                        builder.put(table.getTableName(), table);
                    }
                }

                Map<String, KafkaTable> tableDefinitions = builder.build();

                LOGGER.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

                builder = ImmutableMap.builder();
                for (String definedTable : kafkaConfig.getTableNames()) {
                    if (tableDefinitions.containsKey(definedTable)) {
                        LOGGER.debug("Found Table definition for %s: %s", definedTable, tableDefinitions.get(definedTable));
                        builder.put(definedTable, tableDefinitions.get(definedTable));
                    }
                    else {
                        LOGGER.debug("Created basic Table definition for %s", definedTable);
                        builder.put(definedTable, new KafkaTable(definedTable, definedTable, ImmutableList.<KafkaColumn>of()));
                    }
                }

                return builder.build();
            }
            catch (IOException e) {
                LOGGER.warn(e, "Error: ");
                throw Throwables.propagate(e);
            }
        }

        private List<File> listFiles(File dir)
        {
            if (dir != null && dir.isDirectory()) {
                File[] files = dir.listFiles();
                if (files != null) {
                    LOGGER.debug("Considering files: %s", Arrays.asList(files));
                    return ImmutableList.copyOf(files);
                }
            }
            return ImmutableList.of();
        }
    }
}
