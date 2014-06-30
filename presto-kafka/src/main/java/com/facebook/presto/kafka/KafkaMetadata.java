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

import com.facebook.presto.kafka.decoder.dummy.DummyKafkaRowDecoder;
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

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages the Kafka connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns.
 */
public class KafkaMetadata
        extends ReadOnlyConnectorMetadata
{
    private static final Logger LOG = Logger.get(KafkaMetadata.class);

    private final String connectorId;
    private final KafkaConfig kafkaConfig;
    private final KafkaHandleResolver handleResolver;
    private final JsonCodec<KafkaTopicDescription> tableCodec;

    private final Supplier<Map<String, KafkaTopicDescription>> tableDefinitions;
    private final Set<KafkaInternalFieldDescription> internalFieldDescriptions;

    @Inject
    KafkaMetadata(@Named("connectorId") String connectorId,
            KafkaConfig kafkaConfig,
            KafkaHandleResolver handleResolver,
            JsonCodec<KafkaTopicDescription> topicDescriptionCodec,
            Set<KafkaInternalFieldDescription> internalFieldDescriptions)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.kafkaConfig = checkNotNull(kafkaConfig, "kafkaConfig is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.tableCodec = checkNotNull(topicDescriptionCodec, "topicDescriptionCodec is null");

        LOG.debug("Loading kafka table definitions from %s", kafkaConfig.getTableDescriptionDir().getAbsolutePath());

        this.tableDefinitions = Suppliers.memoize(new TableSupplier());
        this.internalFieldDescriptions = checkNotNull(internalFieldDescriptions, "internalFieldDescriptions is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(kafkaConfig.getSchemaName());
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        handleResolver.checkSchemaName(schemaTableName.getSchemaName());

        KafkaTopicDescription table = getDefinedTables().get(schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        return new KafkaTableHandle(connectorId,
                table.getDataFormat(),
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getTopicName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);
        return getTableMetadata(kafkaTableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        if (schemaNameOrNull == null || schemaNameOrNull.equals(kafkaConfig.getSchemaName())) {
            for (String tableName : getDefinedTables().keySet()) {
                builder.add(new SchemaTableName(kafkaConfig.getSchemaName(), tableName));
            }
        }

        return builder.build();
    }

    @Override
    public ConnectorColumnHandle getColumnHandle(ConnectorTableHandle tableHandle, String columnName)
    {
        return getColumnHandles(tableHandle).get(columnName);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);

        KafkaTopicDescription kafkaTopicDescription = getDefinedTables().get(kafkaTableHandle.getTableName());
        if (kafkaTopicDescription == null) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;
        for (KafkaTopicFieldDescription kafkaTopicFieldDescription : kafkaTopicDescription.getFields()) {
            columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, index++));
        }

        for (KafkaInternalFieldDescription kafkaInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(kafkaInternalFieldDescription.getName(), kafkaInternalFieldDescription.getColumnHandle(connectorId, index++, kafkaConfig.isInternalColumnsAreHidden()));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
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
        handleResolver.convertTableHandle(tableHandle);
        KafkaColumnHandle kafkaColumnHandle = handleResolver.convertColumnHandle(columnHandle);

        return kafkaColumnHandle.getColumnMetadata();
    }

    @VisibleForTesting
    Map<String, KafkaTopicDescription> getDefinedTables()
    {
        return tableDefinitions.get();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        handleResolver.checkSchemaName(schemaTableName.getSchemaName());
        KafkaTopicDescription table = getDefinedTables().get(schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        int index = 0;

        for (KafkaTopicFieldDescription fieldDescription : table.getFields()) {
            builder.add(fieldDescription.getColumnMetadata(index++));
        }

        for (KafkaInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(index++, kafkaConfig.isInternalColumnsAreHidden()));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    private class TableSupplier
            implements Supplier<Map<String, KafkaTopicDescription>>
    {
        @Override
        public Map<String, KafkaTopicDescription> get()
        {
            ImmutableMap.Builder<String, KafkaTopicDescription> builder = ImmutableMap.builder();

            try {
                for (File file : listFiles(kafkaConfig.getTableDescriptionDir())) {
                    if (file.isFile() && file.getName().endsWith(".json")) {
                        KafkaTopicDescription table = tableCodec.fromJson(Files.toByteArray(file));
                        LOG.debug("Kafka table %s: %s", table.getTableName(), table);
                        builder.put(table.getTableName(), table);
                    }
                }

                Map<String, KafkaTopicDescription> tableDefinitions = builder.build();

                LOG.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

                builder = ImmutableMap.builder();
                for (String definedTable : kafkaConfig.getTableNames()) {
                    if (tableDefinitions.containsKey(definedTable)) {
                        KafkaTopicDescription kafkaTable = tableDefinitions.get(definedTable);
                        LOG.debug("Found Table definition for %s: %s", definedTable, kafkaTable);
                        builder.put(kafkaTable.getTableName(), kafkaTable);
                    }
                    else {
                        // A dummy table definition only supports the internal columns.
                        LOG.debug("Created dummy Table definition for %s", definedTable);
                        builder.put(definedTable, new KafkaTopicDescription(definedTable,
                                definedTable,
                                DummyKafkaRowDecoder.NAME,
                                ImmutableList.<KafkaTopicFieldDescription>of()));
                    }
                }

                return builder.build();
            }
            catch (IOException e) {
                LOG.warn(e, "Error: ");
                throw Throwables.propagate(e);
            }
        }

        private List<File> listFiles(File dir)
        {
            if (dir != null && dir.isDirectory()) {
                File[] files = dir.listFiles();
                if (files != null) {
                    LOG.debug("Considering files: %s", Arrays.asList(files));
                    return ImmutableList.copyOf(files);
                }
            }
            return ImmutableList.of();
        }
    }
}
