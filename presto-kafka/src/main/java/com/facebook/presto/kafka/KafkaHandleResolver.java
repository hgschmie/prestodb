package com.facebook.presto.kafka;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.inject.Inject;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.google.inject.name.Named;

public class KafkaHandleResolver
        implements ConnectorHandleResolver
{
    private final String schemaName;
    private final String connectorId;

    @Inject
    KafkaHandleResolver(@Named("connectorId") String connectorId,
                        KafkaConfig kafkaConfig)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        checkNotNull(kafkaConfig, "kafkaConfig is null");

        this.schemaName = kafkaConfig.getSchemaName();
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandle != null && tableHandle instanceof KafkaTableHandle && connectorId.equals(((KafkaTableHandle) tableHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorColumnHandle columnHandle)
    {
        return columnHandle != null && columnHandle instanceof KafkaColumnHandle && connectorId.equals(((KafkaColumnHandle) columnHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return split != null && split instanceof KafkaSplit && connectorId.equals(((KafkaSplit) split).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorIndexHandle indexHandle)
    {
        return false;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return KafkaTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorColumnHandle> getColumnHandleClass()
    {
        return KafkaColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        return null;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return KafkaSplit.class;
    }

    void checkSchemaName(String schemaName)
    {
        if (schemaName != null && !this.schemaName.equals(schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    KafkaTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof KafkaTableHandle, "tableHandle is not an instance of KafkaTableHandle");
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) tableHandle;
        checkArgument(kafkaTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        checkSchemaName(kafkaTableHandle.getSchemaName());

        return kafkaTableHandle;
    }

    KafkaColumnHandle convertColumnHandle(ConnectorColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof KafkaColumnHandle, "columnHandle is not an instance of KafkaColumnHandle");
        KafkaColumnHandle kafkaColumnHandle = (KafkaColumnHandle) columnHandle;
        checkArgument(kafkaColumnHandle.getConnectorId().equals(connectorId), "columnHandle is not for this connector");
        return kafkaColumnHandle;
    }

    KafkaSplit convertSplit(ConnectorSplit split)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof KafkaSplit, "split is not an instance of KafkaSplit");
        KafkaSplit kafkaSplit = (KafkaSplit) split;
        checkArgument(kafkaSplit.getConnectorId().equals(connectorId), "split is not for this connector");
        return kafkaSplit;
    }
}
