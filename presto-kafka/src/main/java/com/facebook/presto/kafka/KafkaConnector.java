package com.facebook.presto.kafka;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputHandleResolver;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaConnector
        implements Connector
{
    private final KafkaMetadata metadata;

    private final KafkaSplitManager splitManager;
    private final KafkaRecordSetProvider recordSetProvider;
    private final KafkaHandleResolver handleResolver;

    @Inject
    public KafkaConnector(
            KafkaHandleResolver handleResolver,
            KafkaMetadata metadata,
            KafkaSplitManager splitManager,
            KafkaRecordSetProvider recordSetProvider)
    {
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.recordSetProvider = checkNotNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public ConnectorOutputHandleResolver getOutputHandleResolver()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorIndexResolver getIndexResolver()
    {
        throw new UnsupportedOperationException();
    }
}
